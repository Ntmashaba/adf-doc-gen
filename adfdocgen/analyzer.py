"""Static analysis of an ADF factory: the single place analysis logic lives.

Every renderer (HTML, Word, JSON) consumes the payload this module produces.
The core walking / harvesting / lineage logic is adapted from the user's
field-tested adf_distill.py; on top of it this module adds:

* normalised source/sink endpoints — the cross-tool lineage contract
  {system, server, database, schema, object, path, url, container}
  (nulls where unknown, parameter/secret names where parameterised;
  matches pbi-doc-gen's partition-source vocabulary for a future join)
* the orchestration graph: trigger -> pipeline -> ExecutePipeline / data flow
* per-pipeline rollups: own and *effective* (transitive) reads/writes,
  flagged incomplete when an invoked child was not supplied
* unused-resource detection, phrased honestly ("not referenced by any
  trigger or pipeline in this factory" — external invocation is invisible)
* structured warnings {severity, category, message}
* inline-credential detection in linked services (flagged, never printed)

The work is split by responsibility; Analyzer combines the parts:

  common.py           JSON guards, text helpers, physical object identity
  activities.py       activity catalogue and which types are opaque
  resources.py        linked services, datasets (parameter binding), triggers
  dataflow_script.py  data flow script parsing: steps, order, dead ends, traces
  pipelines.py        walking pipelines; what each activity reads/writes/runs
  dataflows.py        data flow endpoints and per-invocation lineage
  graph.py            lineage closure, rollups, orchestration relationships
  checks.py           reference resolution and warnings
  issues.py           review-issue priorities and next actions
  output.py           the payload every renderer consumes
  analyzer.py         the registry of objects and edges, and the entry point

Epistemic rules baked in: report parameter and Key Vault secret *names*,
never values; call nothing "unused" beyond factory scope; mark lineage
opaque where work happens in code ADF cannot see; when unsure, say less.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

from .common import arm_parameter, obj, lst, as_text, squeeze, is_dynamic, canon_table, _norm_host, physical_key, split_table, EMPTY_ENDPOINT  # noqa: F401
from .activities import ACTIVITY_CATALOG, OPAQUE_TYPES  # noqa: F401
from .resources import (describe_dataset, describe_linked_service, describe_trigger,  # noqa: F401
                        friendly_system, is_sql_source, sql_names_dynamic, bind_dataset_params)
from .dataflow_script import inline_dataflow_endpoint, parse_dataflow_script  # noqa: F401
from .issues import ISSUE_GUIDE, NOTE_GUIDE, SCHEMA_VERSION  # noqa: F401
from .sql_harvest import harvest_sql  # noqa: F401
from .redact import scrub_definitions
from .pipelines import PipelineMixin
from .dataflows import DataflowMixin
from .graph import GraphMixin
from .checks import ChecksMixin
from .output import OutputMixin


# ---------------------------------------------------------------------------
# The analyzer
# ---------------------------------------------------------------------------
class Analyzer(PipelineMixin, DataflowMixin, GraphMixin, ChecksMixin, OutputMixin):
    RESOLVABLE = {"dataset": "dataset", "dataflow": "dataflow",
                  "pipeline": "pipeline", "linked_service": "linkedservice"}
    IMPACT = {
        "dataset": "physical table/path unknown — lineage shows an opaque dataset name",
        "dataflow": "transformation logic invisible — sink lineage cannot be traced",
        "pipeline": "child pipeline logic invisible — its reads/writes are missing entirely",
        "linked_service": "target server/database/storage account unknown",
    }

    def __init__(self, store: Dict[str, Dict[str, dict]]):
        self.store = store
        # ADF resource names are case-insensitive: SinkDataSet and SinkDataset
        # are one dataset. References resolve to the defined spelling.
        self._names = {kind: {n.lower(): n for n in names} for kind, names in store.items()
                       if isinstance(names, dict)}
        self.ds_info = {n: describe_dataset(n, p) for n, p in store["dataset"].items()}
        self.ls_info = {n: describe_linked_service(n, p) for n, p in store["linkedservice"].items()}
        self.pipelines: List[dict] = []
        self.dataflows: List[dict] = []
        self.entities: Dict[str, dict] = {}
        self.edges: List[dict] = []
        self.refs: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        self.warnings: List[dict] = []
        self.pipeline_calls: List[dict] = []          # {parent, child, activity, waitOnCompletion}
        self._df_done: Set[str] = set()
        self._incomplete: List[str] = []
        self._no_retry: List[str] = []

    def warn(self, severity: str, category: str, message: str,
             resource: Optional[str] = None) -> None:
        if resource is None:    # the first quoted name in the message
            m = re.search(r"'([^']+)'", message)
            resource = m.group(1) if m else ""
        self.warnings.append({"severity": severity, "category": category,
                              "message": message, "resource": resource})

    def warn_at(self, loc: str, severity: str, category: str, message: str) -> None:
        self.warn(severity, category, message, resource=loc)

    # ---- entity registry ------------------------------------------------

    def node(self, kind: str, label: str, role: str, ref: str,
             detail: str = "", alias: str = "",
             endpoint: Optional[dict] = None, scope: str = "") -> str:
        """Register an entity and return its canonical graph key."""
        label = squeeze(label, 200)
        if not label:
            return ""
        key = physical_key(kind, label, endpoint, scope)
        ent = self.entities.setdefault(key, {
            "kind": kind, "label": label, "aliases": set(),
            "roles": set(), "refs": set(), "detail": "",
            "dynamic": is_dynamic(label),
            "endpoint": dict(EMPTY_ENDPOINT),
            "scope": scope or None,
        })
        ent["roles"].add(role)
        ent["refs"].add(ref)
        if alias and alias != label:
            ent["aliases"].add(alias)
        if detail and not ent["detail"]:
            ent["detail"] = detail
        if endpoint:
            for k, v in endpoint.items():
                if v is not None and ent["endpoint"].get(k) is None:
                    ent["endpoint"][k] = v
        return key

    def named(self, store_kind: str, name: str) -> str:
        """The defined spelling of a referenced resource name."""
        return self._names.get(store_kind, {}).get((name or "").lower(), name)

    def track_ref(self, kind: str, name: str, ref: str) -> None:
        name = self.named(self.RESOLVABLE.get(kind, kind), name)
        if name:
            self.refs[(kind, name)].add(ref)

    def _ls_endpoint(self, ls_name: str) -> dict:
        info = self.ls_info.get(self.named("linkedservice", ls_name))
        if not info:
            return dict(EMPTY_ENDPOINT)
        ep = dict(EMPTY_ENDPOINT)
        ep["system"] = info["system"]
        ep["server"] = info["server"]
        ep["database"] = info["database"]
        ep["url"] = info["url"]
        return ep

    def dataset_node(self, ds_name: str, role: str, ref: str,
                     params: Optional[dict] = None) -> str:
        """Resolve a dataset reference to a canonical physical node."""
        ds_name = self.named("dataset", ds_name)
        self.track_ref("dataset", ds_name, ref)
        info = self.ds_info.get(ds_name)
        if not info:
            self._incomplete.append(f"dataset {ds_name}")
            return self.node("dataset", ds_name, role, ref, "definition not supplied")
        if info["parameterized"]:
            # Each invocation binds its own values: Orders and Customers passed to
            # one parameterised dataset are two different objects.
            info = describe_dataset(ds_name, self.store["dataset"][ds_name], params)

        ls = self.named("linkedservice", info["linkedService"])
        if ls:
            self.track_ref("linked_service", ls, ref)
            self.node("linked_service", ls, role, ref,
                      detail=self.ls_info.get(ls, {}).get("system", ""))
        endpoint = self._ls_endpoint(ls)
        for k in ("schema", "object", "path", "url", "container"):
            if info["endpoint"].get(k) is not None:
                endpoint[k] = info["endpoint"][k]

        detail_bits = [b for b in (info["type"], ls) if b]
        if params:
            detail_bits.append("params: " + squeeze(json.dumps(params, default=str), 120))
        detail = " | ".join(detail_bits)

        display = info["display"]
        if display and not info["dynamic"]:
            if endpoint["object"]:
                return self.node("table", display, role, ref, detail,
                                 alias=ds_name, endpoint=endpoint, scope=ls)
            return self.node("file_path", display, role, ref, detail,
                             alias=ds_name, endpoint=endpoint, scope=ls)
        # dynamic or unresolved — keep the dataset itself as the node
        if display:
            detail = (detail + " | resolves to: " + squeeze(display, 120)).strip(" |")
        key = self.node("dataset", ds_name, role, ref, detail, endpoint=endpoint)
        if key and info["dynamic"]:
            self.entities[key]["dynamic"] = True
        return key

    def dataset_connection(self, ds_name: str, ref: str) -> Tuple[dict, str]:
        """The connection a dataset supplies, without claiming its own table.

        Used when a query replaces the dataset's table: the query names the
        data, the dataset only says which server/database it runs against.
        """
        ds_name = self.named("dataset", ds_name)
        self.track_ref("dataset", ds_name, ref)
        info = self.ds_info.get(ds_name)
        if not info:
            self._incomplete.append(f"dataset {ds_name}")
            return dict(EMPTY_ENDPOINT), ""
        ls = self.named("linkedservice", info["linkedService"])
        if ls:
            self.track_ref("linked_service", ls, ref)
            self.node("linked_service", ls, "connection for query", ref,
                      detail=self.ls_info.get(ls, {}).get("system", ""))
        return self._ls_endpoint(ls), ls

    def sql_nodes(self, sql: str, ref: str, read_role="read (SQL)",
                  write_role="written (SQL)", context: Optional[dict] = None,
                  scope: str = "") -> Tuple[List[str], List[str]]:
        reads, writes, procs = harvest_sql(sql)
        base = {k: (context or {}).get(k) for k in ("system", "server", "database", "url")}

        def endpoint(name):
            parts = [x for x in re.split(r"\.(?![^\[]*\])", name) if x]
            ep = {**EMPTY_ENDPOINT, **base,
                  "schema": split_table(name)[0], "object": split_table(name)[1]}
            if len(parts) >= 3:     # database named in the query wins
                ep["database"] = parts[-3].strip("[]\"`")
            return ep

        rk = [self.node("table", t, read_role, ref, endpoint=endpoint(t), scope=scope)
              for t in reads]
        wk = [self.node("table", t, write_role, ref, endpoint=endpoint(t), scope=scope)
              for t in writes]
        for p in procs:
            self.node("stored_procedure", p, "executed", ref, endpoint=endpoint(p), scope=scope)
        return [k for k in rk if k], [k for k in wk if k]

    def edge(self, sources, sinks, mechanism, pipeline, activity,
             detail="", opaque=False, dynamic=False) -> None:
        sources = [s for s in dict.fromkeys(sources) if s]
        sinks = [s for s in dict.fromkeys(sinks) if s]
        if not sources and not sinks:
            return
        # An edge touching a runtime-resolved object is itself uncertain.
        dynamic = dynamic or any(self.entities.get(k, {}).get("dynamic")
                                 for k in sources + sinks)
        self.edges.append({
            "sources": sources, "sinks": sinks, "mechanism": mechanism,
            "pipeline": pipeline, "activity": activity,
            "detail": squeeze(detail, 300), "opaque": opaque, "dynamic": dynamic,
        })

    # ---- pipelines ------------------------------------------------------

    def run(self) -> dict:
        # datasets reference linked services even when no pipeline uses them —
        # count those references so unused-detection is about the factory, not
        # about which pipelines happened to be supplied
        for ds_name, info in self.ds_info.items():
            if info["linkedService"]:
                self.track_ref("linked_service", info["linkedService"], f"dataset {ds_name}")

        def ls_refs(node):
            if isinstance(node, dict):
                if node.get("type") == "LinkedServiceReference" and node.get("referenceName"):
                    yield node["referenceName"]
                for v in node.values():
                    yield from ls_refs(v)
            elif isinstance(node, list):
                for v in node:
                    yield from ls_refs(v)
        for ls_name, props in self.store["linkedservice"].items():
            for other in ls_refs(obj(props.get("typeProperties"))):
                self.track_ref("linked_service", other, f"linked service {ls_name}")

        for name, props in sorted(self.store["pipeline"].items()):
            self.pipelines.append(self.walk_pipeline(name, props))
        for name in sorted(self.store["dataflow"]):
            if name not in self._df_done:
                self.document_dataflow(name, "(not referenced by any supplied pipeline)", "", "")
        self.build_graph()
        self.rollups()
        self.checks()
        return self.as_dict()


def analyze(store: Dict[str, Dict[str, dict]]) -> dict:
    redactions = scrub_definitions(store)
    result = Analyzer(store).run()
    result["redactions"] = redactions
    return result
