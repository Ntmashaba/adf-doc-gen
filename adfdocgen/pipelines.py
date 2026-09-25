"""Walking pipelines and reading what each activity reads, writes and runs."""
from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

from .common import arm_parameter, obj, lst, as_text, squeeze, is_dynamic, canon_table, _norm_host, physical_key, split_table, EMPTY_ENDPOINT  # noqa: F401
from .activities import ACTIVITY_CATALOG, OPAQUE_TYPES  # noqa: F401
from .resources import (describe_dataset, describe_linked_service, describe_trigger,  # noqa: F401
                        is_sql_source, sql_names_dynamic)
from .dataflow_script import (dataflow_dead_ends, dataflow_exec_order,  # noqa: F401
                              inline_dataflow_endpoint, parse_dataflow_script, trace_dataflow)
from .issues import ISSUE_GUIDE, SCHEMA_VERSION  # noqa: F401
from .sql_harvest import harvest_sql  # noqa: F401


class PipelineMixin:
    """Mixed into Analyzer; uses its registry (node, edge, warn, track_ref...)."""


    def walk_pipeline(self, name: str, props: dict) -> dict:
        acts: List[dict] = []
        self._walk(name, lst(props.get("activities")), "(root)", 0, acts)
        cats: Dict[str, int] = defaultdict(int)
        for a in acts:
            cats[a["category"]] += 1
        return {
            "name": name,
            "description": squeeze(props.get("description"), 400) or None,
            "folder": (obj(props.get("folder")).get("name")) or None,
            "parameters": {k: (v or {}).get("type", "?")
                           for k, v in obj(props.get("parameters")).items()},
            "variables": {k: (v or {}).get("type", "?")
                          for k, v in obj(props.get("variables")).items()},
            "concurrency": props.get("concurrency"),
            "activityCount": len(acts),
            "categories": dict(sorted(cats.items())),
            "activities": acts,
        }

    def _walk(self, pipeline, acts, scope, depth, sink_list) -> None:
        order = self._sequence(acts)
        for act in acts:
            row = self._activity(pipeline, act, scope, depth)
            row["step"] = order.get(row["activity"], "?")
            row["parallelWith"] = [n for n, s in order.items()
                                   if s == row["step"] and n != row["activity"]]
            sink_list.append(row)
            self._recurse(pipeline, act, scope, depth, sink_list)

    def _recurse(self, pipeline, act, scope, depth, sink_list) -> None:
        name, atype = act.get("name", "?"), act.get("type", "?")
        tp = obj(act.get("typeProperties"))
        base = name if scope == "(root)" else f"{scope} > {name}"
        if atype in ("ForEach", "Until"):
            self._walk(pipeline, lst(tp.get("activities")),
                       f"{base} [{atype}]", depth + 1, sink_list)
        elif atype == "IfCondition":
            self._walk(pipeline, lst(tp.get("ifTrueActivities")),
                       f"{base} [True]", depth + 1, sink_list)
            self._walk(pipeline, lst(tp.get("ifFalseActivities")),
                       f"{base} [False]", depth + 1, sink_list)
        elif atype == "Switch":
            for case in lst(tp.get("cases")):
                self._walk(pipeline, lst(case.get("activities")),
                           f"{base} [Case={case.get('value','?')}]", depth + 1, sink_list)
            self._walk(pipeline, lst(tp.get("defaultActivities")),
                       f"{base} [Default]", depth + 1, sink_list)

    @staticmethod
    def _sequence(acts: List[dict]) -> Dict[str, int]:
        """BFS-levelled topological order: same step == can run in parallel."""
        names = [a.get("name", "?") for a in acts]
        idx = {n: i for i, n in enumerate(names)}
        indeg = {n: 0 for n in names}
        succ = defaultdict(list)
        for a in acts:
            for dep in lst(a.get("dependsOn")):
                parent = dep.get("activity")
                if parent in indeg:
                    indeg[a.get("name", "?")] += 1
                    succ[parent].append(a.get("name", "?"))
        q = deque(sorted([n for n in names if indeg[n] == 0], key=idx.get))
        order, step = {}, 0
        while q:
            batch = sorted(q, key=idx.get)
            q.clear()
            step += 1
            for n in batch:
                order[n] = step
                for s in succ[n]:
                    indeg[s] -= 1
                    if indeg[s] == 0:
                        q.append(s)
        for n in names:
            order.setdefault(n, 0)   # 0 == cycle
        return order

    # ---- one activity ---------------------------------------------------

    def _activity(self, pipeline: str, act: dict, scope: str, depth: int) -> dict:
        name, atype = act.get("name", "?"), act.get("type", "?")
        tp = obj(act.get("typeProperties"))
        cat, default_desc = ACTIVITY_CATALOG.get(atype, ("other", ""))
        ref = f"{pipeline}::{name}"
        policy = obj(act.get("policy"))
        self._incomplete = []
        first_edge = len(self.edges)
        detail, reads, writes, extras = self._extract(pipeline, name, act, atype, tp, ref)
        incomplete = list(dict.fromkeys(self._incomplete))
        row = {
            "activity": name, "type": atype, "category": cat, "scope": scope,
            "depth": depth,
            "description": squeeze(act.get("description"), 300) or default_desc,
            "reads": sorted({k for k in reads if k in self.entities}),
            "writes": sorted({k for k in writes if k in self.entities}),
            "detail": squeeze(detail, 400),
            "dependsOn": [
                {"activity": d.get("activity"),
                 "on": ",".join(lst(d.get("dependencyConditions")))}
                for d in lst(act.get("dependsOn"))
            ],
        }
        dyn_extract = extras.pop("_dyn", False)
        row.update(extras)
        # Parameter passing is normal; a dynamically built *data target* (query,
        # script, or dataset path) is what makes static lineage unreliable —
        # only flag the latter.
        touches_dynamic_entity = any(self.entities.get(k, {}).get("dynamic")
                                     for k in row["reads"] + row["writes"])
        if dyn_extract or touches_dynamic_entity:
            row["dynamic"] = True
        if atype in OPAQUE_TYPES or any(e["opaque"] for e in self.edges[first_edge:]):
            row["opaque"] = True
        if incomplete:
            row["incomplete"] = incomplete
        if policy.get("retry"):
            row["retry"] = policy["retry"]
        if policy.get("timeout"):
            row["timeout"] = as_text(policy["timeout"])
        if act.get("state") == "Inactive":
            row["inactive"] = True
        return row

    def _extract(self, pipeline, name, act, atype, tp, ref):
        bits, reads, writes = [], [], []
        extras: Dict[str, Any] = {}
        query_text = None
        dyn = False

        if atype == "Copy":
            src, snk = obj(tp.get("source")), obj(tp.get("sink"))
            query = as_text(src.get("sqlReaderQuery") or src.get("oracleReaderQuery")
                            or (src.get("query") if is_sql_source(src.get("type")) else None))
            proc = as_text(src.get("sqlReaderStoredProcedureName"))
            pre = as_text(snk.get("preCopyScript"))
            inputs = [i for i in lst(act.get("inputs")) if i.get("type") == "DatasetReference"]
            outputs = [o for o in lst(act.get("outputs")) if o.get("type") == "DatasetReference"]
            for item in inputs:
                ds = item.get("referenceName", "")
                if query or proc:
                    # The query (or procedure) names the data; the dataset only
                    # supplies the connection it runs on.
                    ctx, ls = self.dataset_connection(ds, ref)
                    r_k, _ = self.sql_nodes(query, ref, read_role="copy source (query)",
                                            context=ctx, scope=ls)
                    if proc:
                        r_k.append(self.node("stored_procedure", proc, "copy source", ref,
                                             endpoint={**ctx, "schema": split_table(proc)[0],
                                                       "object": split_table(proc)[1]},
                                             scope=ls))
                    if not r_k:     # dynamic or unreadable query: say which dataset
                        k = self.node("dataset", self.named("dataset", ds), "copy source (query)",
                                      ref, "query could not be read statically", endpoint=ctx)
                        self.entities[k]["dynamic"] = True
                        r_k = [k]
                    reads += r_k
                    bits.append(f"connection: dataset {ds}")
                else:
                    reads.append(self.dataset_node(ds, "copy source", ref, item.get("parameters")))
            for item in outputs:
                ds = item.get("referenceName", "")
                writes.append(self.dataset_node(ds, "copy sink", ref, item.get("parameters")))
                if pre:
                    ctx, ls = self.dataset_connection(ds, ref)
                    _, w_k = self.sql_nodes(pre, ref, write_role="pre-copy script target",
                                            context=ctx, scope=ls)
                    writes += w_k
            bits.append(f"{src.get('type','?')} → {snk.get('type','?')}")
            if snk.get("writeBehavior") or snk.get("writeMethod"):
                bits.append(f"write: {as_text(snk.get('writeBehavior') or snk.get('writeMethod'))}")
            if query:
                bits.append("source query supplied")
                query_text = query
            if proc:
                bits.append("source proc: " + proc)
            if pre:
                bits.append("pre-copy: " + squeeze(pre, 150))
            if tp.get("enableStaging"):
                bits.append("staged copy")
            tr = obj(tp.get("translator"))
            n_map = len(lst(tr.get("mappings")))
            if n_map:
                bits.append(f"explicit column mapping ({n_map} cols)")
            dyn = sql_names_dynamic(query) or sql_names_dynamic(pre)
            self.edge(reads, writes, "Copy", pipeline, name,
                      squeeze(query or proc, 200), dynamic=dyn)

        elif atype == "Lookup":
            ds = obj(tp.get("dataset")).get("referenceName", "")
            src = obj(tp.get("source"))
            query = as_text(src.get("sqlReaderQuery") or src.get("query"))
            if ds and query and is_sql_source(src.get("type")):
                ctx, ls = self.dataset_connection(ds, ref)
                r_k, _ = self.sql_nodes(query, ref, read_role="lookup (query)",
                                        context=ctx, scope=ls)
                if not r_k:     # dynamic or unreadable query: say which dataset
                    k = self.node("dataset", self.named("dataset", ds), "lookup (query)",
                                  ref, "query could not be read statically", endpoint=ctx)
                    self.entities[k]["dynamic"] = True
                    r_k = [k]
                reads += r_k
            elif ds:
                reads.append(self.dataset_node(ds, "lookup", ref,
                                               obj(tp.get("dataset")).get("parameters")))
            bits.append("first row only" if tp.get("firstRowOnly", True) else "full rowset")
            if query:
                query_text = query
            self.edge(reads, [], "Lookup", pipeline, name, squeeze(query, 150))

        elif atype in ("GetMetadata", "Validation", "Delete"):
            ds = obj(tp.get("dataset")).get("referenceName", "")
            role = {"Delete": "deleted", "GetMetadata": "metadata read",
                    "Validation": "existence check"}[atype]
            if ds:
                key = self.dataset_node(ds, role, ref,
                                        obj(tp.get("dataset")).get("parameters"))
                (writes if atype == "Delete" else reads).append(key)
            if atype == "GetMetadata":
                bits.append("fields: " + ", ".join(as_text(f) for f in lst(tp.get("fieldList"))))
            if atype == "Delete":
                bits.append("destructive")
                self.edge([], writes, "Delete", pipeline, name, "destructive")

        elif atype == "SqlServerStoredProcedure":
            proc = as_text(tp.get("storedProcedureName"))
            ls = obj(act.get("linkedServiceName")).get("referenceName", "")
            pk = self.node("stored_procedure", proc, "executed", ref,
                           detail=f"on {ls}" if ls else "",
                           endpoint={**self._ls_endpoint(ls),
                                     "schema": split_table(proc)[0],
                                     "object": split_table(proc)[1]}, scope=ls)
            if ls:
                self.track_ref("linked_service", ls, ref)
                self.node("linked_service", ls, "proc target", ref)
            writes.append(pk)
            params = obj(tp.get("storedProcedureParameters"))
            bits.append(f"proc: {proc}")
            if params:
                bits.append("params: " + ", ".join(params.keys()))
            self.edge([], [pk], "StoredProcedure", pipeline, name,
                      "logic inside the proc is not visible to ADF", opaque=True)

        elif atype == "Script":
            ls = obj(act.get("linkedServiceName")).get("referenceName", "")
            if ls:
                self.track_ref("linked_service", ls, ref)
                self.node("linked_service", ls, "script target", ref)
            all_r, all_w, texts = [], [], []
            ctx = self._ls_endpoint(ls)
            for s in lst(tp.get("scripts")):
                text = as_text(s.get("text"))
                r_k, w_k = self.sql_nodes(text, ref, read_role="read (script)",
                                          write_role="written (script)",
                                          context=ctx, scope=ls)
                all_r += r_k
                all_w += w_k
                texts.append(f"-- [{s.get('type','Query')}]\n{text}")
                dyn = dyn or sql_names_dynamic(text)
            reads += all_r
            writes += all_w
            bits.append(f"{len(texts)} script block(s)")
            if texts:
                query_text = "\n\n".join(texts)
            self.edge(all_r, all_w, "Script", pipeline, name, dynamic=dyn)

        elif atype == "ExecutePipeline":
            child = self.named("pipeline", obj(tp.get("pipeline")).get("referenceName", ""))
            self.track_ref("pipeline", child, ref)
            wait = tp.get("waitOnCompletion", True)
            self.pipeline_calls.append({"parent": pipeline, "child": child,
                                        "activity": name, "waitOnCompletion": bool(wait)})
            extras["invokesPipeline"] = child
            bits.append(f"invokes: {child}" + ("" if wait else " (fire-and-forget)"))
            params = obj(tp.get("parameters"))
            if params:
                declared = obj(obj(self.store["pipeline"].get(child)).get("parameters"))
                shown = {k: ("[redacted]" if obj(declared.get(k)).get("type") == "SecureString"
                             else v) for k, v in params.items()}
                bits.append("params: " + squeeze(json.dumps(shown, default=str), 250))

        elif atype in ("ExecuteDataFlow", "ExecuteWranglingDataflow"):
            df_ref = tp.get("dataFlow") or obj(tp.get("dataflow"))
            df = df_ref.get("referenceName", "") if isinstance(df_ref, dict) else ""
            self.track_ref("dataflow", df, ref)
            extras["invokesDataflow"] = df
            bits.append(f"data flow: {df}")
            if isinstance(df_ref, dict):
                if df_ref.get("parameters"):
                    bits.append("df params: " + squeeze(json.dumps(df_ref["parameters"], default=str), 250))
                if df_ref.get("datasetParameters"):
                    bits.append("dataset params: " + squeeze(
                        json.dumps(df_ref["datasetParameters"], default=str), 250))
            # Power Query data flows name their sinks on the activity, not in the flow.
            activity_sinks = []
            for sink in obj(tp.get("sinks")).values():
                ds = obj(obj(sink).get("dataset")).get("referenceName", "")
                if ds:
                    activity_sinks.append(self.dataset_node(ds, "dataflow sink", ref))
            doc = self.document_dataflow(df, ref, pipeline, name, activity_sinks,
                                         obj(df_ref.get("datasetParameters"))
                                         if isinstance(df_ref, dict) else None)
            writes += activity_sinks
            if doc:
                reads += doc["sourceKeys"]
                writes += doc["sinkKeys"]
                bits.append(f"{len(doc['steps'])} transformations")
            else:
                bits.append("(definition not supplied — what it reads and writes is unknown)")
                extras["footprintUnknown"] = True
                self._incomplete.append(f"data flow {df}")

        elif atype in OPAQUE_TYPES:
            target = as_text(tp.get("notebookPath") or tp.get("pythonFile")
                             or tp.get("mainClassName") or tp.get("packagePath")
                             or tp.get("notebook") or "")
            kind = "notebook" if "Notebook" in atype else "code_artifact"
            key = self.node(kind, target or f"{atype} in {name}", "executed", ref)
            ls = obj(act.get("linkedServiceName")).get("referenceName", "")
            if ls:
                self.track_ref("linked_service", ls, ref)
                self.node("linked_service", ls, "compute", ref)
            if target:
                bits.append(f"{kind}: {target}")
            extras["footprintUnknown"] = True
            params = tp.get("baseParameters") or obj(tp.get("parameters"))
            if params:
                bits.append("params: " + squeeze(json.dumps(params, default=str), 250))
            self.edge([key], [key], atype, pipeline, name,
                      "reads/writes happen inside external code — inspect the artifact",
                      opaque=True)

        elif atype in ("WebActivity", "WebHook", "AzureFunctionActivity"):
            url = as_text(tp.get("url"))
            fn = as_text(tp.get("functionName"))
            key = self.node("endpoint", fn or url or name, "called", ref,
                            endpoint={**EMPTY_ENDPOINT, "url": url or None,
                                      "system": "Azure Function" if fn else None})
            bits.append(f"{as_text(tp.get('method')) or 'CALL'} {squeeze(fn or url, 200)}")
            self.edge([], [key], atype, pipeline, name, opaque=True)

        elif atype == "ForEach":
            items = as_text(tp.get("items"))
            bits.append(("sequential" if tp.get("isSequential") else "parallel")
                        + (f", batch={tp['batchCount']}" if tp.get("batchCount") else "")
                        + f" over {squeeze(items, 200)}")
        elif atype == "Until":
            bits.append("until " + squeeze(tp.get("expression"), 200))
        elif atype == "IfCondition":
            bits.append("if " + squeeze(tp.get("expression"), 200))
        elif atype == "Switch":
            bits.append("on " + squeeze(tp.get("on"), 150) + " | cases: "
                        + ", ".join(str(c.get("value")) for c in lst(tp.get("cases"))))
        elif atype == "Filter":
            bits.append(f"{squeeze(tp.get('items'),120)} where {squeeze(tp.get('condition'),120)}")
        elif atype in ("SetVariable", "AppendVariable"):
            bits.append(f"{tp.get('variableName','?')} = {squeeze(tp.get('value'), 180)}")
        elif atype == "Wait":
            bits.append(f"wait {as_text(tp.get('waitTimeInSeconds'))}s")
        elif atype == "Fail":
            bits.append("fail: " + squeeze(tp.get("message"), 150))
        elif atype in ("PBISemanticModelRefresh", "RefreshDataflow", "TridentNotebook"):
            ids = [f"{k} {squeeze(tp.get(k), 80)}" for k in
                   ("workspaceId", "groupId", "datasetId", "dataflowId", "notebookId")
                   if tp.get(k) not in (None, "")]
            bits.append(", ".join(ids) or "target not named in the definition")
        elif tp:
            # Unknown activity: list setting names only. Values can hold secrets.
            bits.append("settings: " + ", ".join(sorted(str(k) for k in tp)))

        if query_text:
            extras["query"] = squeeze(query_text, 4000)
        if dyn:
            extras["_dyn"] = True
        return " | ".join(b for b in bits if b), reads, writes, extras
