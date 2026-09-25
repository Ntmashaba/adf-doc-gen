"""Linking data flows into lineage: endpoints and per-invocation edges."""
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


class DataflowMixin:
    """Mixed into Analyzer; uses its registry (node, edge, warn, track_ref...)."""


    # ---- data flows -----------------------------------------------------

    def _df_endpoints(self, tp: dict, ref: str,
                      stream_params: Optional[dict] = None,
                      configs: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, dict]]:
        eps: Dict[str, Dict[str, dict]] = {"source": {}, "sink": {}}
        for key, bucket in (("sources", "source"), ("sinks", "sink")):
            role = f"dataflow {bucket}"
            for item in lst(tp.get(key)):
                nm = item.get("name", "?")
                ds = obj(item.get("dataset")).get("referenceName", "")
                ls = obj(item.get("linkedService")).get("referenceName", "")
                fl = obj(item.get("flowlet")).get("referenceName", "")
                node_key, label = "", nm
                if ds:
                    # Parameters set on the flow's source/sink, overridden by the
                    # values the calling activity passes for that stream.
                    params = {**obj(obj(item.get("dataset")).get("parameters")),
                              **obj(obj(stream_params).get(nm))}
                    node_key = self.dataset_node(ds, role, ref, params or None)
                    label = self.entities.get(node_key, {}).get("label", ds)
                elif ls:
                    self.track_ref("linked_service", ls, ref)
                    ls = self.named("linkedservice", ls)
                    ep = self._ls_endpoint(ls)
                    inline = inline_dataflow_endpoint((configs or {}).get(nm, ""))
                    if inline["query"] and not inline["dynamic"]:
                        r_k, _ = self.sql_nodes(inline["query"], ref, read_role=role + " (query)",
                                                context=ep, scope=ls)
                        node_key = r_k[0] if len(r_k) == 1 else ""
                        extra = r_k if len(r_k) > 1 else []
                        label = self.entities[node_key]["label"] if node_key else f"query via {ls}"
                        if extra:   # several tables: keep them all on the stream
                            eps[bucket][nm] = {"key": extra[0], "keys": extra, "label": label,
                                               "dataset": "", "linkedService": ls, "flowlet": fl}
                            continue
                    elif inline["label"] and not inline["dynamic"]:
                        ep = {**ep, **{k: inline[k] for k in ("schema", "object", "container", "path")
                                       if inline[k]}}
                        node_key = self.node(inline["kind"], inline["label"], role, ref,
                                             detail=f"inline in data flow, via {ls}",
                                             endpoint=ep, scope=ls)
                        label = inline["label"]
                    else:
                        # Target named by a data-flow parameter, or not stated:
                        # keep it per stream so different streams never merge.
                        label = f"{nm} via {ls}" + (f" ({inline['label']})" if inline["label"] else "")
                        node_key = self.node("inline_dataset", label, role, ref,
                                             detail="inline in data flow; target set at runtime"
                                             if inline["dynamic"] else "inline in data flow",
                                             endpoint=ep, scope=ls)
                        if inline["dynamic"]:
                            self.entities[node_key]["dynamic"] = True
                if fl:
                    self.track_ref("dataflow", fl, ref)
                eps[bucket][nm] = {"key": node_key, "label": label,
                                   "dataset": ds, "linkedService": ls, "flowlet": fl}
        return eps

    def document_dataflow(self, df_name: str, ref: str,
                          pipeline: str = "", activity: str = "",
                          activity_sinks: Optional[List[str]] = None,
                          stream_params: Optional[dict] = None) -> Optional[dict]:
        if not df_name:
            return None
        df_name = self.named("dataflow", df_name)
        props = self.store["dataflow"].get(df_name)
        if not props:
            return None
        # A shared data flow is parsed once but linked for every invocation, so
        # each calling pipeline gets its own references and movement edges.
        existing = next((d for d in self.dataflows if d["name"] == df_name), None)
        first = existing is None
        self._df_done.add(df_name)

        tp = obj(props.get("typeProperties"))
        wrangling = props.get("type") == "WranglingDataFlow"
        # Without a data flow script (Power Query flows, older flows that list
        # their transformations), internal lineage is unknown: every source may
        # feed every sink. Say so rather than calling steps dead.
        scripted = not wrangling
        steps = parse_dataflow_script(tp) if scripted else []
        if wrangling:
            steps = ([{"name": s.get("name", "?"), "op": "source", "inputs": [],
                       "inputs_raw": [], "streams": [], "config": ""}
                      for s in lst(tp.get("sources"))]
                     + [{"name": "Power Query", "op": "powerQuery",
                         "inputs": [s.get("name", "?") for s in lst(tp.get("sources"))],
                         "inputs_raw": [], "streams": [], "config": squeeze(tp.get("script"), 400)}])
        elif not steps:
            scripted = False
            steps = ([{"name": s.get("name", "?"), "op": "source", "inputs": [],
                       "inputs_raw": [], "streams": [], "config": ""}
                      for s in lst(tp.get("sources"))]
                     + [{"name": t.get("name", "?"), "op": "transformation", "inputs": [],
                         "inputs_raw": [], "streams": [], "config": ""}
                        for t in lst(tp.get("transformations"))]
                     + [{"name": s.get("name", "?"), "op": "sink", "inputs": [],
                         "inputs_raw": [], "streams": [], "config": ""}
                        for s in lst(tp.get("sinks"))])
            if first:
                self.warn("info", "data flow",
                          f"Data flow '{df_name}' lists its transformations without a script; "
                          f"each sink is assumed to depend on every source.")

        configs = {st["name"]: st.get("config", "") for st in steps}
        eps = self._df_endpoints(tp, ref, stream_params, configs)
        order = dataflow_exec_order(steps)
        dead = dataflow_dead_ends(steps) if scripted else []
        sink_names = list(eps["sink"]) or [s["name"] for s in steps if s["op"] == "sink"]
        traces = trace_dataflow(steps, sink_names) if scripted else []

        for sink, sources, chain in traces:
            ends = lambda ep: ep.get("keys") or ([ep["key"]] if ep.get("key") else [])
            src_keys = [k for s in sources for k in ends(eps["source"].get(s, {}))]
            snk_keys = ends(eps["sink"].get(sink, {}))
            self.edge(src_keys, snk_keys,
                      f"DataFlow:{df_name}", pipeline or "(unreferenced)",
                      activity or df_name, " → ".join(chain))
        if not scripted:
            all_sources = [k for v in eps["source"].values() for k in (v.get("keys") or [v["key"]]) if k]
            all_sinks = [v["key"] for v in eps["sink"].values() if v["key"]] + list(activity_sinks or [])
            self.edge(all_sources, all_sinks, f"DataFlow:{df_name}", pipeline or "(unreferenced)",
                      activity or df_name,
                      "Power Query mash-up" if wrangling else "no script: every source may feed every sink")

        if not first:
            if ref not in existing["usedBy"]:
                existing["usedBy"].append(ref)
            return existing
        doc = {
            "name": df_name,
            "description": squeeze(props.get("description"), 300) or None,
            "usedBy": [ref],
            "sources": [{"stream": k, **{kk: vv for kk, vv in v.items() if kk not in ("key", "keys")}}
                        for k, v in eps["source"].items()],
            "sinks": [{"stream": k, **{kk: vv for kk, vv in v.items() if kk not in ("key", "keys")}}
                      for k, v in eps["sink"].items()],
            "sourceKeys": [k for v in eps["source"].values() for k in (v.get("keys") or [v["key"]]) if k],
            "sinkKeys": [v["key"] for v in eps["sink"].values() if v["key"]],
            "steps": [{
                "execOrder": order.get(s["name"], 0),
                "name": s["name"],
                "op": s["op"],
                "inputs": s.get("inputs_raw") or s["inputs"],
                "outputStreams": s["streams"],
                "deadEnd": any(d.startswith(s["name"]) for d in dead),
                "config": squeeze(s["config"], 400),
            } for s in steps],
            "sinkTraces": [{"sink": s, "sources": src, "chain": ch} for s, src, ch in traces],
            "deadEnds": dead,
        }
        if dead:
            self.warn("warning", "data flow",
                      f"Data flow '{df_name}' has unconsumed output: {', '.join(dead)} "
                      f"(dead logic, or a connection that was never made).")
        self.dataflows.append(doc)
        return doc
