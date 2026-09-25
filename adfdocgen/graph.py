"""Lineage closure, per-pipeline rollups and orchestration relationships."""
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


class GraphMixin:
    """Mixed into Analyzer; uses its registry (node, edge, warn, track_ref...)."""


    # ---- graph closure --------------------------------------------------

    def build_graph(self) -> None:
        fwd: Dict[str, Set[str]] = defaultdict(set)
        rev: Dict[str, Set[str]] = defaultdict(set)
        for e in self.edges:
            for s in e["sources"]:
                for t in e["sinks"]:
                    if s == t:
                        continue
                    fwd[s].add(t)
                    rev[t].add(s)

        def closure(graph, start):
            seen, q = set(), deque(graph.get(start, ()))
            while q:
                n = q.popleft()
                if n in seen:
                    continue
                seen.add(n)
                q.extend(graph.get(n, ()))
            seen.discard(start)
            return sorted(seen)

        self.upstream = {k: closure(rev, k) for k in self.entities}
        self.downstream = {k: closure(fwd, k) for k in self.entities}

    # ---- rollups: own vs effective reads/writes -------------------------

    def rollups(self) -> None:
        by_name = {p["name"]: p for p in self.pipelines}
        children = defaultdict(list)
        for c in self.pipeline_calls:
            children[c["parent"]].append(c["child"])

        for p in self.pipelines:
            p["ownReads"] = sorted({k for a in p["activities"] for k in a["reads"]})
            p["ownWrites"] = sorted({k for a in p["activities"] for k in a["writes"]})
            p["ownIncomplete"] = sorted({x for a in p["activities"]
                                         for x in a.get("incomplete", [])})

        memo: Dict[str, Tuple[Set[str], Set[str], Set[str]]] = {}

        def effective(name: str, visiting: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
            if name in memo:
                return memo[name]
            if name in visiting:            # recursion cycle — stop here
                return set(), set(), set()
            p = by_name.get(name)
            if p is None:                   # child not supplied
                return set(), set(), {f"pipeline {name}"}
            visiting = visiting | {name}
            r, w, miss = set(p["ownReads"]), set(p["ownWrites"]), set(p["ownIncomplete"])
            for child in children.get(name, []):
                cr, cw, cm = effective(child, visiting)
                r |= cr
                w |= cw
                miss |= cm
            memo[name] = (r, w, miss)
            return memo[name]

        for p in self.pipelines:
            r, w, miss = effective(p["name"], set())
            p["effectiveReads"] = sorted(r)
            p["effectiveWrites"] = sorted(w)
            p["effectiveIncomplete"] = sorted(miss)
            p["opaqueCount"] = sum(1 for a in p["activities"] if a.get("opaque"))
            p["dynamicCount"] = sum(1 for a in p["activities"] if a.get("dynamic"))

        # orchestration relationships
        trigger_starts = defaultdict(list)
        self.triggers = [describe_trigger(n, props)
                         for n, props in sorted(self.store["trigger"].items())]
        for t in self.triggers:
            t["startsPipelines"] = [self.named("pipeline", pl) for pl in t["startsPipelines"]]
            t["parameters"] = {self.named("pipeline", pl): v for pl, v in t["parameters"].items()}
            t["dependsOnTriggers"] = [self.named("trigger", d) for d in t["dependsOnTriggers"]]
        for t in self.triggers:
            for pl in t["startsPipelines"]:
                self.track_ref("pipeline", pl, f"trigger {t['name']}")
                trigger_starts[pl].append(t["name"])

        invoked_by = defaultdict(list)
        for c in self.pipeline_calls:
            invoked_by[c["child"]].append(f"{c['parent']}::{c['activity']}")

        for p in self.pipelines:
            p["triggers"] = sorted(trigger_starts.get(p["name"], []))
            p["invokedBy"] = sorted(set(invoked_by.get(p["name"], [])))
            p["invokes"] = sorted({c["child"] for c in self.pipeline_calls
                                   if c["parent"] == p["name"]})
            p["dataflows"] = sorted({a["invokesDataflow"] for a in p["activities"]
                                     if a.get("invokesDataflow")})
            p["entryPoint"] = not p["triggers"] and not p["invokedBy"]
            p["roleLine"] = self._role_line(p)

    @staticmethod
    def _role_line(p: dict) -> str:
        cats = p["categories"]
        moves = cats.get("movement", 0) + cats.get("transform", 0)
        bits = []
        if p["invokes"]:
            bits.append(f"orchestrates {len(p['invokes'])} pipeline(s)")
        if p["dataflows"]:
            bits.append(f"runs {len(p['dataflows'])} data flow(s)")
        if moves:
            n_src = len(p["effectiveReads"]) if "effectiveReads" in p else len(p["ownReads"])
            n_snk = len(p["effectiveWrites"]) if "effectiveWrites" in p else len(p["ownWrites"])
            bits.append(f"moves/transforms data ({n_src} read, {n_snk} written)")
        if not bits:
            if cats.get("external"):
                bits.append("calls external services")
            elif cats.get("management"):
                bits.append("housekeeping (delete/metadata)")
            else:
                bits.append("control/utility")
        return "; ".join(bits)
