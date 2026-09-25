"""Serialising the analysis: the payload every output is rendered from."""
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


class OutputMixin:
    """Mixed into Analyzer; uses its registry (node, edge, warn, track_ref...)."""

    # ---- serialise ------------------------------------------------------

    def as_count(self, what: str) -> int:
        return {"pipelines": len(self.pipelines),
                "activities": sum(p["activityCount"] for p in self.pipelines),
                "dataflows": len(self.store["dataflow"]),
                "datasets": len(self.store["dataset"]),
                "linkedServices": len(self.store["linkedservice"]),
                "triggers": len(self.store["trigger"])}[what]

    def as_dict(self) -> dict:
        ents = []
        for key, e in sorted(self.entities.items()):
            ents.append({
                "key": key, "kind": e["kind"], "label": e["label"],
                "aliases": sorted(e["aliases"]), "roles": sorted(e["roles"]),
                "detail": e["detail"], "dynamic": e["dynamic"],
                "endpoint": e["endpoint"],
                "referencedBy": sorted(e["refs"]),
                "upstream": self.upstream.get(key, []),
                "downstream": self.downstream.get(key, []),
            })

        datasets = []
        for name in sorted(self.store["dataset"]):
            info = self.ds_info[name]
            refs = sorted(self.refs.get(("dataset", name), []))
            datasets.append({**info, "referencedBy": refs, "unreferenced": not refs})

        linked_services = []
        for name in sorted(self.store["linkedservice"]):
            info = self.ls_info[name]
            refs = sorted(self.refs.get(("linked_service", name), []))
            linked_services.append({**info, "referencedBy": refs, "unreferenced": not refs})

        orch_edges = []
        for t in self.triggers:
            for pl in t["startsPipelines"]:
                orch_edges.append({"from": t["name"], "fromKind": "trigger",
                                   "to": pl, "toKind": "pipeline",
                                   "detail": t["type"] + (f" ({t['state']})" if t["state"] else "")})
        for c in self.pipeline_calls:
            orch_edges.append({"from": c["parent"], "fromKind": "pipeline",
                               "to": c["child"], "toKind": "pipeline",
                               "detail": f"ExecutePipeline via {c['activity']}"
                                         + ("" if c["waitOnCompletion"] else " (fire-and-forget)")})
        for p in self.pipelines:
            for df in p["dataflows"]:
                orch_edges.append({"from": p["name"], "fromKind": "pipeline",
                                   "to": df, "toKind": "dataflow",
                                   "detail": "ExecuteDataFlow"})

        origins = obj(self.store.get("__origins__"))
        src = lambda kind, name: obj(origins.get(kind)).get(name)
        for p in self.pipelines:
            p["source"] = src("pipeline", p["name"])
        for d in self.dataflows:
            d["source"] = src("dataflow", d["name"])
        for t in self.triggers:
            t["source"] = src("trigger", t["name"])
        for d in datasets:
            d["source"] = src("dataset", d["name"])
        for l in linked_services:
            l["source"] = src("linkedservice", l["name"])

        # Who ultimately starts each pipeline: triggers through any chain of parents.
        parents = defaultdict(set)
        for c in self.pipeline_calls:
            parents[c["child"]].add(c["parent"])
        by_name = {p["name"]: p for p in self.pipelines}

        def ancestry(name):
            seen, stack = set(), [name]
            while stack:
                for par in parents.get(stack.pop(), ()):
                    if par not in seen:
                        seen.add(par)
                        stack.append(par)
            return seen
        for p in self.pipelines:
            anc = ancestry(p["name"])
            p["orchestratedBy"] = sorted(anc)
            p["startedBy"] = sorted({t for q in anc | {p["name"]}
                                     for t in (by_name.get(q) or {}).get("triggers", [])})

        # Reverse usage: every activity that reads, writes, deletes or runs an object.
        usage = defaultdict(list)
        for p in self.pipelines:
            for a in p["activities"]:
                for key, op in [(k, "read") for k in a["reads"]] + [(k, "write") for k in a["writes"]]:
                    kind = self.entities.get(key, {}).get("kind")
                    if a["type"] == "Delete" and op == "write":
                        op = "delete"
                    elif kind in ("stored_procedure", "notebook", "code_artifact"):
                        op = "execute"
                    elif kind == "endpoint":
                        op = "call"
                    usage[key].append({"pipeline": p["name"], "activity": a["activity"],
                                       "type": a["type"], "operation": op,
                                       "dynamic": bool(a.get("dynamic")),
                                       "opaque": bool(a.get("opaque"))})
        for d in self.dataflows:
            for key in d["sourceKeys"] + d["sinkKeys"]:
                if not usage.get(key):
                    usage[key].append({"pipeline": "", "activity": "", "type": "data flow",
                                       "operation": "read" if key in d["sourceKeys"] else "write",
                                       "dataflow": d["name"], "dynamic": False, "opaque": False})
        for e in ents:
            e["usage"] = usage.get(e["key"], [])
            pls = sorted({u["pipeline"] for u in e["usage"] if u["pipeline"]})
            e["pipelines"] = pls
            e["triggers"] = sorted({t for pl in pls for t in (by_name.get(pl) or {}).get("startedBy", [])})
            e["linkedServices"] = sorted({self.ds_info[a]["linkedService"] for a in e["aliases"]
                                          if a in self.ds_info and self.ds_info[a]["linkedService"]}
                                         | ({self.entities[e["key"]]["scope"]}
                                            if self.entities[e["key"]].get("scope") else set()))

        # Review issues: warnings with a priority, explanation and next action.
        issues = []
        for w in self.warnings:
            if w["severity"] == "info" and w["category"] not in ISSUE_GUIDE:
                continue
            prio, why, action = ISSUE_GUIDE.get(w["category"], ("medium", "", "Review."))
            issues.append({**w, "priority": prio, "why": why, "action": action})
        rank = {"high": 0, "medium": 1, "low": 2}
        issues.sort(key=lambda i: (rank[i["priority"]], i["category"], i["resource"]))

        n_missing = sum(1 for r in self.resolution if r["status"] == "MISSING")
        inp = obj(self.store.get("__input__"))
        skipped = len(obj(self.store.get("__skipped__")))
        acts = [a for p in self.pipelines for a in p["activities"]]
        coverage = {
            "inputFormats": inp.get("formats", []),
            "jsonFiles": inp.get("jsonFiles"),
            "skippedFiles": skipped,
            "duplicates": inp.get("duplicates", []),
            "unrecognised": inp.get("unrecognised", []),
            "deploymentValuesUsed": inp.get("deploymentValuesUsed", []),
            "unresolvedReferences": n_missing,
            "deploymentParameters": sum(1 for r in self.resolution
                                        if r["status"] == "deployment parameter"),
            "activities": len(acts),
            "opaqueActivities": sum(1 for a in acts if a.get("opaque")),
            "dynamicActivities": sum(1 for a in acts if a.get("dynamic")),
            "unknownFootprints": sum(1 for a in acts if a.get("footprintUnknown")),
            "runtimeHealth": "not available: generated from definitions, not run history",
        }
        # "factory" is a claim that nothing is missing. Skipped files or
        # unresolved references make it partial; loose files that happen to be
        # self-contained are a selection, not a proven whole factory.
        whole = set(coverage["inputFormats"]) & {"ARM template", "Git folder"}
        if n_missing or skipped:
            mode = "partial"
        elif whole:
            mode = "factory"
        else:
            mode = "selection"

        # A small, stable summary for the documentation home and cross-tool joins.
        systems = defaultdict(lambda: {"objects": set(), "read": 0, "written": 0})
        for e in ents:
            if e["kind"] not in ("table", "file_path", "stored_procedure", "inline_dataset", "dataset"):
                continue
            ep = e["endpoint"]
            sk = (ep.get("system") or "", ep.get("server") or ep.get("url") or "",
                  ep.get("database") or ep.get("container") or "")
            systems[sk]["objects"].add(e["label"])
            ops = {u["operation"] for u in e["usage"]}
            systems[sk]["read"] += "read" in ops
            systems[sk]["written"] += bool(ops & {"write", "delete"})
        summary = {
            "counts": {k: self.as_count(k) for k in ("pipelines", "activities", "dataflows",
                                                     "datasets", "linkedServices", "triggers")},
            "sources": [{"system": k[0], "server": k[1], "database": k[2],
                         "objects": sorted(v["objects"])[:200],
                         "read": v["read"], "written": v["written"]}
                        for k, v in sorted(systems.items())],
            "issues": {p: sum(1 for i in issues if i["priority"] == p) for p in ("high", "medium", "low")},
            "unresolved": n_missing,
        }

        return {
            "schemaVersion": SCHEMA_VERSION,
            "mode": mode,
            "coverage": coverage,
            "summary": summary,
            "issues": issues,
            "factory": {
                "pipelineCount": len(self.pipelines),
                "datasetCount": len(self.store["dataset"]),
                "dataflowCount": len(self.store["dataflow"]),
                "linkedServiceCount": len(self.store["linkedservice"]),
                "triggerCount": len(self.store["trigger"]),
                "activityCount": sum(p["activityCount"] for p in self.pipelines),
                "entityCount": len(self.entities),
                "lineageEdgeCount": len(self.edges),
                "unresolvedCount": n_missing,
                "warningCount": sum(1 for w in self.warnings if w["severity"] == "warning"),
            },
            "pipelines": self.pipelines,
            "pipelineCalls": self.pipeline_calls,
            "dataflows": self.dataflows,
            "datasets": datasets,
            "linkedServices": linked_services,
            "triggers": self.triggers,
            "entities": ents,
            "lineageEdges": [
                {"sources": e["sources"], "sinks": e["sinks"],
                 "mechanism": e["mechanism"], "pipeline": e["pipeline"],
                 "activity": e["activity"], "detail": e["detail"],
                 "opaque": e["opaque"], "dynamic": e["dynamic"]}
                for e in self.edges
            ],
            "orchestration": {"edges": orch_edges},
            "resolution": self.resolution,
            "warnings": self.warnings,
        }
