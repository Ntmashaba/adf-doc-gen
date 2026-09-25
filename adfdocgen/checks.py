"""Reference resolution and warnings."""
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


class ChecksMixin:
    """Mixed into Analyzer; uses its registry (node, edge, warn, track_ref...)."""


    # ---- checks ---------------------------------------------------------

    def checks(self) -> None:
        self.resolution = []
        for (kind, ref_name), refs in sorted(self.refs.items()):
            resolved = ref_name in self.store[self.RESOLVABLE[kind]]
            deploy_param = arm_parameter(ref_name)
            if deploy_param and not resolved:
                # Templates leave connections to be chosen at deployment.
                self.resolution.append({
                    "kind": kind, "name": ref_name, "status": "deployment parameter",
                    "impact": f"chosen when the template is deployed (parameter '{deploy_param}')",
                    "referencedBy": sorted(refs),
                })
                continue
            self.resolution.append({
                "kind": kind, "name": ref_name,
                "status": "resolved" if resolved else "MISSING",
                "impact": "" if resolved else self.IMPACT[kind],
                "referencedBy": sorted(refs),
            })
            if not resolved:
                self.warn("warning", "unresolved reference",
                          f"Unresolved {kind} '{ref_name}' — {self.IMPACT[kind]}")

        for p in self.pipelines:
            for a in p["activities"]:
                loc = f"{p['name']}::{a['activity']}"
                if a.get("dynamic"):
                    self.warn_at(loc, "info", "dynamic target",
                              f"{loc} builds its target dynamically — the actual "
                              f"table/path is only known at runtime.")
                if a.get("opaque"):
                    self.warn_at(loc, "info", "opaque",
                              f"{loc} delegates to external code ({a['type']}) — "
                              f"ADF cannot see what it reads or writes.")
                if a.get("inactive"):
                    self.warn_at(loc, "warning", "inactive activity",
                              f"{loc} is set Inactive — it is skipped at runtime.")
                for dep in a["dependsOn"]:
                    conds = set((dep["on"] or "").split(","))
                    if conds & {"Failed", "Skipped"}:
                        self.warn_at(loc, "info", "error path",
                                  f"{loc} runs on '{dep['on']}' of {dep['activity']} — "
                                  f"error-handling path, not the happy path.")
                    elif "Completed" in conds:
                        self.warn_at(loc, "info", "error path",
                                  f"{loc} runs on 'Completed' of {dep['activity']} — "
                                  f"it executes whether {dep['activity']} succeeds or fails.")
                if a["step"] == 0:
                    self.warn_at(loc, "warning", "cycle",
                              f"{loc} is in a dependency cycle.")
                if a["category"] in ("movement", "transform") and not a.get("retry"):
                    self._no_retry.append(loc)

        if self._no_retry:
            self.warn("info", "resilience", resource="", message=
                      f"{len(self._no_retry)} data/transform activities have no retry "
                      f"policy: " + ", ".join(self._no_retry[:10])
                      + (" …" if len(self._no_retry) > 10 else ""))

        # unused resources — factory-scope claims only, worded accordingly
        for ds_name in sorted(self.store["dataset"]):
            if ("dataset", ds_name) not in self.refs:
                self.warn("warning", "unreferenced resource",
                          f"Dataset '{ds_name}' is not referenced by any supplied "
                          f"pipeline or data flow. It may still be used by pipelines "
                          f"not included in this input.")
        for ls_name in sorted(self.store["linkedservice"]):
            if ("linked_service", ls_name) not in self.refs:
                self.warn("warning", "unreferenced resource",
                          f"Linked service '{ls_name}' is not referenced by any "
                          f"supplied dataset, pipeline, or data flow.")
        for df in self.dataflows:
            if all(u.startswith("(not referenced") for u in df["usedBy"]):
                self.warn("warning", "unreferenced resource",
                          f"Data flow '{df['name']}' is not invoked by any supplied "
                          f"pipeline.")
        if self.store["trigger"] or len(self.pipelines) > 1:
            for p in self.pipelines:
                if p["entryPoint"] and not p["invokes"] and len(self.pipelines) > 1:
                    self.warn("info", "no known invoker",
                              f"Pipeline '{p['name']}' is not started by any trigger or "
                              f"pipeline in this factory. It may be invoked externally "
                              f"(REST API, Synapse, Logic Apps) or be dead.")
        for t in getattr(self, "triggers", []):
            if t["state"] and t["state"].lower() not in ("started", "unknown"):
                self.warn("warning", "trigger state",
                          f"Trigger '{t['name']}' is {t['state']} — the pipelines it "
                          f"references do not run on this schedule/event until it is started.")
            if not t["startsPipelines"]:
                self.warn("info", "trigger state",
                          f"Trigger '{t['name']}' starts no pipelines.")

        for ls_name, info in sorted(self.ls_info.items()):
            if info["inlineCredential"]:
                self.warn("warning", "credential hygiene",
                          f"Linked service '{ls_name}' appears to contain an inline "
                          f"credential in its connection string (value not shown here). "
                          f"Prefer Key Vault or managed identity.")

        for key, ent in self.entities.items():
            if ent["kind"] in ("table", "file_path") and not self.upstream.get(key) \
                    and not self.downstream.get(key):
                self.warn("info", "isolated entity",
                          f"{ent['label']} is touched but participates in no data "
                          f"movement (read-only reference or metadata-only use).",
                          resource=ent["label"])

        for fname, meta in obj(self.store.get("__skipped__")).items():
            self.warn("warning", "input",
                      f"Input file '{fname}' could not be parsed and was skipped: "
                      f"{meta['error']}")

        for dup in obj(self.store.get("__input__")).get("duplicates", []):
            self.warn("warning", "input",
                      f"{dup['kind'].capitalize()} '{dup['name']}' is defined more than once "
                      f"({', '.join(dup['files'])}); only the last definition was used.")

        sev_order = {"warning": 0, "info": 1}
        seen = set()
        deduped = []
        for w in sorted(self.warnings, key=lambda w: (sev_order.get(w["severity"], 2),
                                                      w["category"], w["message"])):
            sig = (w["severity"], w["category"], w["message"])
            if sig not in seen:
                seen.add(sig)
                deduped.append(w)
        self.warnings = deduped
