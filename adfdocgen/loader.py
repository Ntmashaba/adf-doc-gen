"""Load ADF JSON from any of the shapes the tool supports.

Accepts
-------
* a single resource JSON (Git / ADF Studio "view code" format) — pipeline,
  dataset, data flow, linked service, or trigger
* an ARM template export (Manage > ARM template > Export)
* a folder: an ADF Git repo root with pipeline/ dataset/ dataflow/
  linkedService/ trigger/ subfolders, or just loose JSON files pulled out
  of ADF Studio object by object (the hand-curated workflow)

The classification heuristics and ARM name-unmangling here are carried over
from the user's field-tested distiller — they encode real shape variety seen
in production exports. Change with care.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, Tuple

KINDS = ("pipeline", "dataset", "dataflow", "linkedservice", "trigger")
# Resources the analysis does not document as data objects, kept so they are
# not mistaken for linked services (an integration runtime also has a type and
# typeProperties) and so references to them can resolve.
SUPPORT_KINDS = ("integrationruntime", "credential", "factory", "managedvirtualnetwork",
                 "managedprivateendpoint", "globalparameter")
# ADF Git repositories keep each resource type in its own folder.
GIT_FOLDERS = {"pipeline": "pipeline", "dataset": "dataset", "dataflow": "dataflow",
               "linkedservice": "linkedservice", "trigger": "trigger",
               "integrationruntime": "integrationruntime", "credential": "credential",
               "factory": "factory", "managedvirtualnetwork": "managedvirtualnetwork",
               "managedprivateendpoint": "managedprivateendpoint"}
ARM_TYPES = (("/pipelines", "pipeline"), ("/datasets", "dataset"), ("/dataflows", "dataflow"),
             ("/linkedservices", "linkedservice"), ("/triggers", "trigger"),
             ("/integrationruntimes", "integrationruntime"), ("/credentials", "credential"),
             ("/managedvirtualnetworks", "managedvirtualnetwork"),
             ("/managedprivateendpoints", "managedprivateendpoint"),
             ("/globalparameters", "globalparameter"))


def load_json(path: str) -> Any:
    # utf-8-sig: ADF Studio and Windows tooling frequently add a BOM
    with open(path, "r", encoding="utf-8-sig") as fh:
        return json.load(fh)


def classify_resource(doc: dict, fallback_name: str, folder_hint: str = "") -> Tuple[str, str, dict]:
    """Best-effort classification of a loose resource JSON.

    Order matters: pipelines are identified by their activities, data flows by
    their explicit type, and datasets are distinguished from linked services by
    the presence of a linkedServiceName reference.
    """
    name = doc.get("name", fallback_name)
    props = doc.get("properties", doc)
    if not isinstance(props, dict):
        return "unknown", name, {}
    rtype = (doc.get("type") or "").lower()
    folder = GIT_FOLDERS.get(folder_hint.lower().rstrip("s"))
    if folder:
        return folder, name, props
    for suffix, kind in ARM_TYPES:
        if rtype.endswith(suffix):
            return kind, name, props
    if "pipelines" in rtype or "activities" in props:
        return "pipeline", name, props
    if "dataflows" in rtype or props.get("type") in ("MappingDataFlow", "WranglingDataFlow", "Flowlet"):
        return "dataflow", name, props
    if "linkedservices" in rtype:
        return "linkedservice", name, props
    if "triggers" in rtype or str(props.get("type", "")).endswith("Trigger"):
        return "trigger", name, props
    if "datasets" in rtype or ("linkedServiceName" in props and "typeProperties" in props):
        return "dataset", name, props
    if "typeProperties" in props and "type" in props and "linkedServiceName" not in props:
        return "linkedservice", name, props
    hint = folder_hint.lower().rstrip("s")
    for kind in KINDS:
        if hint == kind:
            return kind, name, props
    return "unknown", name, props


def collect_inputs(input_path: str) -> Dict[str, Dict[str, dict]]:
    """Return {kind: {name: properties}} for every recognised resource."""
    store: Dict[str, Dict[str, dict]] = defaultdict(dict)
    skipped: list = []

    def ingest_arm(res: dict) -> None:
        rtype = (res.get("type") or "").lower()
        raw = res.get("name", "")
        # ARM names arrive as "[concat(parameters('factoryName'), '/PL_x')]"
        m = (re.search(r"/'?\s*,\s*'([^']+)'\)?\]?$", raw)
             or re.search(r"/([^/'\)\]]+)'?\)?\]?$", raw))
        name = m.group(1) if m else raw
        props = res.get("properties", {})
        if not isinstance(props, dict):
            props = {}
        for suffix, kind in ARM_TYPES:
            if rtype.endswith(suffix):
                store[kind][name] = props
        for sub in res.get("resources", []) or []:
            ingest_arm(sub)

    def ingest_doc(doc: Any, fname: str, folder_hint: str = "") -> None:
        if isinstance(doc, dict) and "resources" in doc and "$schema" in doc:
            for res in doc.get("resources", []) or []:
                ingest_arm(res)
            return
        if isinstance(doc, dict):
            kind, name, props = classify_resource(
                doc, os.path.splitext(fname)[0], folder_hint)
            store[kind][name] = props

    if os.path.isdir(input_path):
        for root, _dirs, files in os.walk(input_path):
            for fname in sorted(files):
                if fname.lower().endswith(".json"):
                    try:
                        ingest_doc(load_json(os.path.join(root, fname)), fname,
                                   os.path.basename(root))
                    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
                        skipped.append((fname, str(exc)))
                        print(f"  ! skipping {fname}: {exc}", file=sys.stderr)
    else:
        ingest_doc(load_json(input_path), os.path.basename(input_path))

    for kind in KINDS + SUPPORT_KINDS + ("unknown",):
        store.setdefault(kind, {})
    store["__skipped__"] = {f: {"error": e} for f, e in skipped}
    return store
