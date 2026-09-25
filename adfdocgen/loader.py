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


_ARM_PARAM = re.compile(r"^\[\s*parameters\(\s*'([^']+)'\s*\)\s*\]$")


def _is_arm_template(doc: Any) -> bool:
    return isinstance(doc, dict) and "resources" in doc and "$schema" in doc


def _is_arm_parameters(doc: Any) -> bool:
    return (isinstance(doc, dict) and "resources" not in doc
            and "deploymentParameters" in str(doc.get("$schema", ""))
            and isinstance(doc.get("parameters"), dict))


def _substitute(node: Any, values: Dict[str, Any], used: set) -> Any:
    """Replace "[parameters('x')]" with a known literal deployment value."""
    if isinstance(node, dict):
        return {k: _substitute(v, values, used) for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute(v, values, used) for v in node]
    if isinstance(node, str):
        m = _ARM_PARAM.match(node)
        if m and m.group(1) in values:
            val = values[m.group(1)]
            if not (isinstance(val, str) and val.startswith("[")):
                used.add(m.group(1))
                return val
    return node


def collect_inputs(input_path: str) -> Dict[str, Dict[str, dict]]:
    """Return {kind: {name: properties}} for every recognised resource.

    Extra keys describe the input itself: __skipped__ (files that failed to
    parse), __input__ (format, file counts, duplicates, deployment values used).
    """
    store: Dict[str, Dict[str, dict]] = defaultdict(dict)
    skipped: list = []
    duplicates: list = []
    formats: set = set()
    param_values: Dict[str, Any] = {}
    used_params: set = set()
    seen_at: Dict[Tuple[str, str], str] = {}
    origins: Dict[str, Dict[str, str]] = defaultdict(dict)

    def put(kind: str, name: str, props: dict, fname: str, where: str = "") -> None:
        key = (kind, name.lower())
        if key in seen_at and kind != "unknown":
            duplicates.append({"kind": kind, "name": name, "files": [seen_at[key], fname]})
        seen_at[key] = fname
        store[kind][name] = props
        # Provenance: which file (and where in it) a definition came from.
        origins[kind][name] = fname + (f" › {where}" if where else "")

    def ingest_arm(res: dict, fname: str, where: str = "") -> None:
        rtype = (res.get("type") or "").lower()
        raw = res.get("name", "")
        # ARM names arrive as "[concat(parameters('factoryName'), '/PL_x')]"
        m = (re.search(r"/'?\s*,\s*'([^']+)'\)?\]?$", raw)
             or re.search(r"/([^/'\)\]]+)'?\)?\]?$", raw))
        name = m.group(1) if m else raw
        props = res.get("properties", {})
        if not isinstance(props, dict):
            props = {}
        props = _substitute(props, param_values, used_params)
        for suffix, kind in ARM_TYPES:
            if rtype.endswith(suffix):
                put(kind, name, props, fname, where)
        for i, sub in enumerate(res.get("resources", []) or []):
            ingest_arm(sub, fname, f"{where}.resources[{i}]")

    def ingest_doc(doc: Any, fname: str, folder_hint: str = "") -> None:
        if _is_arm_parameters(doc):
            return
        if _is_arm_template(doc):
            formats.add("ARM template")
            # Template defaults first; a parameters file overrides them.
            for pname, spec in (doc.get("parameters") or {}).items():
                if isinstance(spec, dict) and "defaultValue" in spec:
                    param_values.setdefault(pname, spec["defaultValue"])
            for i, res in enumerate(doc.get("resources", []) or []):
                ingest_arm(res, fname, f"resources[{i}]")
            return
        if isinstance(doc, dict):
            kind, name, props = classify_resource(
                doc, os.path.splitext(fname)[0], folder_hint)
            formats.add("Git folder" if GIT_FOLDERS.get(folder_hint.lower().rstrip("s"))
                        else "resource JSON")
            put(kind, name, props, fname)

    docs: list = []
    json_files = 0
    if os.path.isdir(input_path):
        for root, _dirs, files in os.walk(input_path):
            for fname in sorted(files):
                if fname.lower().endswith(".json"):
                    json_files += 1
                    rel = os.path.relpath(os.path.join(root, fname), input_path)
                    try:
                        docs.append((load_json(os.path.join(root, fname)), rel,
                                     os.path.basename(root)))
                    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
                        skipped.append((rel, str(exc)))
                        print(f"  ! skipping {rel}: {exc}", file=sys.stderr)
    else:
        json_files = 1
        docs.append((load_json(input_path), os.path.basename(input_path), ""))

    # Deployment parameter files are read first so their values fill the template.
    for doc, _fname, _hint in docs:
        if _is_arm_parameters(doc):
            formats.add("ARM parameters")
            for pname, spec in doc["parameters"].items():
                if isinstance(spec, dict) and "value" in spec:
                    param_values[pname] = spec["value"]
    for doc, fname, hint in docs:
        ingest_doc(doc, fname, hint)

    for kind in KINDS + SUPPORT_KINDS + ("unknown",):
        store.setdefault(kind, {})
    store["__skipped__"] = {f: {"error": e} for f, e in skipped}
    store["__origins__"] = dict(origins)
    store["__input__"] = {
        "path": os.path.basename(os.path.normpath(input_path)),
        "formats": sorted(formats),
        "jsonFiles": json_files,
        "skipped": len(skipped),
        "duplicates": duplicates,
        "unrecognised": sorted(store["unknown"]),
        "deploymentValuesUsed": sorted(used_params),
    }
    return store
