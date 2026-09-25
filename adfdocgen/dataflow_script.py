"""Parsing the mapping data flow script: steps, order, dead ends, traces."""
from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

from .common import arm_parameter, obj, lst, as_text, squeeze, is_dynamic, canon_table, _norm_host, physical_key, split_table, EMPTY_ENDPOINT  # noqa: F401

# ---------------------------------------------------------------------------
# Mapping data flow script parser (from the field-tested distiller)
# ---------------------------------------------------------------------------

DF_OUT_RE = re.compile(r"~>\s*([\w]+)(?:\s*@\(([^)]*)\))?")
DF_HEAD_RE = re.compile(r"^\s*(?:([\w\s,@]+?)\s+)?([\w]+)\s*\(", re.DOTALL)


_DF_OPTION = re.compile(
    r"\b(tableName|schemaName|fileSystem|container|folderPath|fileName|entity|objectName|"
    r"resourceName|collection|query|store|format)\s*:\s*"
    r"('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|\((?:[^()]|\([^()]*\))*\)|\$\w+)")


def inline_dataflow_endpoint(config: str) -> dict:
    """What an inline data-flow source/sink points at, from its script options.

    Inline sources and sinks have no dataset: the table, container or path is
    written in the flow script (tableName: 'Orders', folderPath: ($folder)).
    Values set by data-flow parameters ($x) are reported as dynamic.
    """
    opts: Dict[str, str] = {}
    for key, raw in _DF_OPTION.findall(config or ""):
        if key in opts:
            continue
        if raw[0] in "'\"":
            opts[key] = raw[1:-1].replace("\\'", "'")
        else:
            opts[key] = raw.strip("()").strip()
            if "$" in raw:
                opts[key] = "@" + opts[key]    # mark as runtime-resolved
    fmt = opts.get("format", "")
    out = {"kind": "inline_dataset", "label": "", "dynamic": False, "query": "",
           "schema": None, "object": None, "container": None, "path": None}
    if fmt == "query" and opts.get("query"):
        out["query"] = opts["query"]
        out["dynamic"] = opts["query"].startswith("@")
        return out
    if opts.get("tableName"):
        out.update(kind="table", schema=opts.get("schemaName"), object=opts["tableName"])
        out["label"] = ".".join(x for x in (opts.get("schemaName"), opts["tableName"]) if x)
    elif any(opts.get(k) for k in ("fileSystem", "container", "folderPath", "fileName")):
        container = opts.get("fileSystem") or opts.get("container")
        path = "/".join(x for x in (opts.get("folderPath"), opts.get("fileName")) if x)
        out.update(kind="file_path", container=container, path=path or None)
        out["label"] = "/".join(x for x in (container, path) if x)
    else:
        for key in ("entity", "objectName", "resourceName", "collection"):
            if opts.get(key):
                out["label"] = f"{key} {opts[key]}"
                out["object"] = opts[key]
                break
    used = [v for v in (out["schema"], out["object"], out["container"], out["path"]) if v]
    out["dynamic"] = any(v.startswith("@") for v in used)
    return out


def parse_dataflow_script(tp: dict) -> List[dict]:
    """Parse the data flow DSL into ordered transformation steps."""
    raw = tp.get("scriptLines")
    if raw is None:
        raw = tp.get("script", "")
    text = "\n".join(raw) if isinstance(raw, list) else (raw or "")
    if not text.strip():
        return []
    text = re.sub(r"\b(?:parameters|functions)\s*\{[^{}]*\}", "", text)
    steps, prev_end = [], 0
    for m in DF_OUT_RE.finditer(text):
        segment = text[prev_end:m.start()].strip()
        prev_end = m.end()
        name, streams_raw = m.group(1), m.group(2)
        streams = [s.strip() for s in (streams_raw or "").split(",") if s.strip()]
        head = DF_HEAD_RE.match(segment)
        inputs_raw, op, config = [], "?", segment
        if head:
            op = head.group(2)
            if head.group(1):
                inputs_raw = [i.strip() for i in head.group(1).split(",") if i.strip()]
            config = segment[segment.index("(", head.start(2)):]
        if op == "parameters":
            continue
        steps.append({
            "name": name,
            "op": op,
            "inputs_raw": inputs_raw,
            "inputs": [i.split("@")[0] for i in inputs_raw],
            "streams": streams,
            "config": re.sub(r"\s+", " ", config).strip(),
        })
    return steps


def dataflow_exec_order(steps: List[dict]) -> Dict[str, int]:
    idx = {s["name"]: i for i, s in enumerate(steps)}
    alias = {st: s["name"] for s in steps for st in s["streams"]}
    resolved = {s["name"]: [alias.get(i, i) for i in s["inputs"]] for s in steps}
    indeg = {n: sum(1 for i in resolved[n] if i in idx) for n in idx}
    succ = defaultdict(list)
    for n, ins in resolved.items():
        for i in ins:
            if i in idx:
                succ[i].append(n)
    q = deque(sorted([n for n in idx if indeg[n] == 0], key=idx.get))
    order, k = {}, 0
    while q:
        n = q.popleft()
        k += 1
        order[n] = k
        for s in sorted(succ[n], key=idx.get):
            indeg[s] -= 1
            if indeg[s] == 0:
                q.append(s)
    for n in idx:
        order.setdefault(n, 0)
    return order


def dataflow_dead_ends(steps: List[dict]) -> List[str]:
    consumed: Set[str] = set()
    for s in steps:
        for i in s["inputs_raw"]:
            consumed.add(i)
            consumed.add(i.split("@")[0])
    dead = []
    for s in steps:
        if s["op"] == "sink":
            continue
        outs = [s["name"]] + [f"{s['name']}@{st}" for st in s["streams"]] + s["streams"]
        if not any(o in consumed for o in outs):
            dead.append(f"{s['name']} ({s['op']})")
        else:
            for st in s["streams"]:
                if st not in consumed and f"{s['name']}@{st}" not in consumed:
                    dead.append(f"{s['name']}@{st} (unconsumed split branch)")
    return dead


def trace_dataflow(steps: List[dict], sinks: List[str]) -> List[Tuple[str, List[str], List[str]]]:
    by_name = {s["name"]: s for s in steps}
    alias = {st: s["name"] for s in steps for st in s["streams"]}
    results = []
    for sink in sinks:
        if sink not in by_name:
            continue
        seen, chain, sources, stack = set(), [], [], [sink]
        while stack:
            raw = stack.pop()
            node = alias.get(raw, raw)
            if node in seen or node not in by_name:
                continue
            seen.add(node)
            step = by_name[node]
            chain.append(f"{node}({step['op']})")
            if step["op"] == "source":
                sources.append(node)
            stack.extend(step["inputs"])
        results.append((sink, sources, list(reversed(chain))))
    return results
