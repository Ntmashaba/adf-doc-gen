"""Join Power BI sources (pbi-doc-gen) to the Data Factory pipelines that produce them.

Reads the payload embedded in generated HTML (or saved JSON) from both tools
and matches each Power BI source object to ADF objects by where they live:

* exact     same server/account, same database/container, same object or path
* possible  the names match but the location is not proven equal (one side's
            server or database is unknown, or only the object name matches)
* none      nothing in the supplied factories touches it

Only pipelines that write the object count as producers. A producer reached
only through runtime-resolved targets or opaque code is downgraded to possible.
Views and procedures are never treated as proof of the tables underneath
them: a view is matched by its own name only.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from .common import _norm_host
from .details import json_script
from .sql_harvest import harvest_sql

TEMPLATE = Path(__file__).with_name("bridge.html")
SKIP_TYPES = {"Calculated (DAX)", "Entered data", "No partition"}


def load_payloads(path) -> List[Tuple[str, dict]]:
    """Every tool payload in a file or folder (HTML with an embedded payload, or JSON)."""
    path = Path(path)
    files = sorted(path.iterdir()) if path.is_dir() else [path]
    out = []
    for f in files:
        if f.suffix.lower() not in (".html", ".htm", ".json") or not f.is_file():
            continue
        text = f.read_text(encoding="utf-8-sig", errors="replace")
        payload = None
        if f.suffix.lower() == ".json":
            try:
                payload = json.loads(text)
            except ValueError:
                payload = None
        else:
            m = re.search(r"const DATA\s*=\s*", text)
            if m:
                try:
                    payload, _ = json.JSONDecoder().raw_decode(text[m.end():].replace("<\\/", "</"))
                except ValueError:
                    payload = None
        if isinstance(payload, dict):
            out.append((f.name, payload))
    return out


def _name(schema: Optional[str], obj: Optional[str]) -> str:
    return ".".join(x.strip("[]\"`").lower() for x in (schema, obj) if x)


def _url_parts(url: str) -> Tuple[str, str]:
    """(host, path) of a storage URL, path without leading slash, unescaped, lower-case."""
    u = urlparse(url if "://" in url else "https://" + url)
    return (u.hostname or "").lower(), unquote(u.path).strip("/").lower()


def _storage_host(host: str) -> str:
    # blob and dfs endpoints of one account are the same storage
    return re.sub(r"\.(blob|dfs)\.core\.windows\.net$", ".storage", host)


def adf_index(payloads: List[Tuple[str, dict]]) -> List[dict]:
    """ADF objects with who writes and reads them."""
    rows = []
    for fname, p in payloads:
        if p.get("generator") != "adf-doc-gen":
            continue
        pipes = {x["name"]: x for x in p.get("pipelines", [])}
        for e in p.get("entities", []):
            if e.get("kind") not in ("table", "file_path", "dataset", "inline_dataset"):
                continue
            ep = e.get("endpoint") or {}
            writers, readers = [], []
            for u in e.get("usage", []):
                entry = {"pipeline": u.get("pipeline"), "activity": u.get("activity"),
                         "uncertain": bool(u.get("dynamic") or u.get("opaque") or e.get("dynamic")),
                         "triggers": (pipes.get(u.get("pipeline")) or {}).get("startedBy", [])}
                (writers if u.get("operation") in ("write", "delete") else readers).append(entry)
            host = _norm_host(ep.get("server") or ep.get("url"))
            rows.append({
                "factory": p.get("title", fname), "file": fname, "key": e["key"], "label": e["label"],
                "kind": e["kind"], "host": host, "storage": _storage_host(host),
                "database": (ep.get("database") or "").lower(),
                "container": (ep.get("container") or "").lower(),
                "path": (ep.get("path") or "").lower(),
                "name": _name(ep.get("schema"), ep.get("object")) if ep.get("object") else "",
                "dynamic": bool(e.get("dynamic")), "writers": writers, "readers": readers,
            })
    return rows


def _match_table(src_host, src_db, name, adf):
    hits = []
    obj = name.split(".")[-1]
    for a in adf:
        if a["kind"] not in ("table", "dataset", "inline_dataset") or not a["name"]:
            continue
        same_name = a["name"] == name or (("." not in name or "." not in a["name"])
                                          and a["name"].split(".")[-1] == obj)
        if not same_name:
            continue
        host_ok = bool(src_host and a["host"]) and src_host == a["host"]
        db_ok = bool(src_db and a["database"]) and src_db == a["database"]
        conflict = (src_host and a["host"] and src_host != a["host"]) or \
                   (src_db and a["database"] and src_db != a["database"])
        if host_ok and db_ok and a["name"] == name and not a["dynamic"]:
            hits.append((a, "exact"))
        elif not conflict:
            hits.append((a, "possible"))
    return hits


def _match_file(url, adf):
    host, path = _url_parts(url)
    storage = _storage_host(host)
    hits = []
    for a in adf:
        if a["kind"] != "file_path" or not a["storage"]:
            continue
        full = "/".join(x for x in (a["container"], a["path"]) if x) or a["path"]
        if not full:
            continue
        if storage == a["storage"] and (path == full or path.startswith(full + "/")):
            hits.append((a, "possible" if a["dynamic"] else "exact"))
        elif storage == a["storage"] and full.split("/")[-1] == path.split("/")[-1]:
            hits.append((a, "possible"))
    return hits


def build_bridge(adf_path, pbi_path, output) -> Path:
    adf = adf_index(load_payloads(adf_path))
    rows = []
    for fname, p in load_payloads(pbi_path):
        for s in p.get("sourceObjects") or []:
            if not isinstance(s, dict) or s.get("sourceType") in SKIP_TYPES:
                continue
            host = _norm_host(s.get("server"))
            db = (s.get("database") or "").lower()
            targets = []           # (how the object was named, name)
            server = str(s.get("server") or "")
            file_like = server.startswith(("http", "\\\\")) or re.match(r"^[A-Za-z]:[\\/]", server)
            if s.get("object") and not file_like:
                targets.append(("table", _name(s.get("schema"), s.get("object"))))
            for t in harvest_sql(s.get("sql") or "")[0]:
                parts = [x.strip("[]\"`").lower() for x in t.split(".")]
                targets.append(("native query", ".".join(parts[-2:])))
            hits = []
            for how, name in targets:
                hits += [(a, level, how) for a, level in _match_table(host, db, name, adf)]
            if str(s.get("server", "")).startswith("http"):
                hits += [(a, level, "file") for a, level in _match_file(s["server"], adf)]
            producers, consumers, best = [], [], "none"
            for a, level, how in hits:
                for w in a["writers"]:
                    lv = "possible" if (w["uncertain"] or level == "possible") else "exact"
                    producers.append({"factory": a["factory"], "file": a["file"], "object": a["label"],
                                      "pipeline": w["pipeline"], "activity": w["activity"],
                                      "triggers": w["triggers"], "level": lv, "via": how})
                for r in a["readers"]:
                    consumers.append({"factory": a["factory"], "pipeline": r["pipeline"],
                                      "object": a["label"]})
            if producers:
                best = "exact" if any(x["level"] == "exact" for x in producers) else "possible"
            elif hits:
                best = "read only"
            rows.append({
                "report": s.get("report") or p.get("title", fname), "reportFile": fname,
                "table": s.get("table", ""), "sourceType": s.get("sourceType", ""),
                "server": s.get("server", ""), "database": s.get("database", ""),
                "object": ".".join(x for x in (s.get("schema"), s.get("object")) if x) or s.get("object", ""),
                "status": s.get("status", ""), "match": best,
                "producers": producers, "alsoReadBy": consumers[:20],
            })
    factories = sorted({a["factory"] for a in adf})
    html = TEMPLATE.read_text(encoding="utf-8").replace(
        "/*__BRIDGE__*/null", json_script({"rows": rows, "factories": factories}))
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return output
