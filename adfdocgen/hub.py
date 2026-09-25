"""The documentation home: one offline page listing every factory document.

It reads the payload embedded in each generated HTML file in a folder, so it
can be rebuilt at any time without the original ADF JSON. It shows factory
cards (grouped by the "Home page group" detail), a cross-factory inventory of
the servers, databases and storage each factory reads or writes, and the
results of the last batch run, including failures.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote

from .analyzer import _norm_host
from .details import json_script, read_details, validate_details

HUB_MARKER = 'name="adf-documentation-hub"'
HUB_NAME = "adf-home.html"
BATCH_RESULTS = "adf-batch-results.json"
TEMPLATE = Path(__file__).with_name("hub.html")


def _payload(text: str) -> Optional[dict]:
    m = re.search(r"const DATA\s*=\s*", text)
    if not m:
        return None
    try:
        value, _ = json.JSONDecoder().raw_decode(text[m.end():].replace("<\\/", "</"))
    except ValueError:
        return None
    return value if isinstance(value, dict) and value.get("generator") == "adf-doc-gen" else None


def _num(v) -> int:
    return v if isinstance(v, int) and not isinstance(v, bool) and v >= 0 else 0


def _text(v) -> str:
    return v if isinstance(v, str) else ""


def describe(path: Path) -> Optional[dict]:
    """A small, known-shape summary of one factory document (or None)."""
    text = path.read_text(encoding="utf-8-sig")
    if HUB_MARKER in text:
        return None
    payload = _payload(text)
    if payload is None:
        return None
    try:
        details = read_details(text) or validate_details(payload.get("details"))
    except ValueError:
        details = validate_details(None)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    counts = summary.get("counts") if isinstance(summary.get("counts"), dict) else {}
    issues = summary.get("issues") if isinstance(summary.get("issues"), dict) else {}
    sources = []
    for s in summary.get("sources") or []:
        if not isinstance(s, dict):
            continue
        sources.append({
            "system": _text(s.get("system")), "server": _text(s.get("server")),
            "host": _norm_host(_text(s.get("server"))), "database": _text(s.get("database")),
            "objects": [o for o in s.get("objects") or [] if isinstance(o, str)][:200],
            "read": _num(s.get("read")), "written": _num(s.get("written"))})
    return {
        "file": path.name, "href": quote(path.name, safe=""),
        "title": _text(payload.get("title")) or path.stem,
        "generated": _text(payload.get("generated")),
        "schemaVersion": _num(payload.get("schemaVersion")),
        "mode": _text(payload.get("mode")),
        "details": details,
        "counts": {k: _num(counts.get(k)) for k in
                   ("pipelines", "activities", "dataflows", "datasets", "linkedServices", "triggers")},
        "issues": {k: _num(issues.get(k)) for k in ("high", "medium", "low")},
        "unresolved": _num(summary.get("unresolved")),
        "sources": sources,
    }


def build_hub(folder, output=None) -> Path:
    folder = Path(folder).resolve()
    if not folder.is_dir():
        raise ValueError(f"Folder not found: {folder}")
    output = Path(output) if output else folder / HUB_NAME
    if output.exists() and HUB_MARKER not in output.read_text(encoding="utf-8-sig"):
        raise ValueError(f"Refusing to replace a file that is not a documentation home: {output}")
    rows: List[dict] = []
    for path in sorted(folder.iterdir(), key=lambda p: p.name.casefold()):
        if path.suffix.lower() != ".html" or path.resolve() == output.resolve():
            continue
        try:
            row = describe(path)
        except (OSError, UnicodeError) as exc:
            row = {"file": path.name, "href": quote(path.name, safe=""), "title": path.stem,
                   "error": f"Could not be read: {exc}"}
        if row:
            rows.append(row)
    batch = None
    results = folder / BATCH_RESULTS
    if results.exists():
        try:
            value = json.loads(results.read_text(encoding="utf-8"))
            if isinstance(value, dict) and isinstance(value.get("factories"), list):
                batch = value
        except (ValueError, OSError):
            batch = None
    html = (TEMPLATE.read_text(encoding="utf-8")
            .replace("/*__FACTORIES__*/[]", json_script(rows))
            .replace("/*__BATCH__*/null", json_script(batch)))
    output.write_text(html, encoding="utf-8")
    return output
