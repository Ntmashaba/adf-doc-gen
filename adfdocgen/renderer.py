"""Assemble the consolidated JSON payload and inject it into template.html.

The payload is the single source of truth: HTML, Word, and the standalone
JSON are all renderings of it. Injection replaces a /*__DATA__*/null
placeholder and escapes "</" so the JSON can never terminate the script
block (same discipline as pbi-doc-gen).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

TEMPLATE = Path(__file__).parent / "template.html"


def build_payload(analysis: dict, title: str) -> dict:
    return {
        "title": title,
        "generator": "adf-doc-gen",
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        **analysis,
    }


def render_html(payload: dict, out_path: str | Path) -> Path:
    template = TEMPLATE.read_text(encoding="utf-8")
    blob = json.dumps(payload, ensure_ascii=False)
    # keep the embedded JSON from terminating the script block early
    blob = blob.replace("</", "<\\/")
    html = (template
            .replace("__TITLE__", payload["title"].replace("<", "&lt;"))
            .replace("/*__DATA__*/null", blob))
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path
