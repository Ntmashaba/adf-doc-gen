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

from .redact import scrub_payload
from .details import DETAILS_ID, json_script, read_details, validate_details

TEMPLATE = Path(__file__).parent / "template.html"


def build_payload(analysis: dict, title: str) -> dict:
    payload = {
        "title": title,
        "generator": "adf-doc-gen",
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        **analysis,
    }
    # Safety net: every renderer reads this payload, so check it once here.
    payload, extra = scrub_payload(payload)
    payload["redactions"] = payload.get("redactions", 0) + extra
    return payload


def render_html(payload: dict, out_path: str | Path) -> Path:
    out_path = Path(out_path)
    details = payload.get("details")
    if details is None and out_path.exists():
        # Regenerating over an earlier document keeps its maintained details.
        details = read_details(out_path.read_text(encoding="utf-8-sig"))
    details = validate_details(details)
    payload = {**payload, "details": details, "documentationFilename": out_path.name}
    template = TEMPLATE.read_text(encoding="utf-8")
    template = template.replace(
        "<!--__DETAILS__-->",
        f'<script type="application/json" id="{DETAILS_ID}">{json_script(details)}</script>')
    blob = json.dumps(payload, ensure_ascii=False)
    # keep the embedded JSON from terminating the script block early
    blob = blob.replace("</", "<\\/")
    html = (template
            .replace("__TITLE__", payload["title"].replace("<", "&lt;"))
            .replace("/*__DATA__*/null", blob))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path
