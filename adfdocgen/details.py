"""Factory details that people maintain by hand and that survive regeneration.

They live in the generated HTML as a small JSON block. Regenerating over an
existing file keeps them; the page can edit them and download an updated
copy. Only plain text reference fields are accepted, never secrets.
"""
from __future__ import annotations

import json
from html.parser import HTMLParser
from typing import Any, Optional

from .redact import scrub_text

DETAILS_ID = "adf-documentation-details"
FIELDS = {
    "environment": "Environment (e.g. Production)",
    "owner": "Owner or team",
    "factory": "Factory resource (subscription / resource group / name)",
    "sourceLocation": "Source location (Git repository and branch, or export path)",
    "runbook": "Runbook or support page",
    "folder": "Home page group (e.g. Finance / Daily)",
    "notes": "Notes",
}
MAX_LEN = 4000


def validate_details(value: Any) -> dict:
    if value in (None, ""):
        return {k: "" for k in FIELDS}
    if not isinstance(value, dict):
        raise ValueError("Factory details must be a JSON object")
    unknown = set(value) - set(FIELDS)
    if unknown:
        raise ValueError(f"Unsupported factory details: {', '.join(sorted(unknown))}; "
                         f"accepted: {', '.join(FIELDS)}")
    out = {}
    for key in FIELDS:
        text = value.get(key, "")
        if not isinstance(text, str):
            raise ValueError(f"{key} must be text")
        if len(text) > MAX_LEN:
            raise ValueError(f"{key} is longer than {MAX_LEN} characters")
        if scrub_text(text)[1]:
            raise ValueError(f"{key} looks like it contains a secret (password, key or token); "
                             f"record where the secret lives instead")
        out[key] = text
    return out


class _Reader(HTMLParser):
    def __init__(self):
        super().__init__()
        self.capture, self.text, self.found = False, "", None

    def handle_starttag(self, tag, attrs):
        if tag == "script" and dict(attrs).get("id") == DETAILS_ID:
            self.capture, self.text = True, ""

    def handle_data(self, data):
        if self.capture:
            self.text += data

    def handle_endtag(self, tag):
        if tag == "script" and self.capture:
            self.capture = False
            self.found = json.loads(self.text)


def read_details(html: str) -> Optional[dict]:
    reader = _Reader()
    reader.feed(html)
    return validate_details(reader.found) if reader.found is not None else None


def json_script(value: Any) -> str:
    return (json.dumps(value, ensure_ascii=False)
            .replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026"))
