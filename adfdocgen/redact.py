"""Keep secret values out of everything the tool writes.

Two passes, both applied before any renderer sees the data:

* scrub_definitions() cleans the loaded ADF JSON before analysis, so no
  analysis step can copy a secret into a label, detail or query field.
* scrub_payload() re-checks every string in the final payload (HTML, JSON,
  Word and agent output all render from it) as a safety net.

A redacted value is replaced with REDACTED, so readers can see that a value
existed and was withheld.
"""
from __future__ import annotations

import re
from typing import Any, Tuple

REDACTED = "[redacted]"

# Setting names whose literal value is a secret.
SECRET_KEYS = re.compile(
    r"(?:password|passwd|^pwd$|passphrase|secret|token|apikey|api_key|api-key|accountkey|"
    r"accesskey|^sas$|sasuri|privatekey|credential|authorization|subscriptionkey|"
    r"subscription-key|functionkey|serviceprincipalkey|sharedaccess|^pat$)", re.I)
# Names that describe a secret without holding one.
NOT_SECRET_KEYS = re.compile(r"(?:name|type|reference|endpoint|url|version|policy|"
                             r"expiry|expiration|inlinecredential)$", re.I)


def _secret_key(key: str) -> bool:
    return bool(SECRET_KEYS.search(key)) and not NOT_SECRET_KEYS.search(key)


_URL_USERINFO = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s@'\"]+@")
_QUERY_SECRET = re.compile(
    r"(?i)([?&](?:sig|signature|token|access_token|code|key|apikey|api_key|api-key|"
    r"password|pwd|secret|client_secret|subscription-key|auth)=)[^&#\s'\"]+")
_CONN_SECRET = re.compile(
    r"(?i)\b((?:password|pwd|accountkey|sharedaccesskey|sharedaccesssignature|"
    r"secret|clientsecret|accesskey|token)\s*=\s*)(?!\[redacted\])[^;'\"]+")
_BEARER = re.compile(r"(?i)\b(bearer\s+)[a-z0-9._~+/=-]{8,}")


def scrub_text(text: str) -> Tuple[str, int]:
    """Redact secrets embedded in free text. Returns (text, redactions)."""
    count = 0

    def sub(rx, repl, s):
        nonlocal count
        s2, n = rx.subn(repl, s)
        count += n
        return s2

    text = sub(_URL_USERINFO, r"\1" + REDACTED + "@", text)
    text = sub(_QUERY_SECRET, r"\1" + REDACTED, text)
    text = sub(_CONN_SECRET, r"\1" + REDACTED, text)
    text = sub(_BEARER, r"\1" + REDACTED, text)
    return text, count


def _is_expression(value: Any) -> bool:
    """ADF expressions and ARM references name a value; they are not the value."""
    if isinstance(value, dict):
        return value.get("type") == "Expression"
    return isinstance(value, str) and (value.startswith("@") or value.startswith("[")
                                       or value == "")


class _Scrubber:
    def __init__(self, by_key: bool = True):
        self.count = 0
        self.by_key = by_key  # payload keys are ours, not ADF settings

    def walk(self, node: Any, key: str = "") -> Any:
        if isinstance(node, dict):
            if self.by_key and node.get("type") == "SecureString" and "value" in node:
                return {**node, "value": self.secure(node["value"])}
            if node.get("type") == "AzureKeyVaultSecret":
                return node  # names a secret; holds no value
            out = {}
            for k, v in node.items():
                if self.by_key and _secret_key(str(k)) and not isinstance(v, (dict, list)) \
                        and v is not None and not _is_expression(v):
                    out[k] = REDACTED
                    self.count += 1
                elif self.by_key and _secret_key(str(k)) and isinstance(v, dict) \
                        and v.get("type") not in ("AzureKeyVaultSecret", "Expression", "SecureString"):
                    out[k] = self.walk_all_values(v)
                else:
                    out[k] = self.walk(v, str(k))
            return out
        if isinstance(node, list):
            return [self.walk(v, key) for v in node]
        if isinstance(node, str):
            text, n = scrub_text(node)
            self.count += n
            return text
        return node

    def walk_all_values(self, node: Any) -> Any:
        """Everything under a secret-named key is secret, except references."""
        if isinstance(node, dict):
            if node.get("type") in ("AzureKeyVaultSecret", "Expression"):
                return node
            return {k: self.walk_all_values(v) for k, v in node.items()}
        if isinstance(node, list):
            return [self.walk_all_values(v) for v in node]
        if node is None or _is_expression(node):
            return node
        self.count += 1
        return REDACTED

    def secure(self, value: Any) -> Any:
        if not isinstance(value, str) or _is_expression(value):
            return value
        # Connection strings keep their server and database; only secrets go.
        if "=" in value and ";" in value:
            text, n = scrub_text(value)
            self.count += n
            return text
        self.count += 1
        return REDACTED


def scrub_definitions(store: dict) -> int:
    """Redact secrets in the loaded resources, in place. Returns the count."""
    s = _Scrubber()
    for kind, items in list(store.items()):
        if kind.startswith("__") or not isinstance(items, dict):
            continue
        for name in list(items):
            items[name] = s.walk(items[name])
    return s.count


def scrub_payload(payload: Any) -> Tuple[Any, int]:
    """Re-check every string in the output payload."""
    s = _Scrubber(by_key=False)
    clean = s.walk(payload)
    return clean, s.count
