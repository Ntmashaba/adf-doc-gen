"""Small helpers shared by the analysis modules: JSON shape guards, text,
ADF expressions, and the identity of physical objects."""
from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

_ARM_PARAMETER = re.compile(r"^\[\s*parameters\(\s*'([^']+)'\s*\)\s*\]$")


def arm_parameter(value) -> Optional[str]:
    """The parameter name when a value is exactly "[parameters('name')]"."""
    m = _ARM_PARAMETER.match(value.strip()) if isinstance(value, str) else None
    return m.group(1) if m else None


def obj(value) -> dict:
    """A JSON object, or {} when an ARM expression string ("[parameters('x')]")
    or anything else stands where an object is expected."""
    return value if isinstance(value, dict) else {}


def lst(value) -> list:
    """A JSON array, or [] when an ARM expression or other value stands in."""
    return value if isinstance(value, list) else []


def as_text(value: Any) -> str:
    """Render a typeProperties value, which may be a literal or an ADF Expression."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return str(value["value"])
        if "value" in value and len(value) <= 2:
            return as_text(value["value"])
        return json.dumps(value, default=str, sort_keys=True)
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return "\n".join(as_text(v) for v in value)
    return json.dumps(value, default=str)


def squeeze(s: Any, max_len: Optional[int] = None) -> str:
    s = as_text(s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    s = re.sub(r"\n\s*\n+", "\n", s)
    if max_len and len(s) > max_len:
        s = s[:max_len].rstrip() + " …[truncated]"
    return s


def is_dynamic(text: Any) -> bool:
    return "@" in as_text(text)


def canon_table(name: str) -> str:
    """Normalise a table reference so DB1.[dbo].[Fact] and dbo.Fact become one node.

    Letter case is kept: whether dbo.Sales and dbo.SALES are one table depends on a
    collation the definitions do not reveal, so they are not merged on a guess.
    """
    parts = [p.strip().strip("[]\"`") for p in re.split(r"\.(?![^\[]*\])", name or "") if p.strip()]
    if not parts:
        return ""
    # Keep the database when it is named: SalesDW.dbo.Fact and Archive.dbo.Fact are different tables.
    parts = parts[-3:]
    return ".".join(parts)


def _norm_host(value: Optional[str]) -> str:
    """server/account without scheme, port or trailing slash, lower-case."""
    v = (value or "").strip().lower()
    v = re.sub(r"^[a-z][a-z0-9+.-]*://", "", v)
    v = re.sub(r"^tcp:", "", v)
    v = re.sub(r"[,:]\d+$", "", v.split("/")[0])
    return v


# Data Factory system names, plus pbi-doc-gen's name for Synapse (used by the bridge).
SQL_SERVER_SYSTEMS = {"SQL Server", "Azure SQL Database", "Azure SQL Managed Instance", "Azure Synapse SQL",
                      "Azure Synapse / SQL"}
SQL_SERVER_DEFAULT_PORT = 1433


def effective_port(system: Optional[str], server: Optional[str], port) -> Optional[int]:
    """The port that distinguishes an endpoint, or None for the connector default.

    SQL Server-family connectors reach a default instance on 1433 when no port is
    given, so "host" and "host,1433" are one endpoint. A different port, a named
    instance, or any other connector keeps the port it was given.
    """
    if not port:
        return None
    port = int(port)
    if (port == SQL_SERVER_DEFAULT_PORT and system in SQL_SERVER_SYSTEMS
            and "\\" not in (server or "")):
        return None
    return port


def physical_key(kind: str, label: str, endpoint: Optional[dict], scope: str = "") -> str:
    """Identity of a physical object: system location + database + object/path.

    Two tables called dbo.Sales on different servers are different objects.
    When the location is unknown the identity is scoped to the connection
    (linked service) that reached it, never merged on the name alone.
    """
    ep = endpoint or {}
    host = _norm_host(ep.get("server") or ep.get("url"))
    port = effective_port(ep.get("system"), ep.get("server"), ep.get("port"))
    if host and port:
        # One host can serve different instances on different ports.
        host = f"{host}:{port}"
    where = host or (f"ls:{scope.lower()}" if scope else "")
    if kind in ("table", "stored_procedure"):
        name = canon_table(label)
        parts = name.split(".")
        db = ep.get("database") or ""
        if len(parts) == 3:
            db, name = parts[0], ".".join(parts[1:])
        loc = "/".join(x for x in (where, db) if x)
        return f"{kind}:{loc}/{name}" if loc else f"{kind}:{name}"
    if kind == "file_path":
        container = ep.get("container") or ""
        path = "/".join(x for x in (container, ep.get("path") or "") if x) or label
        return f"file_path:{where}/{path}" if where else f"file_path:{path}"
    return f"{kind}:{label.lower()}"


def split_table(name: str) -> Tuple[Optional[str], Optional[str]]:
    """schema, object from a possibly bracketed multi-part table name."""
    parts = [p.strip().strip("[]\"`") for p in re.split(r"\.(?![^\[]*\])", name or "") if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return parts[-2], parts[-1]


EMPTY_ENDPOINT = {"system": None, "server": None, "port": None, "database": None, "schema": None,
                  "object": None, "path": None, "url": None, "container": None}


# ---------------------------------------------------------------------------
