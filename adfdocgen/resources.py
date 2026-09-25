"""Linked services, datasets and triggers: describing each definition."""
from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

from .common import arm_parameter, obj, lst, as_text, squeeze, is_dynamic, canon_table, _norm_host, physical_key, split_table, EMPTY_ENDPOINT  # noqa: F401

# Dataset / linked service resolution (normalised for the lineage contract)
# ---------------------------------------------------------------------------

CONN_SERVER = re.compile(r"(?:Server|Data Source|Host|Endpoint)\s*=\s*([^;]+)", re.I)
CONN_DB = re.compile(r"(?:Database|Initial Catalog)\s*=\s*([^;]+)", re.I)
# Inline credential *detection* only — matched values are never surfaced.
CONN_SECRET = re.compile(
    r"(?:Password|Pwd|AccountKey|SharedAccessSignature|SAS|Secret|AccessKey)\s*=\s*[^;{@\s][^;]*",
    re.I)

LS_SYSTEM = {
    "AzureSqlDatabase": "Azure SQL Database", "AzureSqlDW": "Azure Synapse SQL",
    "AzureSqlMI": "Azure SQL Managed Instance", "SqlServer": "SQL Server",
    "AzureSynapseAnalytics": "Azure Synapse Analytics",
    "AzureBlobStorage": "Azure Blob Storage", "AzureBlobFS": "ADLS Gen2",
    "AzureDataLakeStore": "ADLS Gen1", "AzureFileStorage": "Azure Files",
    "AmazonS3": "Amazon S3", "RestService": "REST API", "HttpServer": "HTTP",
    "OData": "OData", "AzureDatabricks": "Azure Databricks",
    "AzureDatabricksDeltaLake": "Databricks Delta Lake",
    "AzureKeyVault": "Azure Key Vault", "AzureFunction": "Azure Function",
    "Oracle": "Oracle", "Snowflake": "Snowflake", "SnowflakeV2": "Snowflake",
    "PostgreSql": "PostgreSQL", "AzurePostgreSql": "Azure PostgreSQL",
    "MySql": "MySQL", "AzureMySql": "Azure MySQL", "Salesforce": "Salesforce",
    "SapTable": "SAP", "SapHana": "SAP HANA", "Sftp": "SFTP", "FtpServer": "FTP",
    "CosmosDb": "Cosmos DB", "MongoDbAtlas": "MongoDB Atlas", "MongoDbV2": "MongoDB",
    "Dynamics": "Dynamics 365", "DynamicsCrm": "Dynamics CRM",
    "SharePointOnlineList": "SharePoint Online", "Office365": "Microsoft 365",
    "AzureSearch": "Azure AI Search", "AzureDataExplorer": "Azure Data Explorer",
    "GoogleBigQuery": "Google BigQuery", "GoogleBigQueryV2": "Google BigQuery",
    "AmazonRedshift": "Amazon Redshift", "Teradata": "Teradata", "Db2": "IBM Db2",
    "Odbc": "ODBC", "AzureBatch": "Azure Batch", "HDInsight": "HDInsight",
    "HDInsightOnDemand": "HDInsight (on demand)", "AzureML": "Azure ML",
    "AzureMLService": "Azure ML", "FileServer": "File system",
}


NON_SQL_SOURCE = ("cosmos", "mongo", "rest", "http", "odata", "dynamics", "salesforce",
                  "dataexplorer", "search", "json", "delimited", "parquet", "binary",
                  "avro", "orc", "xml", "excel", "sharepoint", "office365", "commondataservice")


_DYNAMIC_OBJECT = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE|MERGE|USING|EXEC(?:UTE)?|CALL)\s+[\w.\[\]\"`]*@", re.I)


def sql_names_dynamic(sql: Any) -> bool:
    """True when an ADF expression builds a table or procedure name in the SQL,
    not merely a filter value (WHERE d > '@{...}')."""
    text = as_text(sql)
    return bool(_DYNAMIC_OBJECT.search(text)) or text.lstrip().startswith("@")


def is_sql_source(source_type: Any) -> bool:
    """Whether a Copy/Lookup source's "query" setting holds SQL (not KQL, OData, Mongo...)."""
    t = as_text(source_type).lower()
    return bool(t) and not any(x in t for x in NON_SQL_SOURCE)


_DS_PARAM_WHOLE = re.compile(r"^@\s*dataset\(\)\.(\w+)\s*$")
_DS_PARAM_INLINE = re.compile(r"@\{\s*dataset\(\)\.(\w+)\s*\}")


def bind_dataset_params(node: Any, values: Dict[str, str]) -> Any:
    """Substitute literal parameter values into a dataset definition.

    Only literals are bound; anything that depends on runtime values stays an
    expression, so it is still reported as dynamic.
    """
    if isinstance(node, dict):
        if node.get("type") == "Expression" and isinstance(node.get("value"), str):
            bound = bind_dataset_params(node["value"], values)
            return bound if not is_dynamic(bound) else {**node, "value": bound}
        return {k: bind_dataset_params(v, values) for k, v in node.items()}
    if isinstance(node, list):
        return [bind_dataset_params(v, values) for v in node]
    if isinstance(node, str):
        m = _DS_PARAM_WHOLE.match(node)
        if m and m.group(1) in values:
            return values[m.group(1)]
        return _DS_PARAM_INLINE.sub(lambda mm: values.get(mm.group(1), mm.group(0)), node)
    return node


def friendly_system(ls_type: str) -> str:
    return LS_SYSTEM.get(ls_type or "", ls_type or "Unknown")


def describe_linked_service(name: str, props: dict) -> dict:
    """Type + target of a linked service. Never returns secret values."""
    tp = obj(props.get("typeProperties"))
    ls_type = props.get("type", "")
    out = {
        "name": name,
        "type": ls_type,
        "system": friendly_system(ls_type),
        "server": None, "database": None, "url": None,
        "auth": None,
        "keyVault": False,
        "inlineCredential": False,
        "parameterized": bool(props.get("parameters")),
        "parameters": sorted(obj(props.get("parameters")).keys()),
        "detail": {},
    }

    def kv_names(node) -> List[str]:
        """Collect Key Vault secret names anywhere under a node."""
        found = []
        if isinstance(node, dict):
            if node.get("type") == "AzureKeyVaultSecret":
                found.append(as_text(node.get("secretName")) or "?")
            for v in node.values():
                found += kv_names(v)
        elif isinstance(node, list):
            for v in node:
                found += kv_names(v)
        return found

    secrets = kv_names(tp)
    if secrets:
        out["keyVault"] = True
        out["auth"] = "Key Vault secret(s): " + ", ".join(sorted(set(secrets)))

    conn = tp.get("connectionString")
    if isinstance(conn, dict):
        ctype = conn.get("type")
        if ctype == "AzureKeyVaultSecret":
            if not out["auth"]:
                out["auth"] = "connection string via Key Vault"
            out["keyVault"] = True
            conn = ""
        elif ctype == "SecureString":
            # Stored in the definition itself, only masked in ADF Studio.
            out["auth"] = out["auth"] or "connection string stored as SecureString in the definition"
            conn = conn.get("value")
        else:
            conn = as_text(conn)
    conn = conn if isinstance(conn, str) else ""
    m = CONN_SERVER.search(conn)
    if m:
        # "tcp:host,1433" is how SQL connection strings spell a host and port.
        out["server"] = re.sub(r",\d+$", "", re.sub(r"(?i)^tcp:", "", m.group(1).strip()))
    m = CONN_DB.search(conn)
    if m:
        out["database"] = m.group(1).strip()
    if conn and CONN_SECRET.search(conn):
        # A credential-looking token with a literal value. Flag it; never print it.
        out["inlineCredential"] = True

    for key, slot in (("server", "server"), ("url", "url"), ("baseUrl", "url"),
                      ("accountEndpoint", "url"), ("serviceEndpoint", "url"),
                      ("workspaceUrl", "url"), ("domain", "url"),
                      ("database", "database")):
        if tp.get(key) and not out[slot]:
            out[slot] = squeeze(tp[key], 160)
    for key in ("cluster", "existingClusterId", "tenant", "baseUrl", "connectVia"):
        if tp.get(key) and key not in ("baseUrl",):
            out["detail"][key] = squeeze(tp[key], 120)

    for slot in ("server", "database", "url"):
        if out[slot] and is_dynamic(out[slot]):
            out["detail"][slot + "Dynamic"] = True
    return out


def describe_dataset(name: str, props: dict, params: Optional[dict] = None) -> dict:
    """Resolve a dataset definition to a normalised physical endpoint.

    params: values an activity passes for this invocation. Literal values (and
    literal defaults) are bound; runtime expressions stay dynamic.
    """
    values = {}
    for pname, spec in obj(props.get("parameters")).items():
        default = obj(spec).get("defaultValue")
        if isinstance(default, (str, int, float)) and not is_dynamic(default):
            values[pname] = str(default)
    for pname, val in obj(params).items() if isinstance(params, dict) else []:
        text = as_text(val)
        if isinstance(val, (str, int, float)) and not is_dynamic(text):
            values[pname] = text
        else:
            values.pop(pname, None)
    tp = obj(bind_dataset_params(props.get("typeProperties"), values))
    ds_type = props.get("type", "")
    ls = obj(props.get("linkedServiceName")).get("referenceName", "")
    out = {
        "name": name,
        "type": ds_type,
        "linkedService": ls,
        "parameterized": bool(props.get("parameters")),
        "parameters": sorted(obj(props.get("parameters")).keys()),
        "endpoint": dict(EMPTY_ENDPOINT),
        "display": "",       # human-readable resolved target
        "dynamic": False,
    }
    ep = out["endpoint"]

    schema = as_text(tp.get("schema"))
    if isinstance(tp.get("schema"), list):   # column schema array, not a db schema
        schema = ""
    table = as_text(tp.get("table") or tp.get("tableName"))
    if table:
        if schema:
            ep["schema"], ep["object"] = schema, table
        else:
            ep["schema"], ep["object"] = split_table(table)
        out["display"] = f"{ep['schema']}.{ep['object']}" if ep["schema"] else (ep["object"] or table)

    loc = obj(tp.get("location"))
    container = next((as_text(loc[k]) for k in ("fileSystem", "container", "bucketName")
                      if loc.get(k)), "")
    path_bits = [as_text(loc.get("folderPath")), as_text(loc.get("fileName"))]
    path = "/".join(b for b in path_bits if b)
    if container:
        ep["container"] = container
    if path:
        ep["path"] = path
    if container or path:
        out["display"] = "/".join(b for b in (container, path) if b)

    if not out["display"]:
        for key in ("folderPath", "fileName", "collectionName", "relativeUrl", "path"):
            if tp.get(key):
                val = as_text(tp[key])
                if key == "relativeUrl":
                    ep["url"] = val
                else:
                    ep["path"] = val
                out["display"] = val
                break

    out["dynamic"] = is_dynamic(out["display"])
    return out


def describe_trigger(name: str, props: dict) -> dict:
    ttype = props.get("type", "?")
    tp = obj(props.get("typeProperties"))
    detail = []
    rec = obj(tp.get("recurrence"))
    if rec:
        detail.append(f"every {rec.get('interval','?')} {rec.get('frequency','?')}"
                      + (f" from {rec.get('startTime')}" if rec.get("startTime") else ""))
    if ttype == "TumblingWindowTrigger":
        detail.append(f"tumbling {tp.get('interval','?')} {tp.get('frequency','?')}")
        for dep in lst(tp.get("dependsOn")):
            detail.append("waits on trigger: "
                          + obj(dep.get("referenceTrigger")).get("referenceName", "?"))
    if ttype in ("BlobEventsTrigger", "CustomEventsTrigger"):
        detail.append("event: " + squeeze(json.dumps(
            {k: tp[k] for k in ("blobPathBeginsWith", "blobPathEndsWith", "events", "scope")
             if k in tp}, default=str), 200))
    if rec.get("timeZone"):
        detail.append(f"time zone {rec['timeZone']}")
    sched = obj(rec.get("schedule"))
    if sched:
        bits = [f"{k} {','.join(as_text(x) for x in lst(v)) or as_text(v)}"
                for k, v in sched.items() if v not in (None, [], {})]
        if bits:
            detail.append("at " + "; ".join(bits))
    # Schedule/event triggers list pipelines; tumbling-window triggers name one.
    targets = lst(props.get("pipelines")) or ([props["pipeline"]]
                                              if isinstance(props.get("pipeline"), dict) else [])
    starts, parameters = [], {}
    for t in targets:
        pl = obj(obj(t).get("pipelineReference")).get("referenceName", "")
        if not pl:
            continue
        starts.append(pl)
        if obj(t.get("parameters")):
            parameters[pl] = {k: squeeze(v, 200) for k, v in t["parameters"].items()}
    return {
        "name": name, "type": ttype,
        # The state saved in the definition, not a live check of the factory.
        "state": props.get("runtimeState", "") or "Unknown",
        "stateSource": "definition",
        "startsPipelines": starts,
        "parameters": parameters,
        "dependsOnTriggers": [obj(d.get("referenceTrigger")).get("referenceName", "")
                              for d in lst(tp.get("dependsOn"))
                              if obj(d.get("referenceTrigger")).get("referenceName")],
        "detail": " | ".join(detail),
    }
