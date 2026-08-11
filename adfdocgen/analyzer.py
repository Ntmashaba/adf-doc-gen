"""Static analysis of an ADF factory: the single place analysis logic lives.

Every renderer (HTML, Word, JSON) consumes the payload this module produces.
The core walking / harvesting / lineage logic is adapted from the user's
field-tested adf_distill.py; on top of it this module adds:

* normalised source/sink endpoints — the cross-tool lineage contract
  {system, server, database, schema, object, path, url, container}
  (nulls where unknown, parameter/secret names where parameterised;
  matches pbi-doc-gen's partition-source vocabulary for a future join)
* the orchestration graph: trigger -> pipeline -> ExecutePipeline / data flow
* per-pipeline rollups: own and *effective* (transitive) reads/writes,
  flagged incomplete when an invoked child was not supplied
* unused-resource detection, phrased honestly ("not referenced by any
  trigger or pipeline in this factory" — external invocation is invisible)
* structured warnings {severity, category, message}
* inline-credential detection in linked services (flagged, never printed)

Epistemic rules baked in: report parameter and Key Vault secret *names*,
never values; call nothing "unused" beyond factory scope; mark lineage
opaque where work happens in code ADF cannot see; when unsure, say less.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Activity knowledge base (from the field-tested distiller)
# ---------------------------------------------------------------------------

ACTIVITY_CATALOG: Dict[str, Tuple[str, str]] = {
    "Copy":                     ("movement",    "Copies data from a source dataset to a sink dataset"),
    "ExecuteDataFlow":          ("transform",   "Runs a Mapping Data Flow (Spark)"),
    "DatabricksNotebook":       ("transform",   "Runs a Databricks notebook"),
    "DatabricksSparkPython":    ("transform",   "Runs a Python script on Databricks"),
    "DatabricksSparkJar":       ("transform",   "Runs a JAR on Databricks"),
    "HDInsightHive":            ("transform",   "Runs a Hive script"),
    "HDInsightPig":             ("transform",   "Runs a Pig script"),
    "HDInsightSpark":           ("transform",   "Runs a Spark program"),
    "HDInsightMapReduce":       ("transform",   "Runs a MapReduce job"),
    "HDInsightStreaming":       ("transform",   "Runs a Hadoop streaming job"),
    "SqlServerStoredProcedure": ("transform",   "Executes a stored procedure"),
    "Script":                   ("transform",   "Executes SQL script(s)"),
    "SynapseNotebook":          ("transform",   "Runs a Synapse notebook"),
    "SparkJob":                 ("transform",   "Runs a Synapse Spark job definition"),
    "AzureMLBatchExecution":    ("transform",   "Runs an Azure ML batch job"),
    "AzureMLExecutePipeline":   ("transform",   "Runs an Azure ML pipeline"),
    "AzureFunctionActivity":    ("external",    "Calls an Azure Function"),
    "WebActivity":              ("external",    "Calls a REST endpoint"),
    "WebHook":                  ("external",    "Calls a webhook and waits for callback"),
    "Custom":                   ("external",    "Runs custom code on Azure Batch"),
    "Lookup":                   ("read",        "Reads a value/rowset (config or control data)"),
    "GetMetadata":              ("read",        "Reads dataset metadata (file lists, existence, size)"),
    "Validation":               ("read",        "Waits until a dataset exists / meets criteria"),
    "Delete":                   ("management",  "Deletes files/folders at a dataset location"),
    "ExecutePipeline":          ("orchestration", "Invokes another pipeline"),
    "ExecuteSSISPackage":       ("orchestration", "Runs an SSIS package on an IR"),
    "ForEach":                  ("control",     "Iterates a collection, running inner activities per item"),
    "Until":                    ("control",     "Loops inner activities until an expression is true"),
    "IfCondition":              ("control",     "Branches into true/false activity sets"),
    "Switch":                   ("control",     "Branches into one of several cases"),
    "Filter":                   ("control",     "Filters an array with an expression"),
    "SetVariable":              ("control",     "Sets a pipeline variable"),
    "AppendVariable":           ("control",     "Appends to an array variable"),
    "Wait":                     ("control",     "Waits n seconds"),
    "Fail":                     ("control",     "Deliberately fails the pipeline"),
}

# Activities whose real work happens in code ADF cannot see.
OPAQUE_TYPES = {
    "DatabricksNotebook", "DatabricksSparkPython", "DatabricksSparkJar",
    "SynapseNotebook", "SparkJob", "Custom", "ExecuteSSISPackage",
    "HDInsightHive", "HDInsightPig", "HDInsightSpark", "HDInsightMapReduce",
    "HDInsightStreaming", "AzureMLBatchExecution", "AzureMLExecutePipeline",
}

# ---------------------------------------------------------------------------
# SQL harvesting
# ---------------------------------------------------------------------------

SQL_READ_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+((?:\[[^\]]+\]|[\w$#]+)(?:\.(?:\[[^\]]+\]|[\w$#]+)){0,2})",
    re.IGNORECASE)
SQL_WRITE_RE = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|MERGE\s+(?:INTO\s+)?|DELETE\s+FROM|TRUNCATE\s+TABLE|"
    r"INTO)\s+((?:\[[^\]]+\]|[\w$#]+)(?:\.(?:\[[^\]]+\]|[\w$#]+)){0,2})",
    re.IGNORECASE)
SQL_EXEC_RE = re.compile(
    r"\b(?:EXEC(?:UTE)?)\s+((?:\[[^\]]+\]|[\w$#]+)(?:\.(?:\[[^\]]+\]|[\w$#]+)){0,2})",
    re.IGNORECASE)
SQL_NOISE = {"select", "values", "table", "dual", "unnest", "openjson", "openrowset",
             "string_split", "set", "where", "on", "as", "with"}
SQL_STRINGS = re.compile(r"'(?:[^']|'')*'")
SQL_COMMENTS = re.compile(r"--[^\n]*|/\*.*?\*/", re.S)


def _clean_sql(text: str) -> str:
    return SQL_STRINGS.sub("''", SQL_COMMENTS.sub(" ", text or ""))


def harvest_sql(sql_text: str) -> Tuple[List[str], List[str], List[str]]:
    """Return (tables_read, tables_written, procs_executed) from a SQL string."""
    if not sql_text:
        return [], [], []
    clean = _clean_sql(sql_text)

    def grab(rx):
        out = []
        for m in rx.finditer(clean):
            nm = m.group(1).strip()
            leaf = nm.split(".")[-1].strip("[]").lower()
            if leaf not in SQL_NOISE and not nm.startswith("@") and not nm.startswith("("):
                out.append(nm)
        return list(dict.fromkeys(out))

    return grab(SQL_READ_RE), grab(SQL_WRITE_RE), grab(SQL_EXEC_RE)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

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
    """Normalise a table reference so DB1.[dbo].[Fact] and dbo.Fact become one node."""
    parts = [p.strip().strip("[]\"`") for p in re.split(r"\.(?![^\[]*\])", name or "") if p.strip()]
    if not parts:
        return ""
    parts = parts[-2:] if len(parts) > 2 else parts
    return ".".join(p.lower() for p in parts)


def split_table(name: str) -> Tuple[Optional[str], Optional[str]]:
    """schema, object from a possibly bracketed multi-part table name."""
    parts = [p.strip().strip("[]\"`") for p in re.split(r"\.(?![^\[]*\])", name or "") if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return parts[-2], parts[-1]


EMPTY_ENDPOINT = {"system": None, "server": None, "database": None, "schema": None,
                  "object": None, "path": None, "url": None, "container": None}


# ---------------------------------------------------------------------------
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


def friendly_system(ls_type: str) -> str:
    return LS_SYSTEM.get(ls_type or "", ls_type or "Unknown")


def describe_linked_service(name: str, props: dict) -> dict:
    """Type + target of a linked service. Never returns secret values."""
    tp = props.get("typeProperties", {}) or {}
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
        "parameters": sorted((props.get("parameters") or {}).keys()),
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
        if not out["auth"]:
            out["auth"] = "connection string via Key Vault"
        out["keyVault"] = True
        conn = ""
    conn = conn if isinstance(conn, str) else ""
    m = CONN_SERVER.search(conn)
    if m:
        out["server"] = m.group(1).strip()
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


def describe_dataset(name: str, props: dict) -> dict:
    """Resolve a dataset definition to a normalised physical endpoint."""
    tp = props.get("typeProperties", {}) or {}
    ds_type = props.get("type", "")
    ls = (props.get("linkedServiceName") or {}).get("referenceName", "")
    out = {
        "name": name,
        "type": ds_type,
        "linkedService": ls,
        "parameterized": bool(props.get("parameters")),
        "parameters": sorted((props.get("parameters") or {}).keys()),
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

    loc = tp.get("location", {}) or {}
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


# ---------------------------------------------------------------------------
# Mapping data flow script parser (from the field-tested distiller)
# ---------------------------------------------------------------------------

DF_OUT_RE = re.compile(r"~>\s*([\w]+)(?:\s*@\(([^)]*)\))?")
DF_HEAD_RE = re.compile(r"^\s*(?:([\w\s,@]+?)\s+)?([\w]+)\s*\(", re.DOTALL)


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


def describe_trigger(name: str, props: dict) -> dict:
    ttype = props.get("type", "?")
    tp = props.get("typeProperties", {}) or {}
    detail = []
    rec = tp.get("recurrence", {}) or {}
    if rec:
        detail.append(f"every {rec.get('interval','?')} {rec.get('frequency','?')}"
                      + (f" from {rec.get('startTime')}" if rec.get("startTime") else ""))
    if ttype == "TumblingWindowTrigger":
        detail.append(f"tumbling {tp.get('interval','?')} {tp.get('frequency','?')}")
        for dep in tp.get("dependsOn", []) or []:
            detail.append("waits on trigger: "
                          + (dep.get("referenceTrigger") or {}).get("referenceName", "?"))
    if ttype in ("BlobEventsTrigger", "CustomEventsTrigger"):
        detail.append("event: " + squeeze(json.dumps(
            {k: tp[k] for k in ("blobPathBeginsWith", "blobPathEndsWith", "events", "scope")
             if k in tp}, default=str), 200))
    return {
        "name": name, "type": ttype,
        "state": props.get("runtimeState", "") or "Unknown",
        "startsPipelines": [(p.get("pipelineReference") or {}).get("referenceName", "?")
                            for p in props.get("pipelines", []) or []],
        "detail": " | ".join(detail),
    }


# ---------------------------------------------------------------------------
# The analyzer
# ---------------------------------------------------------------------------

class Analyzer:
    RESOLVABLE = {"dataset": "dataset", "dataflow": "dataflow",
                  "pipeline": "pipeline", "linked_service": "linkedservice"}
    IMPACT = {
        "dataset": "physical table/path unknown — lineage shows an opaque dataset name",
        "dataflow": "transformation logic invisible — sink lineage cannot be traced",
        "pipeline": "child pipeline logic invisible — its reads/writes are missing entirely",
        "linked_service": "target server/database/storage account unknown",
    }

    def __init__(self, store: Dict[str, Dict[str, dict]]):
        self.store = store
        self.ds_info = {n: describe_dataset(n, p) for n, p in store["dataset"].items()}
        self.ls_info = {n: describe_linked_service(n, p) for n, p in store["linkedservice"].items()}
        self.pipelines: List[dict] = []
        self.dataflows: List[dict] = []
        self.entities: Dict[str, dict] = {}
        self.edges: List[dict] = []
        self.refs: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        self.warnings: List[dict] = []
        self.pipeline_calls: List[dict] = []          # {parent, child, activity, waitOnCompletion}
        self._df_done: Set[str] = set()
        self._no_retry: List[str] = []

    def warn(self, severity: str, category: str, message: str) -> None:
        self.warnings.append({"severity": severity, "category": category, "message": message})

    # ---- entity registry ------------------------------------------------

    def node(self, kind: str, label: str, role: str, ref: str,
             detail: str = "", alias: str = "",
             endpoint: Optional[dict] = None) -> str:
        """Register an entity and return its canonical graph key."""
        label = squeeze(label, 200)
        if not label:
            return ""
        key = f"{kind}:{canon_table(label) if kind == 'table' else label.lower()}"
        ent = self.entities.setdefault(key, {
            "kind": kind, "label": label, "aliases": set(),
            "roles": set(), "refs": set(), "detail": "",
            "dynamic": is_dynamic(label),
            "endpoint": dict(EMPTY_ENDPOINT),
        })
        ent["roles"].add(role)
        ent["refs"].add(ref)
        if alias and alias != label:
            ent["aliases"].add(alias)
        if detail and not ent["detail"]:
            ent["detail"] = detail
        if endpoint:
            for k, v in endpoint.items():
                if v is not None and ent["endpoint"].get(k) is None:
                    ent["endpoint"][k] = v
        return key

    def track_ref(self, kind: str, name: str, ref: str) -> None:
        if name:
            self.refs[(kind, name)].add(ref)

    def _ls_endpoint(self, ls_name: str) -> dict:
        info = self.ls_info.get(ls_name)
        if not info:
            return dict(EMPTY_ENDPOINT)
        ep = dict(EMPTY_ENDPOINT)
        ep["system"] = info["system"]
        ep["server"] = info["server"]
        ep["database"] = info["database"]
        ep["url"] = info["url"]
        return ep

    def dataset_node(self, ds_name: str, role: str, ref: str,
                     params: Optional[dict] = None) -> str:
        """Resolve a dataset reference to a canonical physical node."""
        self.track_ref("dataset", ds_name, ref)
        info = self.ds_info.get(ds_name)
        if not info:
            return self.node("dataset", ds_name, role, ref, "definition not supplied")

        ls = info["linkedService"]
        if ls:
            self.track_ref("linked_service", ls, ref)
            self.node("linked_service", ls, role, ref,
                      detail=self.ls_info.get(ls, {}).get("system", ""))
        endpoint = self._ls_endpoint(ls)
        for k in ("schema", "object", "path", "url", "container"):
            if info["endpoint"].get(k) is not None:
                endpoint[k] = info["endpoint"][k]

        detail_bits = [b for b in (info["type"], ls) if b]
        if params:
            detail_bits.append("params: " + squeeze(json.dumps(params, default=str), 120))
        detail = " | ".join(detail_bits)

        display = info["display"]
        if display and not info["dynamic"]:
            if endpoint["object"]:
                return self.node("table", display, role, ref, detail,
                                 alias=ds_name, endpoint=endpoint)
            return self.node("file_path", display, role, ref, detail,
                             alias=ds_name, endpoint=endpoint)
        # dynamic or unresolved — keep the dataset itself as the node
        if display:
            detail = (detail + " | resolves to: " + squeeze(display, 120)).strip(" |")
        key = self.node("dataset", ds_name, role, ref, detail, endpoint=endpoint)
        if key and info["dynamic"]:
            self.entities[key]["dynamic"] = True
        return key

    def sql_nodes(self, sql: str, ref: str, read_role="read (SQL)",
                  write_role="written (SQL)") -> Tuple[List[str], List[str]]:
        reads, writes, procs = harvest_sql(sql)
        rk = [self.node("table", t, read_role, ref,
                        endpoint={**EMPTY_ENDPOINT,
                                  "schema": split_table(t)[0], "object": split_table(t)[1]})
              for t in reads]
        wk = [self.node("table", t, write_role, ref,
                        endpoint={**EMPTY_ENDPOINT,
                                  "schema": split_table(t)[0], "object": split_table(t)[1]})
              for t in writes]
        for p in procs:
            self.node("stored_procedure", p, "executed", ref)
        return [k for k in rk if k], [k for k in wk if k]

    def edge(self, sources, sinks, mechanism, pipeline, activity,
             detail="", opaque=False, dynamic=False) -> None:
        sources = [s for s in dict.fromkeys(sources) if s]
        sinks = [s for s in dict.fromkeys(sinks) if s]
        if not sources and not sinks:
            return
        self.edges.append({
            "sources": sources, "sinks": sinks, "mechanism": mechanism,
            "pipeline": pipeline, "activity": activity,
            "detail": squeeze(detail, 300), "opaque": opaque, "dynamic": dynamic,
        })

    # ---- pipelines ------------------------------------------------------

    def run(self) -> dict:
        # datasets reference linked services even when no pipeline uses them —
        # count those references so unused-detection is about the factory, not
        # about which pipelines happened to be supplied
        for ds_name, info in self.ds_info.items():
            if info["linkedService"]:
                self.track_ref("linked_service", info["linkedService"], f"dataset {ds_name}")

        for name, props in sorted(self.store["pipeline"].items()):
            self.pipelines.append(self.walk_pipeline(name, props))
        for name in sorted(self.store["dataflow"]):
            if name not in self._df_done:
                self.document_dataflow(name, "(not referenced by any supplied pipeline)", "", "")
        self.build_graph()
        self.rollups()
        self.checks()
        return self.as_dict()

    def walk_pipeline(self, name: str, props: dict) -> dict:
        acts: List[dict] = []
        self._walk(name, props.get("activities", []) or [], "(root)", 0, acts)
        cats: Dict[str, int] = defaultdict(int)
        for a in acts:
            cats[a["category"]] += 1
        return {
            "name": name,
            "description": squeeze(props.get("description"), 400) or None,
            "folder": ((props.get("folder") or {}).get("name")) or None,
            "parameters": {k: (v or {}).get("type", "?")
                           for k, v in (props.get("parameters") or {}).items()},
            "variables": {k: (v or {}).get("type", "?")
                          for k, v in (props.get("variables") or {}).items()},
            "concurrency": props.get("concurrency"),
            "activityCount": len(acts),
            "categories": dict(sorted(cats.items())),
            "activities": acts,
        }

    def _walk(self, pipeline, acts, scope, depth, sink_list) -> None:
        order = self._sequence(acts)
        for act in acts:
            row = self._activity(pipeline, act, scope, depth)
            row["step"] = order.get(row["activity"], "?")
            row["parallelWith"] = [n for n, s in order.items()
                                   if s == row["step"] and n != row["activity"]]
            sink_list.append(row)
            self._recurse(pipeline, act, scope, depth, sink_list)

    def _recurse(self, pipeline, act, scope, depth, sink_list) -> None:
        name, atype = act.get("name", "?"), act.get("type", "?")
        tp = act.get("typeProperties", {}) or {}
        base = name if scope == "(root)" else f"{scope} > {name}"
        if atype in ("ForEach", "Until"):
            self._walk(pipeline, tp.get("activities", []) or [],
                       f"{base} [{atype}]", depth + 1, sink_list)
        elif atype == "IfCondition":
            self._walk(pipeline, tp.get("ifTrueActivities", []) or [],
                       f"{base} [True]", depth + 1, sink_list)
            self._walk(pipeline, tp.get("ifFalseActivities", []) or [],
                       f"{base} [False]", depth + 1, sink_list)
        elif atype == "Switch":
            for case in tp.get("cases", []) or []:
                self._walk(pipeline, case.get("activities", []) or [],
                           f"{base} [Case={case.get('value','?')}]", depth + 1, sink_list)
            self._walk(pipeline, tp.get("defaultActivities", []) or [],
                       f"{base} [Default]", depth + 1, sink_list)

    @staticmethod
    def _sequence(acts: List[dict]) -> Dict[str, int]:
        """BFS-levelled topological order: same step == can run in parallel."""
        names = [a.get("name", "?") for a in acts]
        idx = {n: i for i, n in enumerate(names)}
        indeg = {n: 0 for n in names}
        succ = defaultdict(list)
        for a in acts:
            for dep in a.get("dependsOn", []) or []:
                parent = dep.get("activity")
                if parent in indeg:
                    indeg[a.get("name", "?")] += 1
                    succ[parent].append(a.get("name", "?"))
        q = deque(sorted([n for n in names if indeg[n] == 0], key=idx.get))
        order, step = {}, 0
        while q:
            batch = sorted(q, key=idx.get)
            q.clear()
            step += 1
            for n in batch:
                order[n] = step
                for s in succ[n]:
                    indeg[s] -= 1
                    if indeg[s] == 0:
                        q.append(s)
        for n in names:
            order.setdefault(n, 0)   # 0 == cycle
        return order

    # ---- one activity ---------------------------------------------------

    def _activity(self, pipeline: str, act: dict, scope: str, depth: int) -> dict:
        name, atype = act.get("name", "?"), act.get("type", "?")
        tp = act.get("typeProperties", {}) or {}
        cat, default_desc = ACTIVITY_CATALOG.get(atype, ("other", ""))
        ref = f"{pipeline}::{name}"
        policy = act.get("policy", {}) or {}
        detail, reads, writes, extras = self._extract(pipeline, name, act, atype, tp, ref)
        row = {
            "activity": name, "type": atype, "category": cat, "scope": scope,
            "depth": depth,
            "description": squeeze(act.get("description"), 300) or default_desc,
            "reads": sorted({k for k in reads if k in self.entities}),
            "writes": sorted({k for k in writes if k in self.entities}),
            "detail": squeeze(detail, 400),
            "dependsOn": [
                {"activity": d.get("activity"),
                 "on": ",".join(d.get("dependencyConditions", []) or [])}
                for d in act.get("dependsOn", []) or []
            ],
        }
        dyn_extract = extras.pop("_dyn", False)
        row.update(extras)
        # Parameter passing is normal; a dynamically built *data target* (query,
        # script, or dataset path) is what makes static lineage unreliable —
        # only flag the latter.
        touches_dynamic_entity = any(self.entities.get(k, {}).get("dynamic")
                                     for k in row["reads"] + row["writes"])
        if dyn_extract or touches_dynamic_entity:
            row["dynamic"] = True
        if atype in OPAQUE_TYPES:
            row["opaque"] = True
        if policy.get("retry"):
            row["retry"] = policy["retry"]
        if policy.get("timeout"):
            row["timeout"] = as_text(policy["timeout"])
        if act.get("state") == "Inactive":
            row["inactive"] = True
        return row

    def _extract(self, pipeline, name, act, atype, tp, ref):
        bits, reads, writes = [], [], []
        extras: Dict[str, Any] = {}
        query_text = None
        dyn = False

        if atype == "Copy":
            src, snk = tp.get("source", {}) or {}, tp.get("sink", {}) or {}
            for item in act.get("inputs", []) or []:
                if item.get("type") == "DatasetReference":
                    reads.append(self.dataset_node(item.get("referenceName", ""),
                                                   "copy source", ref,
                                                   item.get("parameters")))
            for item in act.get("outputs", []) or []:
                if item.get("type") == "DatasetReference":
                    writes.append(self.dataset_node(item.get("referenceName", ""),
                                                    "copy sink", ref,
                                                    item.get("parameters")))
            query = as_text(src.get("sqlReaderQuery") or src.get("oracleReaderQuery")
                            or src.get("query"))
            proc = as_text(src.get("sqlReaderStoredProcedureName"))
            pre = as_text(snk.get("preCopyScript"))
            r_k, _ = self.sql_nodes(query, ref, read_role="copy source (query)")
            reads += r_k
            if proc:
                self.node("stored_procedure", proc, "copy source", ref)
            _, w_k = self.sql_nodes(pre, ref, write_role="pre-copy script target")
            writes += w_k
            bits.append(f"{src.get('type','?')} → {snk.get('type','?')}")
            if snk.get("writeBehavior") or snk.get("writeMethod"):
                bits.append(f"write: {as_text(snk.get('writeBehavior') or snk.get('writeMethod'))}")
            if query:
                bits.append("source query supplied")
                query_text = query
            if proc:
                bits.append("source proc: " + proc)
            if pre:
                bits.append("pre-copy: " + squeeze(pre, 150))
            if tp.get("enableStaging"):
                bits.append("staged copy")
            tr = tp.get("translator") or {}
            n_map = len(tr.get("mappings", []) or [])
            if n_map:
                bits.append(f"explicit column mapping ({n_map} cols)")
            dyn = is_dynamic(query) or is_dynamic(pre)
            self.edge(reads, writes, "Copy", pipeline, name,
                      squeeze(query or proc, 200), dynamic=dyn)

        elif atype == "Lookup":
            ds = (tp.get("dataset") or {}).get("referenceName", "")
            src = tp.get("source", {}) or {}
            query = as_text(src.get("sqlReaderQuery") or src.get("query"))
            if ds:
                reads.append(self.dataset_node(ds, "lookup", ref,
                                               (tp.get("dataset") or {}).get("parameters")))
            r_k, _ = self.sql_nodes(query, ref, read_role="lookup (query)")
            reads += r_k
            bits.append("first row only" if tp.get("firstRowOnly", True) else "full rowset")
            if query:
                query_text = query
            self.edge(reads, [], "Lookup", pipeline, name, squeeze(query, 150))

        elif atype in ("GetMetadata", "Validation", "Delete"):
            ds = (tp.get("dataset") or {}).get("referenceName", "")
            role = {"Delete": "deleted", "GetMetadata": "metadata read",
                    "Validation": "existence check"}[atype]
            if ds:
                key = self.dataset_node(ds, role, ref,
                                        (tp.get("dataset") or {}).get("parameters"))
                (writes if atype == "Delete" else reads).append(key)
            if atype == "GetMetadata":
                bits.append("fields: " + ", ".join(as_text(f) for f in tp.get("fieldList", []) or []))
            if atype == "Delete":
                bits.append("destructive")
                self.edge([], writes, "Delete", pipeline, name, "destructive")

        elif atype == "SqlServerStoredProcedure":
            proc = as_text(tp.get("storedProcedureName"))
            ls = (act.get("linkedServiceName") or {}).get("referenceName", "")
            pk = self.node("stored_procedure", proc, "executed", ref,
                           detail=f"on {ls}" if ls else "",
                           endpoint={**self._ls_endpoint(ls),
                                     "schema": split_table(proc)[0],
                                     "object": split_table(proc)[1]})
            if ls:
                self.track_ref("linked_service", ls, ref)
                self.node("linked_service", ls, "proc target", ref)
            writes.append(pk)
            params = tp.get("storedProcedureParameters", {}) or {}
            bits.append(f"proc: {proc}")
            if params:
                bits.append("params: " + ", ".join(params.keys()))
            self.edge([], [pk], "StoredProcedure", pipeline, name,
                      "logic inside the proc is not visible to ADF", opaque=True)

        elif atype == "Script":
            ls = (act.get("linkedServiceName") or {}).get("referenceName", "")
            if ls:
                self.track_ref("linked_service", ls, ref)
                self.node("linked_service", ls, "script target", ref)
            all_r, all_w, texts = [], [], []
            for s in tp.get("scripts", []) or []:
                text = as_text(s.get("text"))
                r_k, w_k = self.sql_nodes(text, ref, read_role="read (script)",
                                          write_role="written (script)")
                all_r += r_k
                all_w += w_k
                texts.append(f"-- [{s.get('type','Query')}]\n{text}")
                dyn = dyn or is_dynamic(text)
            reads += all_r
            writes += all_w
            bits.append(f"{len(texts)} script block(s)")
            if texts:
                query_text = "\n\n".join(texts)
            self.edge(all_r, all_w, "Script", pipeline, name, dynamic=dyn)

        elif atype == "ExecutePipeline":
            child = (tp.get("pipeline") or {}).get("referenceName", "")
            self.track_ref("pipeline", child, ref)
            wait = tp.get("waitOnCompletion", True)
            self.pipeline_calls.append({"parent": pipeline, "child": child,
                                        "activity": name, "waitOnCompletion": bool(wait)})
            extras["invokesPipeline"] = child
            bits.append(f"invokes: {child}" + ("" if wait else " (fire-and-forget)"))
            params = tp.get("parameters", {}) or {}
            if params:
                bits.append("params: " + squeeze(json.dumps(params, default=str), 250))

        elif atype == "ExecuteDataFlow":
            df_ref = tp.get("dataFlow") or tp.get("dataflow") or {}
            df = df_ref.get("referenceName", "") if isinstance(df_ref, dict) else ""
            self.track_ref("dataflow", df, ref)
            extras["invokesDataflow"] = df
            bits.append(f"data flow: {df}")
            if isinstance(df_ref, dict):
                if df_ref.get("parameters"):
                    bits.append("df params: " + squeeze(json.dumps(df_ref["parameters"], default=str), 250))
                if df_ref.get("datasetParameters"):
                    bits.append("dataset params: " + squeeze(
                        json.dumps(df_ref["datasetParameters"], default=str), 250))
            doc = self.document_dataflow(df, ref, pipeline, name)
            if doc:
                reads += doc["sourceKeys"]
                writes += doc["sinkKeys"]
                bits.append(f"{len(doc['steps'])} transformations")
            else:
                bits.append("(definition not supplied — logic not expanded)")

        elif atype in OPAQUE_TYPES:
            target = as_text(tp.get("notebookPath") or tp.get("pythonFile")
                             or tp.get("mainClassName") or tp.get("packagePath")
                             or tp.get("notebook") or "")
            kind = "notebook" if "Notebook" in atype else "code_artifact"
            key = self.node(kind, target or f"{atype} in {name}", "executed", ref)
            ls = (act.get("linkedServiceName") or {}).get("referenceName", "")
            if ls:
                self.track_ref("linked_service", ls, ref)
                self.node("linked_service", ls, "compute", ref)
            if target:
                bits.append(f"{kind}: {target}")
            params = tp.get("baseParameters") or tp.get("parameters") or {}
            if params:
                bits.append("params: " + squeeze(json.dumps(params, default=str), 250))
            self.edge([key], [key], atype, pipeline, name,
                      "reads/writes happen inside external code — inspect the artifact",
                      opaque=True)

        elif atype in ("WebActivity", "WebHook", "AzureFunctionActivity"):
            url = as_text(tp.get("url"))
            fn = as_text(tp.get("functionName"))
            key = self.node("endpoint", fn or url or name, "called", ref,
                            endpoint={**EMPTY_ENDPOINT, "url": url or None,
                                      "system": "Azure Function" if fn else None})
            bits.append(f"{as_text(tp.get('method')) or 'CALL'} {squeeze(fn or url, 200)}")
            self.edge([], [key], atype, pipeline, name, opaque=True)

        elif atype == "ForEach":
            items = as_text(tp.get("items"))
            bits.append(("sequential" if tp.get("isSequential") else "parallel")
                        + (f", batch={tp['batchCount']}" if tp.get("batchCount") else "")
                        + f" over {squeeze(items, 200)}")
        elif atype == "Until":
            bits.append("until " + squeeze(tp.get("expression"), 200))
        elif atype == "IfCondition":
            bits.append("if " + squeeze(tp.get("expression"), 200))
        elif atype == "Switch":
            bits.append("on " + squeeze(tp.get("on"), 150) + " | cases: "
                        + ", ".join(str(c.get("value")) for c in tp.get("cases", []) or []))
        elif atype == "Filter":
            bits.append(f"{squeeze(tp.get('items'),120)} where {squeeze(tp.get('condition'),120)}")
        elif atype in ("SetVariable", "AppendVariable"):
            bits.append(f"{tp.get('variableName','?')} = {squeeze(tp.get('value'), 180)}")
        elif atype == "Wait":
            bits.append(f"wait {as_text(tp.get('waitTimeInSeconds'))}s")
        elif atype == "Fail":
            bits.append("fail: " + squeeze(tp.get("message"), 150))
        else:
            bits.append(squeeze(json.dumps(tp, default=str), 250))

        if query_text:
            extras["query"] = squeeze(query_text, 4000)
        if dyn:
            extras["_dyn"] = True
        return " | ".join(b for b in bits if b), reads, writes, extras

    # ---- data flows -----------------------------------------------------

    def _df_endpoints(self, tp: dict, ref: str) -> Dict[str, Dict[str, dict]]:
        eps: Dict[str, Dict[str, dict]] = {"source": {}, "sink": {}}
        for key, bucket in (("sources", "source"), ("sinks", "sink")):
            role = f"dataflow {bucket}"
            for item in tp.get(key, []) or []:
                nm = item.get("name", "?")
                ds = (item.get("dataset") or {}).get("referenceName", "")
                ls = (item.get("linkedService") or {}).get("referenceName", "")
                fl = (item.get("flowlet") or {}).get("referenceName", "")
                node_key, label = "", nm
                if ds:
                    node_key = self.dataset_node(ds, role, ref)
                    label = self.entities.get(node_key, {}).get("label", ds)
                elif ls:
                    self.track_ref("linked_service", ls, ref)
                    node_key = self.node("inline_dataset", f"inline via {ls}", role, ref,
                                         endpoint=self._ls_endpoint(ls))
                    label = f"inline via {ls}"
                if fl:
                    self.track_ref("dataflow", fl, ref)
                eps[bucket][nm] = {"key": node_key, "label": label,
                                   "dataset": ds, "linkedService": ls, "flowlet": fl}
        return eps

    def document_dataflow(self, df_name: str, ref: str,
                          pipeline: str = "", activity: str = "") -> Optional[dict]:
        if not df_name:
            return None
        props = self.store["dataflow"].get(df_name)
        if not props:
            return None
        if df_name in self._df_done:
            existing = next(d for d in self.dataflows if d["name"] == df_name)
            if ref not in existing["usedBy"]:
                existing["usedBy"].append(ref)
            return existing
        self._df_done.add(df_name)

        tp = props.get("typeProperties", {}) or {}
        eps = self._df_endpoints(tp, ref)
        steps = parse_dataflow_script(tp)
        if not steps:
            steps = ([{"name": s.get("name", "?"), "op": "source", "inputs": [],
                       "inputs_raw": [], "streams": [], "config": ""}
                      for s in tp.get("sources", []) or []]
                     + [{"name": t.get("name", "?"), "op": "transformation", "inputs": [],
                         "inputs_raw": [], "streams": [], "config": ""}
                        for t in tp.get("transformations", []) or []]
                     + [{"name": s.get("name", "?"), "op": "sink", "inputs": [],
                         "inputs_raw": [], "streams": [], "config": ""}
                        for s in tp.get("sinks", []) or []])
            self.warn("warning", "data flow",
                      f"Data flow '{df_name}' has no script — transformation logic and "
                      f"internal lineage are unavailable.")

        order = dataflow_exec_order(steps)
        dead = dataflow_dead_ends(steps)
        sink_names = list(eps["sink"]) or [s["name"] for s in steps if s["op"] == "sink"]
        traces = trace_dataflow(steps, sink_names)

        for sink, sources, chain in traces:
            src_keys = [eps["source"].get(s, {}).get("key") for s in sources]
            snk_key = eps["sink"].get(sink, {}).get("key")
            self.edge([k for k in src_keys if k], [snk_key] if snk_key else [],
                      f"DataFlow:{df_name}", pipeline or "(unreferenced)",
                      activity or df_name, " → ".join(chain))

        doc = {
            "name": df_name,
            "description": squeeze(props.get("description"), 300) or None,
            "usedBy": [ref],
            "sources": [{"stream": k, **{kk: vv for kk, vv in v.items() if kk != "key"}}
                        for k, v in eps["source"].items()],
            "sinks": [{"stream": k, **{kk: vv for kk, vv in v.items() if kk != "key"}}
                      for k, v in eps["sink"].items()],
            "sourceKeys": [v["key"] for v in eps["source"].values() if v["key"]],
            "sinkKeys": [v["key"] for v in eps["sink"].values() if v["key"]],
            "steps": [{
                "execOrder": order.get(s["name"], 0),
                "name": s["name"],
                "op": s["op"],
                "inputs": s.get("inputs_raw") or s["inputs"],
                "outputStreams": s["streams"],
                "deadEnd": any(d.startswith(s["name"]) for d in dead),
                "config": squeeze(s["config"], 400),
            } for s in steps],
            "sinkTraces": [{"sink": s, "sources": src, "chain": ch} for s, src, ch in traces],
            "deadEnds": dead,
        }
        if dead:
            self.warn("warning", "data flow",
                      f"Data flow '{df_name}' has unconsumed output: {', '.join(dead)} "
                      f"(dead logic, or a connection that was never made).")
        self.dataflows.append(doc)
        return doc

    # ---- graph closure --------------------------------------------------

    def build_graph(self) -> None:
        fwd: Dict[str, Set[str]] = defaultdict(set)
        rev: Dict[str, Set[str]] = defaultdict(set)
        for e in self.edges:
            for s in e["sources"]:
                for t in e["sinks"]:
                    if s == t:
                        continue
                    fwd[s].add(t)
                    rev[t].add(s)

        def closure(graph, start):
            seen, q = set(), deque(graph.get(start, ()))
            while q:
                n = q.popleft()
                if n in seen:
                    continue
                seen.add(n)
                q.extend(graph.get(n, ()))
            seen.discard(start)
            return sorted(seen)

        self.upstream = {k: closure(rev, k) for k in self.entities}
        self.downstream = {k: closure(fwd, k) for k in self.entities}

    # ---- rollups: own vs effective reads/writes -------------------------

    def rollups(self) -> None:
        by_name = {p["name"]: p for p in self.pipelines}
        children = defaultdict(list)
        for c in self.pipeline_calls:
            children[c["parent"]].append(c["child"])

        for p in self.pipelines:
            p["ownReads"] = sorted({k for a in p["activities"] for k in a["reads"]})
            p["ownWrites"] = sorted({k for a in p["activities"] for k in a["writes"]})

        memo: Dict[str, Tuple[Set[str], Set[str], Set[str]]] = {}

        def effective(name: str, visiting: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
            if name in memo:
                return memo[name]
            if name in visiting:            # recursion cycle — stop here
                return set(), set(), set()
            p = by_name.get(name)
            if p is None:                   # child not supplied
                return set(), set(), {name}
            visiting = visiting | {name}
            r, w, miss = set(p["ownReads"]), set(p["ownWrites"]), set()
            for child in children.get(name, []):
                cr, cw, cm = effective(child, visiting)
                r |= cr
                w |= cw
                miss |= cm
                if child not in by_name:
                    miss.add(child)
            memo[name] = (r, w, miss)
            return memo[name]

        for p in self.pipelines:
            r, w, miss = effective(p["name"], set())
            p["effectiveReads"] = sorted(r)
            p["effectiveWrites"] = sorted(w)
            p["effectiveIncomplete"] = sorted(miss)
            p["opaqueCount"] = sum(1 for a in p["activities"] if a.get("opaque"))
            p["dynamicCount"] = sum(1 for a in p["activities"] if a.get("dynamic"))

        # orchestration relationships
        trigger_starts = defaultdict(list)
        self.triggers = [describe_trigger(n, props)
                         for n, props in sorted(self.store["trigger"].items())]
        for t in self.triggers:
            for pl in t["startsPipelines"]:
                self.track_ref("pipeline", pl, f"trigger {t['name']}")
                trigger_starts[pl].append(t["name"])

        invoked_by = defaultdict(list)
        for c in self.pipeline_calls:
            invoked_by[c["child"]].append(f"{c['parent']}::{c['activity']}")

        for p in self.pipelines:
            p["triggers"] = sorted(trigger_starts.get(p["name"], []))
            p["invokedBy"] = sorted(set(invoked_by.get(p["name"], [])))
            p["invokes"] = sorted({c["child"] for c in self.pipeline_calls
                                   if c["parent"] == p["name"]})
            p["dataflows"] = sorted({a["invokesDataflow"] for a in p["activities"]
                                     if a.get("invokesDataflow")})
            p["entryPoint"] = not p["triggers"] and not p["invokedBy"]
            p["roleLine"] = self._role_line(p)

    @staticmethod
    def _role_line(p: dict) -> str:
        cats = p["categories"]
        moves = cats.get("movement", 0) + cats.get("transform", 0)
        orch = cats.get("orchestration", 0)
        bits = []
        if p["invokes"]:
            bits.append(f"orchestrates {len(p['invokes'])} pipeline(s)")
        if p["dataflows"]:
            bits.append(f"runs {len(p['dataflows'])} data flow(s)")
        if moves:
            n_src = len(p["effectiveReads"]) if "effectiveReads" in p else len(p["ownReads"])
            n_snk = len(p["effectiveWrites"]) if "effectiveWrites" in p else len(p["ownWrites"])
            bits.append(f"moves/transforms data ({n_src} read, {n_snk} written)")
        if not bits:
            if cats.get("external"):
                bits.append("calls external services")
            elif cats.get("management"):
                bits.append("housekeeping (delete/metadata)")
            else:
                bits.append("control/utility")
        return "; ".join(bits)

    # ---- checks ---------------------------------------------------------

    def checks(self) -> None:
        self.resolution = []
        for (kind, ref_name), refs in sorted(self.refs.items()):
            resolved = ref_name in self.store[self.RESOLVABLE[kind]]
            self.resolution.append({
                "kind": kind, "name": ref_name,
                "status": "resolved" if resolved else "MISSING",
                "impact": "" if resolved else self.IMPACT[kind],
                "referencedBy": sorted(refs),
            })
            if not resolved:
                self.warn("warning", "unresolved reference",
                          f"Unresolved {kind} '{ref_name}' — {self.IMPACT[kind]}")

        for p in self.pipelines:
            for a in p["activities"]:
                loc = f"{p['name']}::{a['activity']}"
                if a.get("dynamic"):
                    self.warn("info", "dynamic target",
                              f"{loc} builds its target dynamically — the actual "
                              f"table/path is only known at runtime.")
                if a.get("opaque"):
                    self.warn("info", "opaque",
                              f"{loc} delegates to external code ({a['type']}) — "
                              f"ADF cannot see what it reads or writes.")
                if a.get("inactive"):
                    self.warn("warning", "inactive activity",
                              f"{loc} is set Inactive — it is skipped at runtime.")
                for dep in a["dependsOn"]:
                    conds = set((dep["on"] or "").split(","))
                    if conds & {"Failed", "Skipped"}:
                        self.warn("info", "error path",
                                  f"{loc} runs on '{dep['on']}' of {dep['activity']} — "
                                  f"error-handling path, not the happy path.")
                    elif "Completed" in conds:
                        self.warn("info", "error path",
                                  f"{loc} runs on 'Completed' of {dep['activity']} — "
                                  f"it executes whether {dep['activity']} succeeds or fails.")
                if a["step"] == 0:
                    self.warn("warning", "cycle",
                              f"{loc} is in a dependency cycle.")
                if a["category"] in ("movement", "transform") and not a.get("retry"):
                    self._no_retry.append(loc)

        if self._no_retry:
            self.warn("info", "resilience",
                      f"{len(self._no_retry)} data/transform activities have no retry "
                      f"policy: " + ", ".join(self._no_retry[:10])
                      + (" …" if len(self._no_retry) > 10 else ""))

        # unused resources — factory-scope claims only, worded accordingly
        for ds_name in sorted(self.store["dataset"]):
            if ("dataset", ds_name) not in self.refs:
                self.warn("warning", "unreferenced resource",
                          f"Dataset '{ds_name}' is not referenced by any supplied "
                          f"pipeline or data flow. It may still be used by pipelines "
                          f"not included in this input.")
        for ls_name in sorted(self.store["linkedservice"]):
            if ("linked_service", ls_name) not in self.refs:
                self.warn("warning", "unreferenced resource",
                          f"Linked service '{ls_name}' is not referenced by any "
                          f"supplied dataset, pipeline, or data flow.")
        for df in self.dataflows:
            if all(u.startswith("(not referenced") for u in df["usedBy"]):
                self.warn("warning", "unreferenced resource",
                          f"Data flow '{df['name']}' is not invoked by any supplied "
                          f"pipeline.")
        if self.store["trigger"] or len(self.pipelines) > 1:
            for p in self.pipelines:
                if p["entryPoint"] and not p["invokes"] and len(self.pipelines) > 1:
                    self.warn("info", "no known invoker",
                              f"Pipeline '{p['name']}' is not started by any trigger or "
                              f"pipeline in this factory. It may be invoked externally "
                              f"(REST API, Synapse, Logic Apps) or be dead.")
        for t in getattr(self, "triggers", []):
            if t["state"] and t["state"].lower() not in ("started", "unknown"):
                self.warn("warning", "trigger state",
                          f"Trigger '{t['name']}' is {t['state']} — the pipelines it "
                          f"references do not run on this schedule/event until it is started.")
            if not t["startsPipelines"]:
                self.warn("info", "trigger state",
                          f"Trigger '{t['name']}' starts no pipelines.")

        for ls_name, info in sorted(self.ls_info.items()):
            if info["inlineCredential"]:
                self.warn("warning", "credential hygiene",
                          f"Linked service '{ls_name}' appears to contain an inline "
                          f"credential in its connection string (value not shown here). "
                          f"Prefer Key Vault or managed identity.")

        for key, ent in self.entities.items():
            if ent["kind"] in ("table", "file_path") and not self.upstream.get(key) \
                    and not self.downstream.get(key):
                self.warn("info", "isolated entity",
                          f"{ent['label']} is touched but participates in no data "
                          f"movement (read-only reference or metadata-only use).")

        for fname, meta in (self.store.get("__skipped__") or {}).items():
            self.warn("warning", "input",
                      f"Input file '{fname}' could not be parsed and was skipped: "
                      f"{meta['error']}")

        sev_order = {"warning": 0, "info": 1}
        seen = set()
        deduped = []
        for w in sorted(self.warnings, key=lambda w: (sev_order.get(w["severity"], 2),
                                                      w["category"], w["message"])):
            sig = (w["severity"], w["category"], w["message"])
            if sig not in seen:
                seen.add(sig)
                deduped.append(w)
        self.warnings = deduped

    # ---- serialise ------------------------------------------------------

    def as_dict(self) -> dict:
        ents = []
        for key, e in sorted(self.entities.items()):
            ents.append({
                "key": key, "kind": e["kind"], "label": e["label"],
                "aliases": sorted(e["aliases"]), "roles": sorted(e["roles"]),
                "detail": e["detail"], "dynamic": e["dynamic"],
                "endpoint": e["endpoint"],
                "referencedBy": sorted(e["refs"]),
                "upstream": self.upstream.get(key, []),
                "downstream": self.downstream.get(key, []),
            })

        datasets = []
        for name in sorted(self.store["dataset"]):
            info = self.ds_info[name]
            refs = sorted(self.refs.get(("dataset", name), []))
            datasets.append({**info, "referencedBy": refs, "unreferenced": not refs})

        linked_services = []
        for name in sorted(self.store["linkedservice"]):
            info = self.ls_info[name]
            refs = sorted(self.refs.get(("linked_service", name), []))
            linked_services.append({**info, "referencedBy": refs, "unreferenced": not refs})

        orch_edges = []
        for t in self.triggers:
            for pl in t["startsPipelines"]:
                orch_edges.append({"from": t["name"], "fromKind": "trigger",
                                   "to": pl, "toKind": "pipeline",
                                   "detail": t["type"] + (f" ({t['state']})" if t["state"] else "")})
        for c in self.pipeline_calls:
            orch_edges.append({"from": c["parent"], "fromKind": "pipeline",
                               "to": c["child"], "toKind": "pipeline",
                               "detail": f"ExecutePipeline via {c['activity']}"
                                         + ("" if c["waitOnCompletion"] else " (fire-and-forget)")})
        for p in self.pipelines:
            for df in p["dataflows"]:
                orch_edges.append({"from": p["name"], "fromKind": "pipeline",
                                   "to": df, "toKind": "dataflow",
                                   "detail": "ExecuteDataFlow"})

        n_missing = sum(1 for r in self.resolution if r["status"] == "MISSING")
        mode = "partial" if n_missing else "factory"

        return {
            "mode": mode,
            "factory": {
                "pipelineCount": len(self.pipelines),
                "datasetCount": len(self.store["dataset"]),
                "dataflowCount": len(self.store["dataflow"]),
                "linkedServiceCount": len(self.store["linkedservice"]),
                "triggerCount": len(self.store["trigger"]),
                "activityCount": sum(p["activityCount"] for p in self.pipelines),
                "entityCount": len(self.entities),
                "lineageEdgeCount": len(self.edges),
                "unresolvedCount": n_missing,
                "warningCount": sum(1 for w in self.warnings if w["severity"] == "warning"),
            },
            "pipelines": self.pipelines,
            "pipelineCalls": self.pipeline_calls,
            "dataflows": self.dataflows,
            "datasets": datasets,
            "linkedServices": linked_services,
            "triggers": self.triggers,
            "entities": ents,
            "lineageEdges": [
                {"sources": e["sources"], "sinks": e["sinks"],
                 "mechanism": e["mechanism"], "pipeline": e["pipeline"],
                 "activity": e["activity"], "detail": e["detail"],
                 "opaque": e["opaque"], "dynamic": e["dynamic"]}
                for e in self.edges
            ],
            "orchestration": {"edges": orch_edges},
            "resolution": self.resolution,
            "warnings": self.warnings,
        }


def analyze(store: Dict[str, Dict[str, dict]]) -> dict:
    return Analyzer(store).run()
