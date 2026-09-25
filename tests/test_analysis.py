"""Regression tests for the analysis guarantees. Run: python -m unittest discover tests"""
import json
import os
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adfdocgen.loader import collect_inputs          # noqa: E402
from adfdocgen.analyzer import analyze               # noqa: E402
from adfdocgen.renderer import build_payload, render_html   # noqa: E402
from adfdocgen.word_writer import render_docx        # noqa: E402
from adfdocgen.agent_writer import render_agent_md   # noqa: E402

FOLDERS = {"pipeline": "pipeline", "dataset": "dataset", "dataflow": "dataflow",
           "linkedservice": "linkedService", "trigger": "trigger"}


def sql_ls(name, server, db, conn_extra=""):
    return ("linkedservice", name, {"type": "AzureSqlDatabase", "typeProperties": {
        "connectionString": f"Server={server};Database={db};{conn_extra}"}})


def adls_ls(name, account):
    return ("linkedservice", name, {"type": "AzureBlobFS", "typeProperties": {
        "url": f"https://{account}.dfs.core.windows.net"}})


def table_ds(name, ls, schema, table, params=None):
    props = {"type": "AzureSqlTable", "linkedServiceName": {"referenceName": ls,
             "type": "LinkedServiceReference"},
             "typeProperties": {"schema": schema, "table": table}}
    if params:
        props["parameters"] = {p: {"type": "string"} for p in params}
    return ("dataset", name, props)


def file_ds(name, ls, container, folder):
    return ("dataset", name, {"type": "Parquet", "linkedServiceName": {"referenceName": ls},
            "typeProperties": {"location": {"type": "AzureBlobFSLocation",
                                            "fileSystem": container, "folderPath": folder}}})


def pipeline(name, *activities, params=None):
    props = {"activities": list(activities)}
    if params:
        props["parameters"] = params
    return ("pipeline", name, props)


def copy(name, src, dst, source=None, src_params=None, dst_params=None):
    return {"name": name, "type": "Copy",
            "inputs": [{"referenceName": src, "type": "DatasetReference",
                        "parameters": src_params or {}}],
            "outputs": [{"referenceName": dst, "type": "DatasetReference",
                         "parameters": dst_params or {}}],
            "typeProperties": {"source": source or {"type": "AzureSqlSource"},
                               "sink": {"type": "AzureSqlSink"}}}


class Factory:
    """Write resources as an ADF Git folder, then load and analyse them."""

    def __init__(self, *resources, extra_files=None):
        self.tmp = tempfile.TemporaryDirectory()
        root = self.tmp.name
        for kind, name, props in resources:
            folder = os.path.join(root, FOLDERS[kind])
            os.makedirs(folder, exist_ok=True)
            with open(os.path.join(folder, name + ".json"), "w", encoding="utf-8") as fh:
                json.dump({"name": name, "properties": props}, fh)
        for rel, text in (extra_files or {}).items():
            path = os.path.join(root, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
        self.payload = build_payload(analyze(collect_inputs(root)), "test")
        self.ents = {e["key"]: e for e in self.payload["entities"]}
        self.pipes = {p["name"]: p for p in self.payload["pipelines"]}

    def outputs(self):
        """Every rendered output as text: HTML, JSON, Word XML, agent Markdown."""
        out = os.path.join(self.tmp.name, "_out")
        texts = {"json": json.dumps(self.payload)}
        texts["html"] = render_html(self.payload, os.path.join(out, "x.html")).read_text("utf-8")
        docx = render_docx(self.payload, os.path.join(out, "x.docx"))
        with zipfile.ZipFile(docx) as z:
            texts["docx"] = "".join(z.read(n).decode("utf-8", "ignore") for n in z.namelist())
        md = render_agent_md(self.payload, os.path.join(out, "x.agent.md"))
        texts["agent"] = open(md, encoding="utf-8").read()
        return texts

    def tables(self, label):
        return [e for e in self.payload["entities"]
                if e["kind"] == "table" and e["label"].lower() == label.lower()]

    def activity(self, pl, act):
        return next(a for a in self.pipes[pl]["activities"] if a["activity"] == act)


class Redaction(unittest.TestCase):
    def test_secret_markers_absent_from_every_output(self):
        f = Factory(
            sql_ls("Prod", "prod.sql.example", "SalesDW", "User ID=u;Password=MARKER_PW_1;"),
            ("linkedservice", "Sec", {"type": "AzureSqlDatabase", "typeProperties": {
                "connectionString": {"type": "SecureString",
                                     "value": "Server=s.example;Database=D;Password=MARKER_PW_2;"}}}),
            ("linkedservice", "Api", {"type": "RestService", "typeProperties": {
                "url": "https://user:MARKER_USERINFO@api.example.com/v1?code=MARKER_QS_1",
                "apiKey": {"type": "SecureString", "value": "MARKER_KEY_1"}}}),
            pipeline("Child", {"name": "W", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}},
                     params={"runKey2": {"type": "SecureString"}, "plain": {"type": "String"}}),
            pipeline("Parent",
                     {"name": "Call", "type": "WebActivity", "typeProperties": {
                         "method": "GET",
                         "url": "https://api.example.com/run?token=MARKER_QS_2&keep=yes",
                         "headers": {"Authorization": "Bearer MARKER_BEARER_123"}}},
                     {"name": "Run", "type": "ExecutePipeline", "typeProperties": {
                         "pipeline": {"referenceName": "Child"},
                         "parameters": {"runKey2": "MARKER_PARAM_1", "plain": "visible-value"}}},
                     {"name": "Odd", "type": "BrandNewActivity", "typeProperties": {
                         "thing": {"type": "SecureString", "value": "MARKER_SECURE_1"},
                         "note": "MARKER_UNKNOWN_1"}}),
        )
        for fmt, text in f.outputs().items():
            for marker in ("MARKER_PW_1", "MARKER_PW_2", "MARKER_USERINFO", "MARKER_QS_1",
                           "MARKER_KEY_1", "MARKER_QS_2", "MARKER_BEARER_123",
                           "MARKER_PARAM_1", "MARKER_SECURE_1", "MARKER_UNKNOWN_1"):
                self.assertNotIn(marker, text, f"{marker} leaked into {fmt}")
        self.assertIn("visible-value", f.outputs()["json"])   # ordinary values stay
        self.assertIn("keep=yes", f.outputs()["json"])
        self.assertGreater(f.payload["redactions"], 0)

    def test_secure_string_is_not_key_vault(self):
        f = Factory(("linkedservice", "Sec", {"type": "AzureSqlDatabase", "typeProperties": {
            "connectionString": {"type": "SecureString",
                                 "value": "Server=s.example;Database=D;Password=x;"}}}))
        ls = f.payload["linkedServices"][0]
        self.assertFalse(ls["keyVault"])
        self.assertTrue(ls["inlineCredential"])
        self.assertEqual((ls["server"], ls["database"]), ("s.example", "D"))


    def test_selection_mode_renders(self):
        tmp = tempfile.TemporaryDirectory()
        with open(os.path.join(tmp.name, "pl.json"), "w") as fh:
            json.dump({"name": "PL", "properties": {"activities": [{"name": "W", "type": "Wait"}]}}, fh)
        payload = build_payload(analyze(collect_inputs(tmp.name)), "t")
        self.assertEqual(payload["mode"], "selection")
        render_docx(payload, os.path.join(tmp.name, "o.docx"))
        render_html(payload, os.path.join(tmp.name, "o.html"))


class Identity(unittest.TestCase):
    def test_same_table_on_two_servers_stays_two_objects(self):
        f = Factory(
            sql_ls("Prod", "prod.sql.example", "SalesDW"),
            sql_ls("Arch", "archive.sql.example", "ArchiveDW"),
            table_ds("Src", "Prod", "dbo", "Sales"),
            table_ds("Dst", "Arch", "dbo", "Sales"),
            pipeline("PL", copy("Archive", "Src", "Dst")),
        )
        sales = f.tables("dbo.Sales")
        self.assertEqual(len(sales), 2)
        self.assertEqual({e["endpoint"]["server"] for e in sales},
                         {"prod.sql.example", "archive.sql.example"})
        prod = next(e for e in sales if e["endpoint"]["server"] == "prod.sql.example")
        arch = next(e for e in sales if e["endpoint"]["server"] == "archive.sql.example")
        edges = [e for e in f.payload["lineageEdges"] if e["activity"] == "Archive"]
        self.assertEqual(len(edges), 1)
        self.assertEqual((edges[0]["sources"], edges[0]["sinks"]), ([prod["key"]], [arch["key"]]))
        self.assertIn(arch["key"], prod["downstream"])

    def test_same_path_in_two_storage_accounts_stays_two_objects(self):
        f = Factory(
            adls_ls("A", "accounta"), adls_ls("B", "accountb"),
            file_ds("InA", "A", "raw", "sales"), file_ds("InB", "B", "raw", "sales"),
            pipeline("PL", copy("Move", "InA", "InB")),
        )
        files = [e for e in f.payload["entities"] if e["kind"] == "file_path"]
        self.assertEqual(len(files), 2)
        self.assertEqual(len({e["key"] for e in files}), 2)


class QuerySource(unittest.TestCase):
    def test_query_reads_its_tables_on_the_dataset_connection(self):
        f = Factory(
            sql_ls("Prod", "prod.sql.example", "SalesDW"),
            sql_ls("Arch", "archive.sql.example", "ArchiveDW"),
            table_ds("Src", "Prod", "dbo", "Sales"),
            table_ds("Dst", "Arch", "dbo", "Out"),
            pipeline("PL", copy("Q", "Src", "Dst", source={
                "type": "AzureSqlSource", "sqlReaderQuery": "SELECT * FROM dbo.Orders"})),
        )
        act = f.activity("PL", "Q")
        read_labels = [f.ents[k]["label"] for k in act["reads"]]
        self.assertEqual(read_labels, ["dbo.Orders"])
        orders = f.ents[act["reads"][0]]
        self.assertEqual((orders["endpoint"]["server"], orders["endpoint"]["database"]),
                         ("prod.sql.example", "SalesDW"))
        self.assertFalse(f.tables("dbo.Sales"), "the dataset's own table was not read")


class Triggers(unittest.TestCase):
    def tumbling(self, target):
        return ("trigger", "Hourly", {"type": "TumblingWindowTrigger", "runtimeState": "Started",
                "pipeline": {"pipelineReference": {"referenceName": target},
                             "parameters": {"windowStart": "@trigger().outputs.windowStartTime"}},
                "typeProperties": {"frequency": "Hour", "interval": 1,
                                   "startTime": "2026-01-01T00:00:00Z"}})

    def test_tumbling_window_links_its_pipeline(self):
        f = Factory(pipeline("PL", {"name": "W", "type": "Wait"}), self.tumbling("pl"))
        trig = f.payload["triggers"][0]
        self.assertEqual(trig["startsPipelines"], ["PL"])
        self.assertEqual(f.pipes["PL"]["triggers"], ["Hourly"])
        self.assertFalse(f.pipes["PL"]["entryPoint"])
        self.assertIn("windowStart", json.dumps(trig["parameters"]))

    def test_missing_trigger_target_is_unresolved(self):
        f = Factory(pipeline("PL", {"name": "W", "type": "Wait"}), self.tumbling("Gone"))
        self.assertIn(("pipeline", "Gone", "MISSING"),
                      {(r["kind"], r["name"], r["status"]) for r in f.payload["resolution"]})


class Coverage(unittest.TestCase):
    def test_skipped_file_prevents_a_completeness_claim(self):
        f = Factory(pipeline("PL", {"name": "W", "type": "Wait"}),
                    extra_files={"pipeline/broken.json": "{ not json"})
        self.assertNotEqual(f.payload["mode"], "factory")
        self.assertEqual(f.payload["coverage"]["skippedFiles"], 1)

    def test_missing_data_flow_is_an_unknown_footprint(self):
        f = Factory(pipeline("PL", {"name": "Run", "type": "ExecuteDataFlow", "typeProperties": {
            "dataflow": {"referenceName": "Gone", "type": "DataFlowReference"}}}))
        self.assertIn("data flow Gone", f.pipes["PL"]["effectiveIncomplete"])
        self.assertTrue(f.activity("PL", "Run").get("footprintUnknown"))
        self.assertEqual(f.payload["mode"], "partial")

    def test_web_activity_counts_as_opaque(self):
        f = Factory(pipeline("PL", {"name": "Call", "type": "WebActivity", "typeProperties": {
            "method": "POST", "url": "https://api.example.com/go"}}))
        self.assertEqual(f.pipes["PL"]["opaqueCount"], 1)
        self.assertTrue(any(w["category"] == "opaque" for w in f.payload["warnings"]))


def mapping_flow(name, src_ds, sink_ds):
    return ("dataflow", name, {"type": "MappingDataFlow", "typeProperties": {
        "sources": [{"name": "src", "dataset": {"referenceName": src_ds}}],
        "sinks": [{"name": "snk", "dataset": {"referenceName": sink_ds}}],
        "scriptLines": ["source(allowSchemaDrift: true) ~> src", "src sink() ~> snk"]}})


class SharedDataFlow(unittest.TestCase):
    def test_every_caller_is_attributed(self):
        run = lambda n: {"name": n, "type": "ExecuteDataFlow", "typeProperties": {
            "dataflow": {"referenceName": "DF", "type": "DataFlowReference"}}}
        f = Factory(
            sql_ls("Prod", "prod.sql.example", "SalesDW"),
            table_ds("A", "Prod", "dbo", "A"), table_ds("B", "Prod", "dbo", "B"),
            mapping_flow("DF", "A", "B"),
            pipeline("P1", run("R1")), pipeline("P2", run("R2")),
        )
        callers = {e["pipeline"] for e in f.payload["lineageEdges"] if e["mechanism"] == "DataFlow:DF"}
        self.assertEqual(callers, {"P1", "P2"})
        src = f.tables("dbo.A")[0]
        self.assertTrue({"P1::R1", "P2::R2"} <= set(src["referencedBy"]))


class ParameterisedDatasets(unittest.TestCase):
    def test_literal_bindings_stay_distinct(self):
        f = Factory(
            sql_ls("Prod", "prod.sql.example", "SalesDW"),
            ("dataset", "Src", {"type": "AzureSqlTable", "linkedServiceName": {"referenceName": "Prod"},
             "parameters": {"t": {"type": "string"}},
             "typeProperties": {"schema": "dbo", "table": {"value": "@dataset().t", "type": "Expression"}}}),
            table_ds("Dst", "Prod", "stg", "Land"),
            pipeline("PL",
                     copy("C1", "Src", "Dst", src_params={"t": "Orders"}),
                     copy("C2", "Src", "Dst", src_params={"t": "Customers"}),
                     copy("C3", "Src", "Dst", src_params={
                         "t": {"value": "@pipeline().parameters.x", "type": "Expression"}})),
        )
        self.assertEqual(len(f.tables("dbo.Orders")), 1)
        self.assertEqual(len(f.tables("dbo.Customers")), 1)
        by_act = {e["activity"]: e for e in f.payload["lineageEdges"]}
        self.assertFalse(by_act["C1"]["dynamic"])
        self.assertTrue(by_act["C3"]["dynamic"])


class SqlReader(unittest.TestCase):
    def test_statements(self):
        from adfdocgen.sql_harvest import harvest_sql as h
        self.assertEqual(h("WITH r AS (SELECT * FROM dbo.A) SELECT * FROM r JOIN dbo.B b ON 1=1"),
                         (["dbo.A", "dbo.B"], [], []))
        self.assertEqual(h("SELECT * INTO #t FROM s.A; INSERT INTO dbo.T SELECT * FROM #t"),
                         (["s.A"], ["dbo.T"], []))
        self.assertEqual(h("SELECT EXTRACT(YEAR FROM d) FROM s.o"), (["s.o"], [], []))
        self.assertEqual(h("MERGE dw.D t USING (SELECT * FROM stg.D) s ON 1=1 "
                           "WHEN MATCHED THEN UPDATE SET a=1;"), (["stg.D"], ["dw.D"], []))
        self.assertEqual(h('SELECT * FROM "PUBLIC"."ORDERS" WHERE x = \'FROM fake\''),
                         (["PUBLIC.ORDERS"], [], []))
        self.assertEqual(h("UPDATE t SET a=1 FROM dbo.T t; EXEC dbo.usp_X"),
                         (["dbo.T"], ["dbo.T"], ["dbo.usp_X"]))


if __name__ == "__main__":
    unittest.main()
