"""Endpoints stay distinct unless proven equal (SQL port, letter case, path case),
and deleting an object never makes a pipeline its producer."""
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import generate_docs                     # noqa: E402


def ls(name, typ, tp):
    return {"name": name, "properties": {"type": typ, "typeProperties": tp}}


def ds(name, typ, lsn, tp):
    return {"name": name, "properties": {"type": typ, "typeProperties": tp,
                                         "linkedServiceName": {"referenceName": lsn, "type": "LinkedServiceReference"}}}


def ref(n):
    return {"referenceName": n, "type": "DatasetReference"}


def lake(name, folder):
    return ds(name, "Parquet", "LS_Lake", {"location": {"type": "AzureBlobFSLocation", "fileSystem": "raw",
                                                         "folderPath": folder}})


FACTORY = {
    "linkedService": [
        ls("LS_Default", "AzureSqlDatabase", {"connectionString": "Server=tcp:sql1.corp.local;Database=DW;"}),
        ls("LS_1433", "AzureSqlDatabase", {"connectionString": "Server=tcp:sql1.corp.local,1433;Database=DW;"}),
        ls("LS_1444", "AzureSqlDatabase", {"connectionString": "Server=tcp:sql1.corp.local,1444;Database=DW;"}),
        ls("LS_Lake", "AzureBlobFS", {"url": "https://lake.dfs.core.windows.net"}),
    ],
    "dataset": [
        ds("DS_Default", "AzureSqlTable", "LS_Default", {"schema": "dbo", "table": "Sales"}),
        ds("DS_1433", "AzureSqlTable", "LS_1433", {"schema": "dbo", "table": "Sales"}),
        ds("DS_1444", "AzureSqlTable", "LS_1444", {"schema": "dbo", "table": "Sales"}),
        ds("DS_Upper", "AzureSqlTable", "LS_1433", {"schema": "dbo", "table": "SALES"}),
        lake("DS_Daily", "Sales/Daily"), lake("DS_LowerCase", "sales/daily"), lake("DS_Purged", "archive/old"),
    ],
    "pipeline": [{"name": "PL", "properties": {"activities": [
        {"name": "c1", "type": "Copy", "inputs": [ref("DS_Daily")], "outputs": [ref("DS_Default")],
         "typeProperties": {"source": {"type": "ParquetSource"}, "sink": {"type": "AzureSqlSink"}}},
        {"name": "c2", "type": "Copy", "inputs": [ref("DS_LowerCase")], "outputs": [ref("DS_1433")],
         "typeProperties": {"source": {"type": "ParquetSource"}, "sink": {"type": "AzureSqlSink"}}},
        {"name": "c3", "type": "Copy", "inputs": [ref("DS_Daily")], "outputs": [ref("DS_1444")],
         "typeProperties": {"source": {"type": "ParquetSource"}, "sink": {"type": "AzureSqlSink"}}},
        {"name": "c4", "type": "Copy", "inputs": [ref("DS_Daily")], "outputs": [ref("DS_Upper")],
         "typeProperties": {"source": {"type": "ParquetSource"}, "sink": {"type": "AzureSqlSink"}}},
        {"name": "purge", "type": "Delete", "typeProperties": {"dataset": ref("DS_Purged")}},
    ]}}],
}


def quiet(argv):
    with open(os.devnull, "w") as null:
        out, sys.stdout = sys.stdout, null
        try:
            return generate_docs.main(argv)
        finally:
            sys.stdout = out


class EndpointIdentity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        root = cls.tmp / "factory"
        for kind, items in FACTORY.items():
            (root / kind).mkdir(parents=True)
            for it in items:
                (root / kind / f"{it['name']}.json").write_text(json.dumps(it), encoding="utf-8")
        (cls.tmp / "adf").mkdir()
        cls.html = cls.tmp / "adf" / "f.html"
        quiet([str(root), "-o", str(cls.html), "--json"])
        cls.p = json.loads(cls.html.with_suffix(".json").read_text(encoding="utf-8"))
        cls.keys = {e["key"] for e in cls.p["entities"]}

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp)

    def test_default_port_is_the_same_server_other_port_is_not(self):
        self.assertIn("table:sql1.corp.local/DW/dbo.Sales", self.keys)          # no port and 1433
        self.assertIn("table:sql1.corp.local:1444/DW/dbo.Sales", self.keys)
        self.assertNotIn("table:sql1.corp.local:1433/DW/dbo.Sales", self.keys)

    def test_sql_case_is_not_merged(self):
        self.assertIn("table:sql1.corp.local/DW/dbo.SALES", self.keys)

    def test_path_case_is_kept(self):
        self.assertIn("file_path:lake.dfs.core.windows.net/raw/Sales/Daily", self.keys)
        self.assertIn("file_path:lake.dfs.core.windows.net/raw/sales/daily", self.keys)

    def test_embedded_data_cannot_spell_markup(self):
        text = self.html.read_text(encoding="utf-8")
        blob = re.search(r"const DATA = (.*?);\n", text).group(1)
        self.assertNotIn("<", blob)
        self.assertEqual(json.loads(blob)["title"], self.p["title"])

    def test_delete_is_not_a_producer_in_the_bridge(self):
        pbi = self.tmp / "pbi"
        pbi.mkdir(exist_ok=True)
        (pbi / "r.json").write_text(json.dumps({"schemaVersion": 1, "title": "R", "sourceObjects": [
            {"report": "R", "table": "Old", "sourceType": "Azure Blob Storage", "status": "Resolved",
             "server": "https://lake.blob.core.windows.net/raw/archive/old", "database": "", "schema": "",
             "object": "", "sql": ""}]}), encoding="utf-8")
        out = self.tmp / "bridge.html"
        quiet(["--bridge", str(self.tmp / "adf"), str(pbi), "-o", str(out)])
        row = json.loads(re.search(r"const BRIDGE = (.*);\n", out.read_text(encoding="utf-8")).group(1))["rows"][0]
        self.assertEqual(row["producers"], [])
        self.assertEqual(row["deletedBy"][0]["activity"], "purge")


if __name__ == "__main__":
    unittest.main()
