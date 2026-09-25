"""Power BI sources matched to the Data Factory pipelines that write them."""
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

EXAMPLE = ROOT / "examples" / "contoso-sales-etl"


def src(table, source_type, server, database="", schema="", obj="", sql=""):
    return {"report": "Sales report", "table": table, "sourceType": source_type, "server": server,
            "database": database, "schema": schema, "object": obj, "sql": sql, "status": "Resolved"}


class Bridge(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        (self.tmp / "adf").mkdir()
        (self.tmp / "pbi").mkdir()
        with open(os.devnull, "w") as null:
            out, sys.stdout = sys.stdout, null
            try:
                generate_docs.main([str(EXAMPLE), "-o", str(self.tmp / "adf" / "contoso.html")])
            finally:
                sys.stdout = out
        pbi = {"schemaVersion": 1, "title": "Sales report", "sourceObjects": [
            src("Fact archive", "SQL Server", "contoso-archive.database.windows.net", "ArchiveDW", "dbo", "FactSales"),
            src("Fact live", "SQL Server", "contoso-sales.database.windows.net", "SalesDW", "dbo", "FactSales"),
            src("Other server", "SQL Server", "elsewhere.example", "ArchiveDW", "dbo", "FactSales"),
            src("Enriched", "Azure Blob Storage",
                "https://contosolake.blob.core.windows.net/curated/sales/orders_enriched/part-0.parquet"),
            src("Watermark", "SQL Server", "contoso-sales.database.windows.net", "SalesDW",
                sql="SELECT * FROM etl.Watermark"),
            src("Calc", "Calculated (DAX)", ""),
        ]}
        (self.tmp / "pbi" / "report.json").write_text(json.dumps(pbi))
        out = self.tmp / "bridge.html"
        with open(os.devnull, "w") as null:
            o, sys.stdout = sys.stdout, null
            try:
                generate_docs.main(["--bridge", str(self.tmp / "adf"), str(self.tmp / "pbi"), "-o", str(out)])
            finally:
                sys.stdout = o
        text = out.read_text(encoding="utf-8")
        blob = re.search(r"const BRIDGE = (.*);\n", text).group(1)
        self.rows = {r["table"]: r for r in json.loads(blob)["rows"]}

    def test_levels(self):
        self.assertEqual(self.rows["Fact archive"]["match"], "exact")
        self.assertEqual(self.rows["Fact archive"]["producers"][0]["pipeline"], "PL_Transform_Sales")
        self.assertEqual(self.rows["Fact live"]["match"], "read only")
        self.assertEqual(self.rows["Other server"]["match"], "none")
        self.assertEqual(self.rows["Enriched"]["match"], "exact")
        self.assertEqual(self.rows["Watermark"]["match"], "read only")
        self.assertNotIn("Calc", self.rows)
        # producers carry the triggers that start them
        self.assertIn("TR_Daily_0600", self.rows["Fact archive"]["producers"][0]["triggers"])


if __name__ == "__main__":
    unittest.main()
