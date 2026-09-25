"""The checked-in example factory documents the way the README says it does."""
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from generate_docs import main  # noqa: E402

EXAMPLE = os.path.join(ROOT, "examples", "contoso-sales-etl")


class ContosoExample(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.html = os.path.join(cls.tmp.name, "contoso.html")
        with open(os.devnull, "w") as quiet:
            stdout, sys.stdout = sys.stdout, quiet
            try:
                code = main([EXAMPLE, "-o", cls.html, "--title", "Contoso Sales ETL",
                             "--json", "--word", "--agent"])
            finally:
                sys.stdout = stdout
        assert code == 0
        with open(cls.html[:-5] + ".json", encoding="utf-8") as fh:
            cls.p = json.load(fh)

    def test_complete_factory(self):
        self.assertEqual(self.p["mode"], "factory")
        self.assertEqual(self.p["coverage"]["skippedFiles"], 0)

    def test_no_example_secret_in_any_output(self):
        for ext in (".html", ".json", ".agent.md", ".docx"):
            with open(self.html[:-5] + ext, "rb") as fh:
                data = fh.read()
            self.assertNotIn(b"example-not-a-real", data, ext)

    def test_fact_tables_on_two_servers(self):
        facts = [e for e in self.p["entities"] if e["label"] == "dbo.FactSales"]
        self.assertEqual(len(facts), 2)

    def test_query_tables_and_shared_flow(self):
        edges = {(e["pipeline"], e["activity"]): e for e in self.p["lineageEdges"]}
        land = edges[("PL_Ingest_Sales", "Land orders")]
        self.assertEqual(len(land["sources"]), 2)          # sales.Orders + ref.Store
        self.assertFalse(land["dynamic"])                  # only a filter value is dynamic
        self.assertIn(("PL_Rerun_Enrichment", "Rerun enrich"), edges)
        self.assertIn(("PL_Transform_Sales", "Enrich orders"), edges)

    def test_triggers(self):
        t = {x["name"]: x for x in self.p["triggers"]}
        self.assertEqual(t["TR_Hourly_Window"]["startsPipelines"], ["PL_Ingest_Sales"])
        self.assertEqual(t["TR_Daily_0600"]["parameters"]["PL_Master_Daily"], {"target": "OrdersSync"})


if __name__ == "__main__":
    unittest.main()
