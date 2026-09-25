"""Batch generation, the documentation home, factory details and CSV output."""
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import generate_docs                                   # noqa: E402
from adfdocgen.details import read_details, validate_details   # noqa: E402
from adfdocgen.hub import build_hub, describe          # noqa: E402

EXAMPLE = ROOT / "examples" / "contoso-sales-etl"


def quiet(fn, *a, **k):
    with open(os.devnull, "w") as null:
        out, err, sys.stdout, sys.stderr = sys.stdout, sys.stderr, null, null
        try:
            return fn(*a, **k)
        finally:
            sys.stdout, sys.stderr = out, err


class Batch(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        src = self.tmp / "factories"
        shutil.copytree(EXAMPLE, src / "Sales ETL")
        shutil.copytree(EXAMPLE, src / "sales-etl")        # same safe name: must not collide
        (src / "broken").mkdir()
        (src / "broken" / "x.json").write_text('{"name": "ds", "properties": {"type": "X"}}')
        self.src = src

    def test_batch_documents_each_factory_and_reports_failures(self):
        code = quiet(generate_docs.main, ["--batch", str(self.src), "--csv"])
        self.assertEqual(code, 1)                           # one factory failed
        out = self.src / "documentation"
        results = json.loads((out / "adf-batch-results.json").read_text())
        status = {r["input"]: r["status"] for r in results["factories"]}
        self.assertEqual(status, {"Sales ETL": "ok", "sales-etl": "ok", "broken": "failed"})
        htmls = sorted(p.name for p in out.glob("*.html") if p.name != "adf-home.html")
        self.assertEqual(len(htmls), 2)                     # distinct names despite the clash
        home = (out / "adf-home.html").read_text(encoding="utf-8")
        self.assertIn('name="adf-documentation-hub"', home)
        self.assertIn("contoso-sales.database.windows.net", home)
        self.assertTrue(any(out.glob("*-csv/objects.csv")))

    def test_hub_refuses_to_replace_other_files(self):
        (self.tmp / "adf-home.html").write_text("<html>mine</html>")
        with self.assertRaises(ValueError):
            build_hub(self.tmp)


class Details(unittest.TestCase):
    def test_details_survive_regeneration_and_reject_secrets(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp)
        details = tmp / "d.json"
        details.write_text(json.dumps({"owner": "Data Platform", "environment": "Production",
                                       "folder": "Sales"}))
        html = tmp / "doc.html"
        self.assertEqual(quiet(generate_docs.main, [str(EXAMPLE), "-o", str(html),
                                                    "--details", str(details)]), 0)
        # Regenerate without --details: the file's own details are kept.
        self.assertEqual(quiet(generate_docs.main, [str(EXAMPLE), "-o", str(html)]), 0)
        kept = read_details(html.read_text(encoding="utf-8"))
        self.assertEqual((kept["owner"], kept["folder"]), ("Data Platform", "Sales"))
        self.assertEqual(describe(html)["details"]["environment"], "Production")
        with self.assertRaises(ValueError):
            validate_details({"notes": "Server=x;Password=hunter2"})
        with self.assertRaises(ValueError):
            validate_details({"password": "x"})

    def test_csv_inventories(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp)
        quiet(generate_docs.main, [str(EXAMPLE), "-o", str(tmp / "doc.html"), "--csv"])
        csv_dir = tmp / "doc-csv"
        self.assertEqual(sorted(p.name for p in csv_dir.iterdir()),
                         ["edges.csv", "issues.csv", "objects.csv", "usage.csv"])
        usage = (csv_dir / "usage.csv").read_text(encoding="utf-8-sig")
        self.assertIn("PL_Transform_Sales", usage)


if __name__ == "__main__":
    unittest.main()
