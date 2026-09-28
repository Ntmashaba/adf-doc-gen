"""The library viewer bridge (bi-doc-viewer protocol v1) ships in every page."""
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import generate_docs  # noqa: E402


class ViewerBridge(unittest.TestCase):
    def test_registered_views_activity_ids_and_parent_only(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "f.html"
            with open(os.devnull, "w") as null:
                stdout, sys.stdout = sys.stdout, null
                try:
                    generate_docs.main([str(ROOT / "examples" / "contoso-sales-etl"), "-o", str(out)])
                finally:
                    sys.stdout = stdout
            text = out.read_text(encoding="utf-8")
        views = re.search(r"const VIEWER_VIEWS = \{(.*?)\n\};", text, re.S).group(1)
        self.assertEqual(sorted(re.findall(r'"(adf\.[a-z]+)"', views)),
                         ["adf.activity", "adf.dataflow", "adf.overview", "adf.pipeline", "adf.trigger"])
        self.assertIn('id="act-${slug(pipeline+"/"+a.activity)}"', text)
        self.assertIn("if(ev.source!==window.parent) return;", text)
        self.assertIn("if(window.parent===window", text)


if __name__ == "__main__":
    unittest.main()
