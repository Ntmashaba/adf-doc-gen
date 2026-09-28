"""The wheel installs a working CLI with its HTML/CSS/JS templates.

Builds offline in a throwaway virtual environment (its bundled pip and setuptools,
no build isolation), installs the wheel there, and runs the console script from
outside the repository.
"""
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = "adfdocgen"


class PackagingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        tmp = Path(cls.tmp.name)
        subprocess.run([sys.executable, "-m", "venv", str(tmp / "venv")], check=True, capture_output=True)
        bin_dir = tmp / "venv" / ("Scripts" if os.name == "nt" else "bin")
        cls.python = next(p for p in bin_dir.iterdir() if p.stem == "python")
        cls.script = bin_dir / ("adf-doc-gen.exe" if os.name == "nt" else "adf-doc-gen")
        subprocess.run([str(cls.python), "-m", "pip", "wheel", "--no-deps", "--no-build-isolation",
                        "-w", str(tmp / "dist"), str(ROOT)], check=True, capture_output=True)
        cls.wheel = next((tmp / "dist").glob("*.whl"))
        subprocess.run([str(cls.python), "-m", "pip", "install", "--no-deps", str(cls.wheel)],
                       check=True, capture_output=True)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_wheel_contains_cli_and_assets(self):
        names = set(zipfile.ZipFile(self.wheel).namelist())
        self.assertIn("generate_docs.py", names)
        for asset in (ROOT / PKG).iterdir():
            if asset.suffix in (".html", ".css", ".js"):
                self.assertIn(f"{PKG}/{asset.name}", names)

    def _run(self, cwd, *args):
        return subprocess.run([str(self.script), *args], cwd=cwd, capture_output=True, text=True)

    def test_console_script_runs_outside_repository(self):
        with tempfile.TemporaryDirectory() as cwd:
            out = self._run(cwd, "--help")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("usage", out.stdout.lower())

    def test_installed_cli_generates_html(self):
        with tempfile.TemporaryDirectory() as cwd:
            out = self._run(cwd, str(ROOT / "examples" / "contoso-sales-etl"), "-o", str(Path(cwd) / "f.html"))
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertIn("const DATA = ", (Path(cwd) / "f.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
