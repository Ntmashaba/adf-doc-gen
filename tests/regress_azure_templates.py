"""Run every template in Microsoft's Azure-DataFactory repository through the tool.

This is a dev check, not a unit test: it needs a local clone.

    git clone https://github.com/Azure/Azure-DataFactory
    git -C Azure-DataFactory checkout ce4c9cab41e5e4fa66f4bddd042cab649c36a4ca
    python tests/regress_azure_templates.py Azure-DataFactory [--out folder]

It fails (exit 1) if any template crashes the loader, analysis or a renderer,
and prints the mode counts and warning categories so changes are easy to see.
Pinning the commit keeps the numbers comparable between runs.
"""
import argparse
import collections
import contextlib
import glob
import io
import os
import sys
import tempfile
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adfdocgen.loader import collect_inputs            # noqa: E402
from adfdocgen.analyzer import analyze                 # noqa: E402
from adfdocgen.renderer import build_payload, render_html  # noqa: E402
from adfdocgen.word_writer import render_docx          # noqa: E402
from adfdocgen.agent_writer import render_agent_md     # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", help="path to a clone of Azure/Azure-DataFactory")
    ap.add_argument("--out", help="keep the generated HTML/Word/agent files here")
    args = ap.parse_args()
    out = args.out or tempfile.mkdtemp()
    os.makedirs(out, exist_ok=True)

    templates = sorted(glob.glob(os.path.join(args.repo, "templates", "*", "")))
    ok, errors = 0, []
    modes, cats = collections.Counter(), collections.Counter()
    for folder in templates:
        name = os.path.basename(os.path.normpath(folder))
        files = [f for f in glob.glob(os.path.join(folder, "*.json"))
                 if not f.endswith("manifest.json")]
        if not files:
            continue
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                payload = build_payload(analyze(collect_inputs(files[0])), name)
            safe = "".join(c if c.isalnum() or c in " -_" else "_" for c in name)
            render_html(payload, os.path.join(out, safe + ".html"))
            render_docx(payload, os.path.join(out, safe + ".docx"))
            render_agent_md(payload, os.path.join(out, safe + ".agent.md"))
            ok += 1
            modes[payload["mode"]] += 1
            for w in payload["warnings"]:
                cats[(w["severity"], w["category"])] += 1
        except Exception:                                  # noqa: BLE001
            errors.append((name, traceback.format_exc(limit=3)))

    print(f"{ok} templates documented, {len(errors)} failed ({len(templates)} folders)")
    print("modes:", dict(modes))
    for (sev, cat), n in cats.most_common():
        print(f"  {n:4} {sev:8} {cat}")
    for name, tb in errors:
        print(f"\n--- {name}\n{tb}")
    if not args.out:
        print(f"outputs in {out}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
