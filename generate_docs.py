#!/usr/bin/env python3
"""adf-doc-gen — generate living documentation from Azure Data Factory JSON.

Usage:
    python generate_docs.py <input> [-o out.html] [--title "..."] [--json] [--word]

<input> is any of:
    a single resource JSON        (pipeline / dataset / data flow / linked
                                   service / trigger, as exported from ADF
                                   Studio's "view code" or a Git repo)
    an ARM template export        (Manage > ARM template > Export)
    a folder                      (an ADF Git repo root, or a hand-curated
                                   folder of loose JSON files)

Outputs a single self-contained interactive HTML file. Optionally also the
consolidated JSON payload (--json) and a narrative Word document (--word).
Requires Python 3.8+ and nothing else — no pip installs.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from adfdocgen.loader import collect_inputs
from adfdocgen.analyzer import analyze
from adfdocgen.renderer import build_payload, render_html


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="generate_docs.py",
        description="Generate interactive documentation from Azure Data Factory JSON.")
    ap.add_argument("input", help="resource JSON, ARM template, or folder")
    ap.add_argument("-o", "--output", default=None,
                    help="output HTML path (default: <input-name>_docs.html)")
    ap.add_argument("--title", default=None,
                    help="document title (default: derived from the input name)")
    ap.add_argument("--json", action="store_true",
                    help="also write the consolidated JSON payload next to the HTML")
    ap.add_argument("--word", action="store_true",
                    help="also write a narrative Word (.docx) document next to the HTML")
    ap.add_argument("--agent", action="store_true",
                    help="also write an agent context document (.agent.md) next to the "
                         "HTML — the compact markdown distillation for LLM agents, "
                         "successor to adf_distill.py's output")
    args = ap.parse_args(argv)

    in_path = Path(args.input)
    if not in_path.exists():
        ap.error(f"input not found: {in_path}")

    stem = in_path.stem if in_path.is_file() else in_path.name
    title = args.title or stem.replace("_", " ").replace("-", " ").strip() or "Data Factory"
    out_html = Path(args.output) if args.output else Path(f"{stem}_docs.html")

    print(f"Reading {in_path} …")
    store = collect_inputs(str(in_path))
    counts = {k: len(v) for k, v in store.items()
              if not k.startswith("__") and k != "unknown" and v}
    if store.get("unknown"):
        print(f"  ? {len(store['unknown'])} file(s) could not be classified: "
              + ", ".join(sorted(store["unknown"])), file=sys.stderr)
    if not store["pipeline"] and not store["dataflow"]:
        print("No pipelines or data flows found in the input — nothing to document.\n"
              "Expected ADF resource JSON (Git format / ADF Studio 'view code') or an "
              "ARM template export.", file=sys.stderr)
        return 2
    print("  found: " + ", ".join(f"{n} {k}(s)" for k, n in sorted(counts.items())))

    print("Analyzing …")
    analysis = analyze(store)
    payload = build_payload(analysis, title)
    f = payload["factory"]
    print(f"  mode: {payload['mode']}"
          + (f"  ({f['unresolvedCount']} unresolved reference(s))"
             if f["unresolvedCount"] else "")
          + f" · {f['entityCount']} entities · {f['lineageEdgeCount']} movement edges"
          + f" · {len(payload['warnings'])} warnings")

    render_html(payload, out_html)
    print(f"  wrote {out_html}")

    if args.json:
        import json as _json
        jpath = out_html.with_suffix(".json")
        jpath.write_text(_json.dumps(payload, indent=2, ensure_ascii=False),
                         encoding="utf-8")
        print(f"  wrote {jpath}")

    if args.word:
        from adfdocgen.word_writer import render_docx
        dpath = out_html.with_suffix(".docx")
        render_docx(payload, dpath)
        print(f"  wrote {dpath}")

    if args.agent:
        from adfdocgen.agent_writer import render_agent_md, estimate_tokens
        apath = out_html.with_suffix(".agent.md")
        render_agent_md(payload, apath)
        print(f"  wrote {apath}  (~{estimate_tokens(apath.read_text(encoding='utf-8')):,} tokens)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
