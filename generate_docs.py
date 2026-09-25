#!/usr/bin/env python3
"""adf-doc-gen — generate living documentation from Azure Data Factory JSON.

Usage:
    python generate_docs.py <input> [-o out.html] [--title "..."] [--json] [--word]
                            [--agent] [--csv [FOLDER]] [--details details.json]
    python generate_docs.py --batch <folder> [--output-dir FOLDER] [--json] [--csv]
    python generate_docs.py --hub <folder of generated HTML>

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
import csv
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from adfdocgen.loader import collect_inputs
from adfdocgen.analyzer import analyze
from adfdocgen.renderer import build_payload, render_html
from adfdocgen.details import validate_details


def write_csv(payload: dict, folder: Path) -> list:
    """Evidence inventories as CSV: objects, usage, movement edges, issues."""
    folder.mkdir(parents=True, exist_ok=True)
    label = {e["key"]: e["label"] for e in payload["entities"]}
    tables = {
        "objects.csv": (["key", "kind", "name", "system", "server", "database", "container", "schema",
                         "object", "path", "dynamic", "datasets", "linked services", "pipelines", "triggers"],
                        [[e["key"], e["kind"], e["label"], e["endpoint"].get("system"),
                          e["endpoint"].get("server") or e["endpoint"].get("url"),
                          e["endpoint"].get("database"), e["endpoint"].get("container"),
                          e["endpoint"].get("schema"), e["endpoint"].get("object"), e["endpoint"].get("path"),
                          e["dynamic"], "; ".join(e["aliases"]), "; ".join(e.get("linkedServices", [])),
                          "; ".join(e.get("pipelines", [])), "; ".join(e.get("triggers", []))]
                         for e in payload["entities"]]),
        "usage.csv": (["object key", "object", "operation", "pipeline", "activity", "activity type",
                       "dynamic", "opaque"],
                      [[e["key"], e["label"], u["operation"], u["pipeline"], u["activity"], u["type"],
                        u["dynamic"], u["opaque"]]
                       for e in payload["entities"] for u in e.get("usage", [])]),
        "edges.csv": (["sources", "sinks", "mechanism", "pipeline", "activity", "dynamic", "opaque", "detail"],
                      [["; ".join(label.get(k, k) for k in e["sources"]),
                        "; ".join(label.get(k, k) for k in e["sinks"]), e["mechanism"], e["pipeline"],
                        e["activity"], e["dynamic"], e["opaque"], e["detail"]]
                       for e in payload["lineageEdges"]]),
        "issues.csv": (["priority", "category", "resource", "finding", "why it matters", "next action"],
                       [[i["priority"], i["category"], i["resource"], i["message"], i["why"], i["action"]]
                        for i in payload.get("issues", [])]),
    }
    written = []
    for name, (head, rows) in tables.items():
        path = folder / name
        # utf-8-sig so Excel opens names with accents correctly
        with open(path, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(head)
            w.writerows(rows)
        written.append(path)
    return written


def document(in_path: Path, out_html: Path, title: str, *, json_out=False, word=False,
             agent=False, details=None, csv_dir=None, quiet=False) -> dict:
    """Generate every requested output for one factory input. Returns the payload."""
    say = (lambda *a, **k: None) if quiet else print
    say(f"Reading {in_path} …")
    store = collect_inputs(str(in_path))
    counts = {k: len(v) for k, v in store.items()
              if not k.startswith("__") and k != "unknown" and v}
    if store.get("unknown"):
        say(f"  ? {len(store['unknown'])} file(s) could not be classified: "
            + ", ".join(sorted(store["unknown"])), file=sys.stderr)
    if not store["pipeline"] and not store["dataflow"]:
        raise ValueError("No pipelines or data flows found in the input. Expected ADF resource "
                         "JSON (Git format / ADF Studio 'view code') or an ARM template export.")
    say("  found: " + ", ".join(f"{n} {k}(s)" for k, n in sorted(counts.items())))

    say("Analyzing …")
    payload = build_payload(analyze(store), title)
    if details is None and out_html.exists():
        from adfdocgen.details import read_details
        details = read_details(out_html.read_text(encoding="utf-8-sig"))
    if details is not None:
        payload["details"] = details
    f = payload["factory"]
    say(f"  mode: {payload['mode']}"
        + (f"  ({f['unresolvedCount']} unresolved reference(s))" if f["unresolvedCount"] else "")
        + f" · {f['entityCount']} entities · {f['lineageEdgeCount']} movement edges"
        + f" · {len(payload['issues'])} review issue(s)")

    render_html(payload, out_html)
    say(f"  wrote {out_html}")
    if json_out:
        jpath = out_html.with_suffix(".json")
        jpath.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        say(f"  wrote {jpath}")
    if word:
        from adfdocgen.word_writer import render_docx
        dpath = out_html.with_suffix(".docx")
        render_docx(payload, dpath)
        say(f"  wrote {dpath}")
    if agent:
        from adfdocgen.agent_writer import render_agent_md, estimate_tokens
        apath = out_html.with_suffix(".agent.md")
        render_agent_md(payload, apath)
        say(f"  wrote {apath}  (~{estimate_tokens(apath.read_text(encoding='utf-8')):,} tokens)")
    if csv_dir:
        for path in write_csv(payload, Path(csv_dir)):
            say(f"  wrote {path}")
    return payload


def batch_inputs(folder: Path) -> list:
    """Each subfolder is one factory; so is each ARM template file at the top level."""
    found = []
    for child in sorted(folder.iterdir(), key=lambda p: p.name.casefold()):
        if child.name.startswith(".") or child.name in ("documentation",):
            continue
        if child.is_dir():
            found.append(child)
        elif child.suffix.lower() == ".json" and "parameters" not in child.name.lower():
            found.append(child)
    return found


def output_names(inputs: list) -> dict:
    """File names that never collide, even for folders whose names differ only
    in punctuation or case (Sales ETL, sales-etl)."""
    base = {p: re.sub(r"[^\w.-]+", "_", p.stem if p.is_file() else p.name).strip("_") or "factory"
            for p in inputs}
    seen = {}
    for p, b in base.items():
        seen.setdefault(b.lower(), []).append(p)
    names = {}
    for p, b in base.items():
        clash = len(seen[b.lower()]) > 1
        tag = "--" + hashlib.sha1(str(p.resolve()).encode()).hexdigest()[:8] if clash else ""
        names[p] = f"{b}{tag}.html"
    return names


def run_batch(args) -> int:
    from adfdocgen.hub import BATCH_RESULTS, build_hub
    folder = Path(args.batch)
    if not folder.is_dir():
        print(f"Batch folder not found: {folder}", file=sys.stderr)
        return 2
    out_dir = Path(args.output_dir) if args.output_dir else folder / "documentation"
    out_dir.mkdir(parents=True, exist_ok=True)
    inputs = batch_inputs(folder)
    names = output_names(inputs)
    results = {"started": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
               "folder": folder.name, "factories": []}
    for p in inputs:
        out = out_dir / names[p]
        title = (p.stem if p.is_file() else p.name).replace("_", " ").replace("-", " ").strip()
        try:
            payload = document(p, out, title, json_out=args.json, word=args.word, agent=args.agent,
                               csv_dir=(out_dir / (out.stem + "-csv")) if args.csv is not None else None,
                               quiet=True)
            results["factories"].append({"input": p.name, "status": "ok", "output": out.name,
                                         "mode": payload["mode"]})
            print(f"  ok      {p.name} → {out.name} ({payload['mode']})")
        except Exception as exc:        # noqa: BLE001 - one bad factory must not stop the batch
            results["factories"].append({"input": p.name, "status": "failed",
                                         "error": f"{type(exc).__name__}: {exc}"})
            print(f"  FAILED  {p.name}: {exc}", file=sys.stderr)
    (out_dir / BATCH_RESULTS).write_text(json.dumps(results, indent=2), encoding="utf-8")
    hub = build_hub(out_dir)
    failed = sum(1 for r in results["factories"] if r["status"] != "ok")
    print(f"{len(inputs) - failed} documented, {failed} failed. Home: {hub}")
    return 1 if failed else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="generate_docs.py",
        description="Generate interactive documentation from Azure Data Factory JSON.")
    ap.add_argument("input", nargs="?", help="resource JSON, ARM template, or folder")
    ap.add_argument("-o", "--output", default=None,
                    help="output HTML path (default: <input-name>_docs.html)")
    ap.add_argument("--title", default=None,
                    help="document title (default: derived from the input name)")
    ap.add_argument("--json", action="store_true",
                    help="also write the consolidated JSON payload next to the HTML")
    ap.add_argument("--word", action="store_true",
                    help="also write a narrative Word (.docx) document next to the HTML")
    ap.add_argument("--agent", action="store_true",
                    help="also write an agent context document (.agent.md) next to the HTML")
    ap.add_argument("--csv", nargs="?", const="", default=None, metavar="FOLDER",
                    help="also write objects/usage/edges/issues CSV files "
                         "(default folder: <output>-csv)")
    ap.add_argument("--details", metavar="JSON",
                    help="factory details to embed (environment, owner, factory, sourceLocation, "
                         "runbook, folder, notes); otherwise details already in the output file are kept")
    ap.add_argument("--batch", metavar="FOLDER",
                    help="document every factory in a folder (each subfolder or ARM template) "
                         "and build the documentation home")
    ap.add_argument("--output-dir", metavar="FOLDER",
                    help="folder for the outputs; with --batch the default is <FOLDER>/documentation")
    ap.add_argument("--hub", metavar="FOLDER",
                    help="rebuild the documentation home (adf-home.html) from the HTML files in a folder")
    ap.add_argument("--bridge", nargs=2, metavar=("ADF_DOCS", "PBI_DOCS"),
                    help="match Power BI sources (pbi-doc-gen HTML/JSON) to the Data Factory "
                         "pipelines that write them (adf-doc-gen HTML/JSON); writes -o or "
                         "powerbi-adf-bridge.html")
    args = ap.parse_args(argv)

    if args.bridge:
        from adfdocgen.bridge import build_bridge
        out = build_bridge(args.bridge[0], args.bridge[1], args.output or "powerbi-adf-bridge.html")
        print(f"wrote {out}")
        return 0
    if args.hub:
        from adfdocgen.hub import build_hub
        print(f"wrote {build_hub(args.hub)}")
        return 0
    if args.batch:
        return run_batch(args)
    if not args.input:
        ap.error("an input is required (or use --batch / --hub)")

    in_path = Path(args.input)
    if not in_path.exists():
        ap.error(f"input not found: {in_path}")
    stem = in_path.stem if in_path.is_file() else in_path.name
    title = args.title or stem.replace("_", " ").replace("-", " ").strip() or "Data Factory"
    out_html = Path(args.output) if args.output else Path(f"{stem}_docs.html")
    if args.output_dir:
        # --output-dir places the document (and its companions) in that folder;
        # a relative -o name is kept, inside it.
        if out_html.is_absolute():
            ap.error("use either an absolute -o path or --output-dir, not both")
        out_html = Path(args.output_dir) / out_html
    out_html.parent.mkdir(parents=True, exist_ok=True)
    details = None
    if args.details:
        try:
            details = validate_details(json.loads(Path(args.details).read_text(encoding="utf-8-sig")))
        except (OSError, ValueError) as exc:
            ap.error(f"--details: {exc}")
    csv_dir = None
    if args.csv is not None:
        csv_dir = Path(args.csv) if args.csv else out_html.with_name(out_html.stem + "-csv")
    try:
        document(in_path, out_html, title, json_out=args.json, word=args.word, agent=args.agent,
                 details=details, csv_dir=csv_dir)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
