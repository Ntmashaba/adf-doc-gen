"""Render the consolidated analysis payload as a Word document (.docx).

Pure standard library — a .docx is a ZIP of XML parts, so this module writes
the OOXML directly (document, styles, content types, relationships, core
properties). No python-docx, no pip install, honoring the project's
zero-dependency promise.

The Word document is deliberately NOT the HTML flattened. Word is linear and
carries no search or cross-links, so this renderer produces the narrative
subset a stakeholder or handover pack needs: what the factory is, how work
starts and cascades, where data moves, what each pipeline does, and what the
analysis flagged — then a reference appendix (datasets, linked services,
triggers, data flows, entities). The HTML remains the working document.

OOXML conventions carried over from pbi-doc-gen (they encode real Word/
LibreOffice compatibility lessons): A4 with gutter=0; pPr child order
keepNext -> spacing -> outlineLvl; DXA widths on the grid AND every cell,
summing exactly; headings carry outlineLvl so navigation works; code blocks
are one shaded paragraph per line, never a newline inside a run.
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

# --------------------------------------------------------------------------
# Page geometry (A4, 2 cm margins) — all values in DXA (1440 = 1 inch)
# --------------------------------------------------------------------------
PAGE_W, PAGE_H, MARGIN = 11906, 16838, 1134
CONTENT_W = PAGE_W - 2 * MARGIN

ACCENT = "B45F2A"       # copper — the sink/write hue, shared with the HTML
ACCENT2 = "0F766E"      # teal — the source/read hue
GREY_HDR = "ECF1F0"     # table header shading
GREY_CODE = "F2F5F4"    # code block shading
MUTED = "6B6B6B"

_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _t(text) -> str:
    """Escape text for XML and strip control chars Word rejects."""
    return escape(_CTRL.sub("", str(text if text is not None else "")))


# --------------------------------------------------------------------------
# Low-level OOXML builders
# --------------------------------------------------------------------------

def run(text, bold=False, italic=False, color=None, size=None, font=None) -> str:
    props = []
    if font:
        props.append(f'<w:rFonts w:ascii="{font}" w:hAnsi="{font}" w:cs="{font}"/>')
    if bold:
        props.append("<w:b/>")
    if italic:
        props.append("<w:i/>")
    if color:
        props.append(f'<w:color w:val="{color}"/>')
    if size:
        props.append(f'<w:sz w:val="{size * 2}"/><w:szCs w:val="{size * 2}"/>')
    rpr = f"<w:rPr>{''.join(props)}</w:rPr>" if props else ""
    return f'<w:r>{rpr}<w:t xml:space="preserve">{_t(text)}</w:t></w:r>'


def para(runs_xml: str, style: str | None = None, shade: str | None = None,
         space_after: int | None = None, keep_next: bool = False) -> str:
    props = []
    if style:
        props.append(f'<w:pStyle w:val="{style}"/>')
    if keep_next:
        props.append("<w:keepNext/>")
    if shade:
        props.append(f'<w:shd w:val="clear" w:color="auto" w:fill="{shade}"/>')
    if space_after is not None:
        props.append(f'<w:spacing w:after="{space_after}"/>')
    ppr = f"<w:pPr>{''.join(props)}</w:pPr>" if props else ""
    return f"<w:p>{ppr}{runs_xml}</w:p>"


def text_para(text, style=None, **kw) -> str:
    return para(run(text), style=style, **kw)


def heading(text, level: int) -> str:
    return text_para(text, style=f"Heading{level}")


def code_block(text: str) -> str:
    """Each line is its own shaded paragraph — never \\n inside a run."""
    out = []
    lines = (text or "").splitlines() or [""]
    for line in lines:
        out.append(para(run(line, font="Consolas", size=8),
                        style="CodeLine", shade=GREY_CODE))
    return "".join(out)


def _cell(content_xml: str, width: int, shade: str | None = None) -> str:
    props = [f'<w:tcW w:w="{width}" w:type="dxa"/>']
    if shade:
        props.append(f'<w:shd w:val="clear" w:color="auto" w:fill="{shade}"/>')
    return f"<w:tc><w:tcPr>{''.join(props)}</w:tcPr>{content_xml}</w:tc>"


def table(headers: list[str], rows: list[list], proportions: list[float] | None = None) -> str:
    """Bordered table. DXA widths on the grid AND every cell, summing exactly."""
    n = len(headers)
    proportions = proportions or [1.0 / n] * n
    widths = [int(CONTENT_W * p) for p in proportions]
    widths[-1] = CONTENT_W - sum(widths[:-1])  # exact sum

    grid = "".join(f'<w:gridCol w:w="{w}"/>' for w in widths)
    border = '<w:tblBorders>' + "".join(
        f'<w:{side} w:val="single" w:sz="4" w:space="0" w:color="CBD5D2"/>'
        for side in ("top", "left", "bottom", "right", "insideH", "insideV")
    ) + '</w:tblBorders>'

    def row_xml(cells, is_header=False):
        tcs = []
        for i, val in enumerate(cells[:n]):
            content = para(run(val, bold=is_header, size=8,
                               color="2E3A38" if is_header else None),
                           style="TableText")
            tcs.append(_cell(content, widths[i], GREY_HDR if is_header else None))
        trpr = "<w:trPr><w:tblHeader/></w:trPr>" if is_header else ""
        return f"<w:tr>{trpr}{''.join(tcs)}</w:tr>"

    body = row_xml(headers, is_header=True)
    for r in rows:
        body += row_xml(["" if c is None else c for c in r])

    return (f'<w:tbl><w:tblPr><w:tblW w:w="{CONTENT_W}" w:type="dxa"/>{border}'
            f'<w:tblLayout w:type="fixed"/></w:tblPr>'
            f'<w:tblGrid>{grid}</w:tblGrid>{body}</w:tbl>'
            + text_para("", space_after=60))  # spacer so tables don't fuse


def page_break() -> str:
    return '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'


# --------------------------------------------------------------------------
# Static package parts
# --------------------------------------------------------------------------

_CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>"""

_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""

_DOC_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"""

_APP = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
<Application>adf-doc-gen</Application></Properties>"""


def _core(title: str, generated: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:title>{_t(title)}</dc:title>
<dc:creator>adf-doc-gen</dc:creator>
<dc:description>Azure Data Factory documentation generated {_t(generated)}</dc:description>
</cp:coreProperties>"""


def _style(sid, name, *, based="Normal", size=None, bold=False, color=None,
           font=None, outline=None, before=0, after=80, keep_next=False) -> str:
    rpr = []
    if font:
        rpr.append(f'<w:rFonts w:ascii="{font}" w:hAnsi="{font}" w:cs="{font}"/>')
    if bold:
        rpr.append("<w:b/>")
    if color:
        rpr.append(f'<w:color w:val="{color}"/>')
    if size:
        rpr.append(f'<w:sz w:val="{size * 2}"/><w:szCs w:val="{size * 2}"/>')
    # OOXML enforces pPr child order: keepNext -> spacing -> outlineLvl
    ppr = []
    if keep_next:
        ppr.append("<w:keepNext/>")
    ppr.append(f'<w:spacing w:before="{before}" w:after="{after}"/>')
    if outline is not None:
        ppr.append(f'<w:outlineLvl w:val="{outline}"/>')
    return (f'<w:style w:type="paragraph" w:styleId="{sid}">'
            f'<w:name w:val="{name}"/><w:basedOn w:val="{based}"/>'
            f'<w:pPr>{"".join(ppr)}</w:pPr>'
            f'<w:rPr>{"".join(rpr)}</w:rPr></w:style>')


_STYLES = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
           '<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
           '<w:docDefaults><w:rPrDefault><w:rPr>'
           '<w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" w:cs="Calibri"/>'
           '<w:sz w:val="20"/><w:szCs w:val="20"/><w:color w:val="222B29"/>'
           '</w:rPr></w:rPrDefault>'
           '<w:pPrDefault><w:pPr><w:spacing w:after="120" w:line="264" w:lineRule="auto"/>'
           '</w:pPr></w:pPrDefault></w:docDefaults>'
           '<w:style w:type="paragraph" w:default="1" w:styleId="Normal">'
           '<w:name w:val="Normal"/></w:style>'
           + _style("Title", "Title", size=26, bold=True, color="222B29",
                    font="Calibri Light", after=40)
           + _style("Subtitle", "Subtitle", size=10, color=MUTED, after=240)
           + _style("Heading1", "heading 1", size=16, bold=True, color=ACCENT,
                    font="Calibri Light", outline=0, before=320, after=120, keep_next=True)
           + _style("Heading2", "heading 2", size=12, bold=True, color="2E3A38",
                    outline=1, before=240, after=80, keep_next=True)
           + _style("Heading3", "heading 3", size=10, bold=True, color=ACCENT2,
                    outline=2, before=200, after=60, keep_next=True)
           + _style("CodeLine", "Code Line", size=8, font="Consolas", after=0)
           + _style("TableText", "Table Text", size=8, after=20)
           + _style("Muted", "Muted", size=8, color=MUTED, after=60)
           + '</w:styles>')


# --------------------------------------------------------------------------
# Content assembly
# --------------------------------------------------------------------------

_MODE_NOTES = {
    "factory": (
        "Every reference between the supplied objects resolved, so orchestration, "
        "lineage, and per-pipeline rollups below are complete within this factory. "
        "All of it is design-time truth derived statically from JSON definitions: "
        "no run history exists here, parameterised connections show parameter or "
        "Key Vault secret names (never values), work delegated to notebooks, stored "
        "procedures or external services is marked opaque rather than guessed at, "
        "and a pipeline no trigger starts may still be invoked externally."
    ),
    "partial": (
        "Some referenced objects were not supplied, so parts of this analysis are "
        "explicitly incomplete — the Overview lists exactly which objects to export "
        "next. Everything stated is design-time truth derived statically from JSON "
        "definitions: no run history, parameter and secret names never values, "
        "opaque hops marked rather than guessed at, and external invocation "
        "invisible."
    ),
}


def _join(items, sep=", ", empty="—"):
    items = [str(i) for i in items if i]
    return sep.join(items) if items else empty


def _labels(keys, entities):
    return [entities.get(k, {}).get("label", k) for k in keys]


def build_docx_body(payload: dict) -> str:
    mode = payload["mode"]
    fac = payload["factory"]
    entities = {e["key"]: e for e in payload["entities"]}
    parts: list[str] = []

    # ---- cover -----------------------------------------------------------
    parts.append(text_para(payload["title"], style="Title"))
    parts.append(text_para(
        f"Azure Data Factory documentation · mode: {mode} · generated "
        f"{payload['generated']} · {fac['pipelineCount']} pipelines, "
        f"{fac['activityCount']} activities, {fac['dataflowCount']} data flows",
        style="Subtitle"))
    parts.append(para(run("What this document can claim.  ", bold=True)
                      + run(_MODE_NOTES[mode]), shade=GREY_HDR, space_after=240))

    # ---- overview --------------------------------------------------------
    parts.append(heading("Overview", 1))
    parts.append(table(["Metric", "Value"], [
        ["Pipelines", fac["pipelineCount"]],
        ["Activities (all scopes)", fac["activityCount"]],
        ["Mapping data flows", fac["dataflowCount"]],
        ["Datasets", fac["datasetCount"]],
        ["Linked services", fac["linkedServiceCount"]],
        ["Triggers", fac["triggerCount"]],
        ["Distinct data entities touched", fac["entityCount"]],
        ["Data movement edges", fac["lineageEdgeCount"]],
        ["Unresolved references", fac["unresolvedCount"]],
        ["Warnings flagged", fac["warningCount"]],
    ], [0.6, 0.4]))

    missing = [r for r in payload["resolution"] if r["status"] == "MISSING"]
    if missing:
        parts.append(heading("Input completeness — what to export next", 2))
        parts.append(text_para(
            "These objects are referenced by the supplied input but were not included. "
            "Export each from ADF Studio (or run against the full repo) and regenerate.",
            style="Muted"))
        parts.append(table(
            ["Kind", "Missing object", "Impact", "Referenced by"],
            [[r["kind"], r["name"], r["impact"], _join(r["referencedBy"][:3], "; ")]
             for r in missing],
            [0.13, 0.2, 0.37, 0.3]))

    if payload["linkedServices"]:
        parts.append(heading("External systems this factory touches", 2))
        rows = []
        for ls in payload["linkedServices"]:
            flags = []
            if ls["keyVault"]:
                flags.append("Key Vault")
            if ls["inlineCredential"]:
                flags.append("inline credential (value not shown)")
            if ls["parameterized"]:
                flags.append("parameterised: " + _join(ls["parameters"]))
            if ls["unreferenced"]:
                flags.append("unreferenced")
            rows.append([ls["name"], ls["system"],
                         _join([ls["server"], ls["database"], ls["url"]], " · "),
                         _join(flags, "; ")])
        parts.append(table(["Linked service", "System", "Target", "Notes"],
                           rows, [0.2, 0.17, 0.33, 0.3]))

    # ---- orchestration ---------------------------------------------------
    parts.append(heading("Orchestration — how work starts and cascades", 1))
    if payload["triggers"]:
        parts.append(table(
            ["Trigger", "Type", "State", "Starts", "Schedule / event"],
            [[t["name"], t["type"], t["state"], _join(t["startsPipelines"]),
              t["detail"] or "—"] for t in payload["triggers"]],
            [0.18, 0.16, 0.1, 0.22, 0.34]))
        parts.append(text_para(
            "A trigger that is not Started does not fire — its pipelines only run "
            "when invoked another way.", style="Muted"))
    else:
        parts.append(text_para(
            "No trigger definitions were supplied. How and when these pipelines "
            "start is invisible to this document.", style="Muted"))

    calls = payload["pipelineCalls"]
    if calls:
        parts.append(heading("Pipeline call graph", 2))
        parts.append(table(
            ["Parent pipeline", "Invokes", "Via activity", "Waits?"],
            [[c["parent"], c["child"], c["activity"],
              "yes" if c["waitOnCompletion"] else "fire-and-forget"] for c in calls],
            [0.3, 0.3, 0.25, 0.15]))

    entry = [p for p in payload["pipelines"] if p["entryPoint"]]
    if entry:
        parts.append(heading("Entry points (no known invoker in this factory)", 2))
        parts.append(text_para(
            "Normal for orchestrators; a worker pipeline here may be dead or invoked "
            "externally (REST API, Synapse, Logic Apps) — this document cannot tell which.",
            style="Muted"))
        parts.append(table(["Pipeline", "Role"],
                           [[p["name"], p["roleLine"]] for p in entry], [0.35, 0.65]))

    # ---- lineage ---------------------------------------------------------
    parts.append(page_break())
    parts.append(heading("Data movement lineage", 1))
    parts.append(text_para(
        "Every movement edge the static analysis found. Opaque edges hand off to "
        "code ADF cannot see (notebooks, procs, external services) — the chain is "
        "broken there, and absence of downstream entries past that point is not "
        "evidence data does not flow. Dynamic targets are built at runtime; the "
        "static name is indicative only.", style="Muted"))
    parts.append(table(
        ["Source(s)", "Sink(s)", "Via", "Pipeline :: activity", "Notes"],
        [[_join(_labels(e["sources"], entities), "; "),
          _join(_labels(e["sinks"], entities), "; "),
          e["mechanism"], f"{e['pipeline']} :: {e['activity']}",
          _join([w for w in [("opaque" if e["opaque"] else ""),
                             ("dynamic" if e["dynamic"] else ""), e["detail"]] if w], " — ", "")]
         for e in payload["lineageEdges"]],
        [0.2, 0.2, 0.13, 0.22, 0.25]))

    movers = [e for e in payload["entities"] if e["upstream"] or e["downstream"]]
    roots = [e["label"] for e in movers if not e["upstream"]]
    leaves = [e["label"] for e in movers if not e["downstream"]]
    parts.append(heading("Roots and terminals", 2))
    parts.append(table(["", "Entities"], [
        ["Roots — external inputs nothing here writes", _join(roots)],
        ["Terminals — final outputs nothing here consumes", _join(leaves)],
    ], [0.35, 0.65]))

    # ---- pipelines -------------------------------------------------------
    parts.append(page_break())
    parts.append(heading("Pipelines", 1))
    parts.append(text_para(
        "Effective reads/writes include everything a pipeline transitively invokes "
        "(child pipelines and data flows). Step numbers give execution order within "
        "a scope; equal steps can run in parallel.", style="Muted"))
    for p in payload["pipelines"]:
        parts.append(heading(p["name"], 2))
        if p["description"]:
            parts.append(text_para(p["description"], style="Muted"))
        meta = [f"Role: {p['roleLine']}"]
        started = p["triggers"] + p["invokedBy"]
        meta.append("Started by: " + (_join(started) if started
                    else "nothing in this factory (may be invoked externally)"))
        if p["invokes"]:
            meta.append("Invokes: " + _join(p["invokes"]))
        if p["dataflows"]:
            meta.append("Data flows: " + _join(p["dataflows"]))
        if p["parameters"]:
            meta.append("Parameters: " + _join(f"{k} ({v})" for k, v in p["parameters"].items()))
        if p["variables"]:
            meta.append("Variables: " + _join(f"{k} ({v})" for k, v in p["variables"].items()))
        parts.append(text_para(".  ".join(meta) + ".", style="Muted"))
        rd = _join(_labels(p["effectiveReads"], entities))
        wr = _join(_labels(p["effectiveWrites"], entities))
        inc = ("  (incomplete — not supplied: " + _join(p["effectiveIncomplete"]) + ")"
               if p["effectiveIncomplete"] else "")
        parts.append(table(["", "Effective footprint" + inc], [
            ["Reads", rd], ["Writes", wr]], [0.12, 0.88]))

        rows = []
        for a in p["activities"]:
            flags = []
            if a.get("dynamic"):
                flags.append("dynamic")
            if a.get("opaque"):
                flags.append("opaque")
            if a.get("inactive"):
                flags.append("INACTIVE")
            if not a.get("retry") and a["category"] in ("movement", "transform"):
                flags.append("no retry")
            err = [d for d in a["dependsOn"] if d["on"] and d["on"] != "Succeeded"]
            if err:
                flags.append("runs on " + _join(f"{d['activity']}[{d['on']}]" for d in err))
            rows.append([
                "?" if a["step"] == 0 else a["step"],
                a["activity"] + ("" if a["scope"] == "(root)" else f"  [{a['scope']}]"),
                a["type"],
                _join(_labels(a["reads"], entities), "; "),
                _join(_labels(a["writes"], entities), "; "),
                _join(flags, "; ", ""),
            ])
        parts.append(table(["#", "Activity (scope)", "Type", "Reads", "Writes", "Flags"],
                           rows, [0.05, 0.25, 0.15, 0.2, 0.2, 0.15]))

    # ---- warnings --------------------------------------------------------
    parts.append(page_break())
    parts.append(heading("Warnings — read before changing anything", 1))
    if payload["warnings"]:
        parts.append(table(
            ["Severity", "Category", "Detail"],
            [[w["severity"], w["category"], w["message"]] for w in payload["warnings"]],
            [0.11, 0.2, 0.69]))
    else:
        parts.append(text_para("Nothing flagged by the static analysis.", style="Muted"))

    # ---- appendices ------------------------------------------------------
    parts.append(page_break())
    parts.append(heading("Appendix A — Datasets", 1))
    if payload["datasets"]:
        rows = []
        for d in payload["datasets"]:
            ep = d["endpoint"]
            target = _join([f"{ep['schema']}.{ep['object']}" if ep["schema"] and ep["object"] else ep["object"],
                            "/".join(x for x in [ep["container"], ep["path"]] if x) or None,
                            ep["url"]], " · ") if any(ep.values()) else (d["display"] or "—")
            notes = []
            if d["dynamic"]:
                notes.append("dynamic")
            if d["parameterized"]:
                notes.append("parameterised: " + _join(d["parameters"]))
            if d["unreferenced"]:
                notes.append("not referenced by any supplied pipeline or data flow")
            rows.append([d["name"], d["type"], d["linkedService"] or "—", target,
                         _join(notes, "; ", "")])
        parts.append(table(["Dataset", "Type", "Linked service", "Resolves to", "Notes"],
                           rows, [0.2, 0.14, 0.18, 0.26, 0.22]))
    else:
        parts.append(text_para("No dataset definitions were supplied.", style="Muted"))

    parts.append(heading("Appendix B — Data flows", 1))
    if payload["dataflows"]:
        for df in payload["dataflows"]:
            parts.append(heading(df["name"], 3))
            src = _join(s["dataset"] or s["label"] for s in df["sources"])
            snk = _join(s["dataset"] or s["label"] for s in df["sinks"])
            parts.append(text_para(f"Sources: {src}.  Sinks: {snk}.  "
                                   f"Used by: {_join(df['usedBy'], '; ')}.", style="Muted"))
            parts.append(table(
                ["#", "Step", "Op", "Inputs", "Notes"],
                [[s["execOrder"] or "?", s["name"], s["op"], _join(s["inputs"], "; "),
                  ("DEAD END — output unconsumed" if s["deadEnd"] else "")]
                 for s in sorted(df["steps"], key=lambda s: s["execOrder"] or 10**6)],
                [0.06, 0.24, 0.15, 0.3, 0.25]))
    else:
        parts.append(text_para("No data flow definitions were supplied.", style="Muted"))

    parts.append(heading("Appendix C — Triggers", 1))
    if payload["triggers"]:
        parts.append(table(
            ["Trigger", "Type", "State", "Starts", "Schedule / event"],
            [[t["name"], t["type"], t["state"], _join(t["startsPipelines"]),
              t["detail"] or "—"] for t in payload["triggers"]],
            [0.18, 0.16, 0.1, 0.22, 0.34]))
    else:
        parts.append(text_para("No trigger definitions were supplied.", style="Muted"))

    parts.append(heading("Appendix D — Entities (canonical data objects)", 1))
    parts.append(text_para(
        "Canonicalised so one table reached through two datasets is a single entry. "
        "Endpoint fields follow the cross-tool contract (system / server / database / "
        "schema / object / path / url / container) shared with pbi-doc-gen output.",
        style="Muted"))
    rows = []
    for e in payload["entities"]:
        ep = e["endpoint"]
        eps = _join([f"{k}={v}" for k, v in ep.items() if v], "; ", "")
        rows.append([e["label"], e["kind"], _join(e["roles"], "; "), eps,
                     _join(_labels(e["upstream"], entities), "; ", "root"),
                     _join(_labels(e["downstream"], entities), "; ", "terminal")])
    parts.append(table(["Entity", "Kind", "Roles", "Endpoint", "Depends on", "Feeds"],
                       rows, [0.18, 0.09, 0.15, 0.22, 0.18, 0.18]))

    sect = (f'<w:sectPr><w:pgSz w:w="{PAGE_W}" w:h="{PAGE_H}"/>'
            f'<w:pgMar w:top="{MARGIN}" w:right="{MARGIN}" w:bottom="{MARGIN}" '
            f'w:left="{MARGIN}" w:header="720" w:footer="720" w:gutter="0"/></w:sectPr>')

    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            f'<w:body>{"".join(parts)}{sect}</w:body></w:document>')


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def render_docx(payload: dict, out_path: str | Path) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    document = build_docx_body(payload)
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", _CONTENT_TYPES)
        z.writestr("_rels/.rels", _RELS)
        z.writestr("word/document.xml", document)
        z.writestr("word/_rels/document.xml.rels", _DOC_RELS)
        z.writestr("word/styles.xml", _STYLES)
        z.writestr("docProps/core.xml", _core(payload["title"], payload["generated"]))
        z.writestr("docProps/app.xml", _APP)
    return out_path
