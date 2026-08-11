# adf-doc-gen

Generate **living documentation** from Azure Data Factory JSON — a single
self-contained interactive HTML file, plus optional Word, JSON, and **agent
context** (markdown) outputs — built for the person (or LLM agent) who
inherits a factory and needs to answer, fast:

* How does work start, and what cascades from what? (triggers → orchestrators
  → child pipelines → data flows, as a drill-down tree)
* Where does the data come from and where does it land? (normalised lineage
  with a transitive "what breaks if X dies?" explorer)
* What does each pipeline actually do — at a glance, as a flow, and per
  activity in full detail?
* What is dead, fragile, opaque, or misconfigured? (a first-class Warnings
  view: unreferenced resources, broken references, missing retries, stopped
  triggers, inline credentials, dead data-flow branches…)

It is the Azure Data Factory sibling of
[`pbi-doc-gen`](../pbi-doc-gen) and shares its architecture, visual identity,
and honesty rules. The two tools' outputs share a **cross-tool lineage
contract** (see below) so they can later be joined end-to-end.

## Requirements

* Python 3.8+ (3.10+ recommended)
* **Nothing else.** No pip installs, no internet access, no Azure access.
  Everything — parsing, analysis, HTML, Word — is standard library only.

## Getting the JSON out of ADF

The tool accepts three input shapes; use whichever fits how your factory is
managed. (Menu names below are as of the Studio UI at time of writing —
Microsoft moves them occasionally, but the capabilities persist.)

### 1. Git repository (best)

If the factory has Git integration configured, the repo *is* the input: it
contains `pipeline/`, `dataset/`, `dataflow/`, `linkedService/`, `trigger/`
folders of clean per-resource JSON. Clone it and point the tool at the root:

```
python generate_docs.py path/to/adf-repo -o factory_docs.html
```

### 2. ARM template export

In ADF Studio: **Manage (toolbox icon) → ARM template → Export ARM template**.
Unzip and point the tool at `ARMTemplateForFactory.json` (or the folder).
The tool un-mangles ARM resource names (`[concat(parameters('factoryName'),
'/PL_x')]` → `PL_x`) automatically.

### 3. Hand-picked object JSON (the pragmatic path)

When the factory lives in a huge multi-project repo, or you only care about
one pipeline family, pull objects one at a time: open any pipeline, dataset,
data flow, linked service, or trigger in ADF Studio and use the **`{}` (view
code)** button in the top-right of the canvas. Paste each into a file in one
folder — names don't matter, subfolders don't matter; the tool classifies
each file by its content.

Start with the pipeline(s) you care about and run the tool. The **Overview
tab shows an "Input completeness" checklist** of every referenced object you
haven't supplied yet, with its impact — a literal shopping list for the next
round of *view code → paste*. Iterate until the banner says all references
resolved (or until you've decided the remaining gaps don't matter).

## Running

```
python generate_docs.py <input> [-o out.html] [--title "..."] [--json] [--word]
```

| Argument | Meaning |
|---|---|
| `<input>` | a resource JSON, an ARM template, or a folder of either |
| `-o / --output` | HTML output path (default `<input-name>_docs.html`) |
| `--title` | document title (default derived from the input name) |
| `--json` | also write the consolidated JSON payload next to the HTML |
| `--word` | also write a narrative Word document next to the HTML |
| `--agent` | also write an agent context document (`.agent.md`) next to the HTML |

Examples (see `examples/contoso-sales-etl/` for a full synthetic factory):

```
# full factory, all four outputs
python generate_docs.py examples/contoso-sales-etl --title "Contoso Sales ETL" --json --word --agent

# one hand-picked pipeline
python generate_docs.py PL_Ingest_Landing.json -o ingest_docs.html
```

### Batch loops

PowerShell, one document per subfolder of factories:

```powershell
Get-ChildItem -Directory .\factories | ForEach-Object {
  python generate_docs.py $_.FullName -o "docs\$($_.Name).html" --title $_.Name --json
}
```

bash:

```bash
for d in factories/*/; do
  n=$(basename "$d")
  python generate_docs.py "$d" -o "docs/$n.html" --title "$n" --json
done
```

### The agent context document (`--agent`)

`adf-doc-gen` began life as `adf_distill.py`, a distiller that wrote compact
markdown *for LLM agents' context windows* rather than for browsers. That
output survives here as the fourth renderer: `--agent` writes
`<output>.agent.md` — orientation and claims first (so a truncated read still
orients correctly), then missing inputs, warnings, the call tree, per-pipeline
effective footprints with compact activity tables and truncated queries,
lineage closure, resource tables, and the entity endpoint contract. Same
payload, same honesty rules as the HTML; a rough token count is printed on
generation so you can budget. Paste it into an agent's context when you want
the agent to *reason about* the factory rather than browse it.

## The two modes, and what the document may claim

The banner at the top of every tab states the mode:

* **factory** — every reference between the supplied objects resolved.
  Orchestration, lineage and rollups are complete *within this factory*.
* **partial** — some referenced objects weren't supplied. The analysis is
  explicitly incomplete; the Overview lists exactly what's missing and what
  each gap costs. Pipelines whose rolled-up reads/writes are affected carry a
  **rollup incomplete** badge.

Regardless of mode, these honesty rules always hold — they are the point of
the tool:

* **Design-time truth only.** Derived statically from JSON definitions. No
  run history exists here: nothing can be called "working", "failing", or
  "actively used".
* **Names, never secrets.** Key Vault references report the secret *name*;
  parameterised connections report the parameter *name*; a credential spotted
  inline in a connection string is **flagged but never printed**.
* **Opaque means opaque.** Notebooks, stored procedures, Azure Functions,
  SSIS packages and web calls do their real work in code ADF cannot see.
  Those hops are marked `opaque`, and lineage past them is *unknown, not
  absent*.
* **"Unused" is factory-scoped.** "Not referenced by any trigger or pipeline
  in this factory" is the strongest claim made. External invocation (REST
  API, Synapse, Logic Apps, another factory) is invisible to static analysis.
* **Dynamic is indicative.** A target built with expressions
  (`@concat(...)`) is resolved only at runtime; the static name shown is a
  best effort and badged `dynamic`.

## The tabs

* **Overview** — the conveyor banner (sources ▸ factory ▸ sinks), counts, the
  mode note, the input-completeness checklist (partial mode), and every
  external system the factory touches.
* **Orchestration** — the front door. The factory as an expandable tree:
  triggers → the pipelines they start → `ExecutePipeline` children → data
  flows. Entry points (pipelines nothing here invokes) are listed separately
  with honest phrasing. Repeated nodes show `↺` instead of looping forever.
* **Lineage** — the impact explorer ("type a table, get its full upstream and
  blast radius"), every movement edge with opaque/dynamic filters, roots
  (external inputs), terminals (final products), and the full transitive
  closure table.
* **Pipelines** — three disclosure levels per pipeline: (1) a one-line role
  plus **effective** reads/writes — rolled up through everything the pipeline
  transitively invokes; (2) the activity flow in execution order, grouped by
  container scope (ForEach/If/Switch/Until), where equal step numbers can run
  in parallel; (3) full per-activity detail — reads/writes, dependencies,
  retry/timeout policy, and the harvested SQL query or script.
* **Data flows** — each Mapping Data Flow's transformation chain parsed from
  its script, in execution order, with dead-end detection and every sink
  traced back to its sources.
* **Datasets** — each dataset resolved through its linked service to a
  physical target, with dynamic/parameterised/unreferenced badges.
* **Linked services** — connections, never credentials: system, target,
  auth style (Key Vault / inline-flagged / parameterised), and what uses it.
* **Triggers** — type, state (a Stopped trigger is loudly badged — its
  pipelines don't run on that schedule), what each starts.
* **Warnings** — everything flagged, filterable by category. Read it before
  changing anything.
* **Entities** — the canonical registry: every table, file path, proc,
  notebook and endpoint, deduplicated (`DB.dbo.Fact` ≡ `dbo.Fact`), with its
  aliases, endpoint contract, and transitive up/downstream.

Tabs that have nothing to show for *this input* are greyed out with a tooltip
explaining what's missing and what that costs — never silently empty.

## The cross-tool lineage contract (future `bi-lineage-join`)

Every source/sink entity carries a normalised endpoint:

```json
{"system": …, "server": …, "database": …, "schema": …,
 "object": …, "path": …, "url": …, "container": …}
```

with `null` for unknown and parameter/secret *names* where parameterised.
`pbi-doc-gen` emits the mirror image per semantic-model partition. A planned
third tool, **`bi-lineage-join`**, will take any mix of the two tools' JSON
payloads and render the bridge: report page → semantic model table →
partition → warehouse table → ADF activity → upstream source. That join will
be **confidence-tiered and honest about loose ends** (server aliases,
views-over-tables, and opaque hops make an exact join impossible), which is
exactly why both tools persist the raw normalised fields rather than a
pre-computed match.

## Reading the verdicts — caveats that matter

* `canon_table` merges same-named tables *across databases* into one entity
  (`SalesDW.dbo.Fact` and `Archive.dbo.Fact` become `dbo.fact`). In factories
  that copy between identically-named schemas this over-merges; check the
  entity's aliases and endpoint fields before acting on it.
* SQL harvesting is regex-based: it reads `FROM`/`JOIN`/`INSERT`/`MERGE`/
  `EXEC` targets from query text. CTEs, dynamic SQL built inside procs, and
  exotic dialects can slip through. It deliberately ignores string literals
  and comments.
* Data-flow lineage comes from parsing the flow script DSL. Flowlets are
  tracked as references but not expanded inline.
* An activity's "no retry" flag matters most on Copy/transform activities —
  ADF's default is zero retries, which is rarely what production wants.
* `Completed` dependencies run whether the parent succeeds *or fails* — the
  tool calls this out because it is a common source of "why did cleanup run
  after the load failed?" surprises.

## Repository layout

```
generate_docs.py          CLI
adfdocgen/
  loader.py               input collection + resource classification
  analyzer.py             all analysis (the single source of truth)
  renderer.py             payload assembly + HTML injection
  template.html           the self-contained interactive app
  word_writer.py          narrative .docx via hand-written OOXML (stdlib)
  agent_writer.py         agent context markdown (adf_distill.py's successor)
examples/contoso-sales-etl/   synthetic factory exercising every feature
out/                      pre-generated sample outputs
tests_render.js           jsdom render test (dev only; requires node+jsdom)
```

The embedded payload in any HTML output (or the `--json` file) is stable,
documented by example, and safe to feed into catalogs, CI checks, or the
future join tool.
