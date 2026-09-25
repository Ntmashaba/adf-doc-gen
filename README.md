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
[`pbi-doc-gen`](https://github.com/Ntmashaba/pbi-doc-gen) and shares its architecture, visual identity,
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
Unzip and point the tool at the folder. The tool un-mangles ARM resource
names (`[concat(parameters('factoryName'), '/PL_x')]` → `PL_x`) automatically.
If `ARMTemplateParametersForFactory.json` is in the same folder, its values
fill in connections the template leaves as `[parameters('…')]`; anything
still unset is listed as *chosen at deployment* rather than missing.

### 3. Hand-picked object JSON (the pragmatic path)

When the factory lives in a huge multi-project repo, or you only care about
one pipeline family, pull objects one at a time: open any pipeline, dataset,
data flow, linked service, or trigger in ADF Studio and use the **`{}` (view
code)** button in the top-right of the canvas. Paste each into a file in one
folder — names don't matter, subfolders don't matter; the tool classifies
each file by its content.

Start with the pipeline(s) you care about and run the tool. The **Overview
shows an "Input completeness" checklist** of every referenced object you
haven't supplied yet, with its impact — a literal shopping list for the next
round of *view code → paste*. Iterate until every reference resolves (or until you've decided the remaining
gaps don't matter). A hand-picked folder is reported as a **selection**, never
as the whole factory: other pipelines may use the same objects.

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

Examples (`examples/contoso-sales-etl/` is a small synthetic factory in Git
folder layout that exercises the features below):

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

## The three modes, and what the document may claim

The overview states the mode and a **"What this document can and cannot
see"** table (also in the Word and agent outputs):

* **factory**: a Git folder or ARM template where every file was read and
  every reference resolved.
* **selection**: hand-picked objects whose references all resolved. It
  cannot show that the whole factory was supplied.
* **partial**: some referenced objects weren't supplied, or some input files
  could not be read. The overview lists exactly what's missing and what each
  gap costs. Pipelines whose rolled-up reads/writes are affected carry a
  **rollup incomplete** badge; a missing data flow or dataset makes that
  activity's footprint *unknown*, never silently empty.

The coverage table also counts opaque activities, runtime-resolved targets,
connections chosen at deployment, objects defined twice, and secret values
withheld. Run history is always "not available".

Regardless of mode, these honesty rules always hold. They are the point of
the tool:

* **Design-time truth only.** Derived statically from JSON definitions. No
  run history exists here: nothing can be called "working", "failing", or
  "actively used". A trigger's state is the one saved in the definition.
* **Names, never secrets.** Secret values are removed before analysis and
  every output is checked again before it is written: passwords and keys in
  connection strings, `SecureString` values, secret-named settings, tokens in
  URL query strings, user info in URLs, bearer tokens, and parameters a child
  pipeline declares as `SecureString`. Each is replaced with `[redacted]`.
  Key Vault references report the secret *name*; an inline credential is
  flagged but never printed; a `SecureString` connection string is reported
  as stored in the definition, not as Key Vault.
* **Opaque means opaque.** Notebooks, stored procedures, Azure Functions,
  SSIS packages and web calls do their real work in code ADF cannot see.
  Those hops are marked `opaque`, and lineage past them is *unknown, not
  absent*.
* **"Unused" is scoped to the input.** "Not referenced by any trigger or
  pipeline in this input" is the strongest claim made. External invocation
  (REST API, Synapse, Logic Apps, another factory) is invisible to static
  analysis.
* **Dynamic is indicative.** A target built with expressions
  (`@concat(...)`) is resolved only at runtime and badged `dynamic`. Literal
  values passed to a parameterised dataset are resolved, so `Orders` and
  `Customers` through one generic dataset stay two objects.

## The views

* **Overview**: the summary banner (sources ▸ factory ▸ sinks), counts, the
  mode note, the coverage table, the input-completeness checklist, and every
  external system the factory touches.
* **Orchestration**: the factory as a tree: triggers → the pipelines they
  start → `ExecutePipeline` children → data flows. Entry points (pipelines
  nothing here invokes) are listed separately. Repeated nodes show `↺`
  instead of looping forever.
* **Lineage**: the impact explorer ("type a table, get its full upstream and
  downstream"), every movement edge with opaque/dynamic filters, roots
  (external inputs) and terminals (final products). The full transitive
  closure is behind a disclosure.
* **Pipelines**: per pipeline, a one-line role plus **effective** reads and
  writes rolled up through everything it invokes; the activity flow in
  execution order, grouped by container scope (ForEach/If/Switch/Until),
  where equal step numbers can run in parallel; and per-activity detail:
  reads/writes, dependencies, retry/timeout, and the SQL query or script.
* **Data flows**: each mapping data flow's transformations parsed from its
  script, in execution order, with dead-end detection and every sink traced
  back to its sources. A data flow used by several pipelines is credited to
  each of them. Power Query flows and flows without a script are shown with
  every source feeding every sink, and say so.
* **Datasets**: each dataset resolved through its linked service to a
  physical target, with dynamic/parameterised/unreferenced badges.
* **Linked services**: connections, never credentials: system, target,
  auth style (Key Vault / SecureString / inline-flagged / parameterised), and
  what uses it.
* **Triggers**: type, saved state (a Stopped trigger is flagged: its
  pipelines don't run on that schedule), schedule with time zone, the
  pipelines each starts with the parameter values it passes, and triggers it
  waits on. Schedule, event and tumbling-window triggers are all read.
* **Warnings**: real warnings first; notes about the limits of static
  analysis (dynamic targets, opaque work, missing retries, error paths)
  grouped by category and collapsed.
* **Entities**: every table, file path, procedure, notebook and endpoint,
  with its aliases, endpoint fields, and transitive up/downstream.

Views with nothing to show for *this input* are greyed out with a tooltip
explaining what's missing and what that costs. Search text and open
sections are kept when you switch views. Names containing quotes,
backslashes or Unicode link correctly, and long identifiers wrap so pages
fit desktop and phone widths.

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

## Reading the verdicts: caveats that matter

* **Objects are identified by where they live.** A table is keyed by server,
  database, schema and name; a file by storage account, container and path.
  `dbo.Sales` on the production server and `dbo.Sales` on the archive server
  are two objects. When the location is unknown, the object is scoped to the
  linked service that reached it rather than merged by name. A query that
  names a table without a database (`dbo.Fact`) takes the database from the
  connection it runs on.
* **A query replaces the dataset's table.** When a Copy or Lookup source has
  a SQL query or stored procedure, the tables the query names are what is
  read, on the dataset's connection. The dataset's own table is not claimed.
* **SQL is read, not executed.** A token-based reader handles comments,
  strings and quoted names (`[..]`, `".."`, backticks), and finds reads
  (`FROM`/`JOIN`/`APPLY`/`USING`), writes (`INSERT`, `UPDATE`, `MERGE`,
  `DELETE`, `TRUNCATE`, `SELECT … INTO`, `CREATE TABLE … AS`, `COPY INTO`)
  and procedures (`EXEC`, `CALL`). CTE names, `#temp` tables, table
  variables and `EXTRACT(… FROM …)` are not treated as tables. Dynamic SQL
  built inside procedures, and table names built with ADF expressions, can't
  be seen; the latter are badged `dynamic`.
* **Data-flow lineage** comes from parsing the flow script. Flowlets are
  tracked as references but not expanded inline. Endpoints defined only
  inside the script (inline sources without a dataset) are shown via their
  linked service.
* An activity's "no retry" note matters most on Copy/transform activities:
  ADF's default is zero retries, which is rarely what production wants.
* `Completed` dependencies run whether the parent succeeds *or fails*. The
  tool calls this out because it is a common source of "why did cleanup run
  after the load failed?" surprises.

## Testing

```
python -m unittest discover tests
```

The unit tests cover the guarantees above with small synthetic factories:
secrets absent from all four outputs, same-named objects on different
servers, query sources, tumbling-window triggers, coverage and modes, shared
data flows, parameterised datasets, and the SQL reader. `tests/test_example.py`
builds the Contoso example end to end.

Two dev checks need more than Python:

* `tests/regress_azure_templates.py` runs all 95 templates from Microsoft's
  [Azure-DataFactory](https://github.com/Azure/Azure-DataFactory) repository
  (pinned to commit `ce4c9ca`; see the script for the clone commands) and
  fails if any crashes. At that commit every template documents in factory
  mode, with 7 warnings in total.
* `tests/browser_check.cjs` opens generated pages in Chromium with
  Playwright and fails on script errors or pages wider than the window at
  1400px and 390px. Pages named `zz-*.html` also have every link clicked;
  generate one from `tests/fixtures/odd-names` (names with quotes,
  backslashes, Unicode and very long identifiers).

## Repository layout

```
generate_docs.py          CLI
adfdocgen/
  loader.py               input collection, classification, ARM parameters
  redact.py               secret removal for definitions and outputs
  sql_harvest.py          token-based SQL reader (reads, writes, procedures)
  analyzer.py             all analysis (the single source of truth)
  renderer.py             payload assembly + HTML injection
  template.html           the self-contained interactive page
  word_writer.py          narrative .docx via hand-written OOXML (stdlib)
  agent_writer.py         agent context markdown
examples/contoso-sales-etl/   synthetic factory (Git folder layout)
tests/                    unit tests, example test, dev checks, fixtures
```

Generated outputs are not kept in the repository; run the example command
above to produce them. The embedded payload in any HTML output (or the
`--json` file) is the same data every output is rendered from.
