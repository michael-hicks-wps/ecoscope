# EcoScope Desktop Workflow Build Guide

A practical guide based on building the CCFN SMART Download workflow. Update this as new patterns are discovered.

---

## Project Structure

```
projects/<project-name>/
├── tasks/                              # Task package (pip-installable)
│   ├── pyproject.toml
│   └── <project>_tasks/
│       ├── __init__.py
│       ├── _client.py                  # API client
│       └── _connection.py              # EcoScope connection class
├── workflows/                          # Importable workflow folders only
│   ├── <name>-dev/                     # -dev suffix = testing/development
│   │   ├── spec.yaml
│   │   ├── compile.sh
│   │   ├── layout.json
│   │   ├── metadata.json
│   │   ├── pixi.toml
│   │   └── ecoscope-workflows-<name>-workflow/   # compiled output
│   └── <name>/                         # no suffix = production/stable
├── dev_scripts/                        # Standalone dev/diagnostic scripts
└── outputs/                            # Local test outputs
```

**Naming convention:**
- `<name>-dev/` in `workflows/` means the template is still being developed/tested.
- Drop the `-dev` suffix when promoting to production.
- EcoScope Desktop shows the workflow folder name to the user, so keep it readable.

---

## The Task Package (`tasks/`)

### pyproject.toml

```toml
[project]
name = "<project>-tasks"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "wt-registry",
    "geopandas",
    "pandas",
    "requests",
    "urllib3",
    "pydantic-settings",
]

[project.entry-points."wt_registry"]
<project>_tasks = "<project>_tasks"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

The `wt_registry` entry point is how `wt-compiler` discovers your task functions.

### Task functions (`__init__.py`)

Tasks are plain Python functions decorated with `@task`:

```python
from ecoscope_workflows.decorators import task

@task
def my_task(param: str) -> pd.DataFrame:
    ...
```

- Return type must be concrete (not `Any`).
- Parameters become the workflow config form — keep them minimal and use `partial:` in `spec.yaml` to pre-fill anything that shouldn't change between runs.
- Avoid positional-only params; wt-compiler uses keyword arguments.

---

## The Connection System

EcoScope has a structured connection system. Use it instead of raw env vars so connections appear in the Desktop connections UI in the future.

### Connection class (`_connection.py`)

```python
from typing import Annotated, ClassVar
from pydantic import Field, SecretStr
from pydantic.functional_validators import BeforeValidator
from pydantic.json_schema import WithJsonSchema
from ecoscope.platform.connections import DataConnection

class CCFNConnection(DataConnection[SMARTConnectClient]):
    __ecoscope_connection_type__: ClassVar[str] = "ccfn"  # must be unique, lowercase

    server: Annotated[str, Field(description="CCFN SMART Connect server URL")]
    username: Annotated[str, Field(description="CCFN username")]
    password: Annotated[SecretStr, Field(description="CCFN password")]
    ca_uuid: Annotated[str, Field(description="Conservation Area UUID")]

    def get_client(self) -> SMARTConnectClient:
        return SMARTConnectClient(
            server=self.server,
            username=self.username,
            password=self.password.get_secret_value(),
        )

# This type resolves a connection name string to a CCFNConnection at runtime
CCFNConnectionParam = Annotated[
    CCFNConnection,
    BeforeValidator(CCFNConnection.from_named_connection),
    WithJsonSchema({"type": "string", "description": "A named CCFN SMART Connect connection."}),
]
```

### Environment variables

`DataConnection.from_named_connection(name)` reads env vars with this pattern:

```
ECOSCOPE_WORKFLOWS__CONNECTIONS__{TYPE}__{NAME}__{FIELD}
```

For a connection type `ccfn`, name `ccfn`, with fields `server`, `username`, `password`, `ca_uuid`:

```
ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__CCFN__SERVER=https://smart.example.org
ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__CCFN__USERNAME=myuser
ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__CCFN__PASSWORD=mypassword
ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__CCFN__CA_UUID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

Note the double-underscore separators and ALL_CAPS field names.

### Using the connection in a task

```python
@task
def download_smart_observations(
    connection: CCFNConnectionParam,
    time_range: ...,
) -> gpd.GeoDataFrame:
    client = connection.get_client()
    ca_uuid = connection.ca_uuid
    ...
```

In `spec.yaml`, set the connection name as a partial:

```yaml
- name: Animal Observations
  id: observations
  task: download_smart_observations
  partial:
    connection: "ccfn"      # matches the connection name in env vars
    time_range: ${{ workflow.time_range.return }}
```

---

## spec.yaml

### Key rules

1. **Absolute WSL paths only** for local package dependencies — relative paths are not supported:
   ```yaml
   requirements:
     - name: ccfn-tasks
       path: "/mnt/c/Users/Michael/Documents/Codex/ecoscope/projects/ccfn/tasks"
       editable: true
   ```
   `compile.sh` patches this to a Windows path after compilation.

2. **No YAML folded/literal scalars for strings that go into Python code.** The `>` and `|` YAML scalars embed literal newlines, which break ruff format during wt-compiler (exit code 2). Use single-line quoted strings:
   ```yaml
   # WRONG — embeds a newline, breaks compilation:
   description: >
     My long description.

   # RIGHT:
   description: "My long description."
   ```

3. **skipif: conditions: [never]** on output steps (maps, charts, widgets, dashboard) prevents them being skipped when data is empty — they should always render even with empty data:
   ```yaml
   skipif:
     conditions:
       - never
   ```

4. **Pre-fill everything that shouldn't vary per-run** in `partial:` to keep the config form short. Things to standardize:
   - `connection` name
   - `tile_layers` (TERRAIN base + SATELLITE at 0.5 opacity is a good default)
   - `static: false` on maps
   - `min_patrol_km`, thresholds, column names
   - `time_format`
   - `north_arrow_style` and `legend_style` on `draw_ecomap` (see below — must be pre-filled to avoid form errors)
   - `layer_style` on `create_point_layer` / `create_polyline_layer`

### Pre-filling `draw_ecomap` style params (required to avoid "must be object" error)

`NorthArrowStyle.style` is typed as `Dict[str, Any]`. The Desktop form cannot render an arbitrary dict input and shows a blocking "must be object" validation error. Pre-fill the entire `north_arrow_style` and `legend_style` blocks to eliminate them from the form:

```yaml
- name: Animal Observations Map
  id: obs_map
  task: draw_ecomap
  partial:
    geo_layers: ${{ workflow.obs_layer.return }}
    title: "..."
    tile_layers:
      - layer_name: "TERRAIN"
      - layer_name: "SATELLITE"
        opacity: 0.5
    static: false
    north_arrow_style:
      placement: "top-left"
      style:
        transform: "scale(0.8)"
    legend_style:
      placement: "bottom-right"
      title: "Legend"
      format_title: false
    max_zoom: 20
```

### Pre-filling layer styles

Pre-filling `layer_style` on point/polyline layer steps removes the "Advanced Configurations" section from the form. Use defaults from the generated `params.py`:

```yaml
# create_point_layer
layer_style:
  auto_highlight: false
  opacity: 1.0
  pickable: true
  filled: true
  get_line_width: 1.0
  line_width_units: "pixels"
  stroked: false
  get_radius: 5.0
  radius_units: "pixels"
  radius_scale: 1.0

# create_polyline_layer
layer_style:
  auto_highlight: false
  opacity: 1.0
  pickable: true
  get_width: 3.0
  width_units: "pixels"
  cap_rounded: true
```

> **Tip:** After compilation, check the generated `params.py` in the compiled workflow package to see exactly what fields each step exposes and what their defaults are. This is the authoritative source for what can be pre-filled.

### `rjsf-overrides` — Alternative field hiding mechanism

Instead of pre-filling a field with `null` to remove it from the form, you can hide it via `rjsf-overrides` in the task instance:

```yaml
- id: my_task
  task: my_custom_task
  rjsf-overrides:
    uiSchema:
      internal_param:
        "ui:widget": "hidden"
```

This is useful when:
- You can't easily pre-fill the field (e.g., it expects a complex object and `null` isn't valid)
- You want to keep the parameter removable without recompiling

Pre-filling with `partial:` is still preferred for most cases (cleaner, works with all field types).

Also supported: populating dropdowns from EarthRanger live data using `EarthRangerEnumResolver`:
```yaml
rjsf-overrides:
  schema:
    $defs:
      EventTypeEnum:
        resolver: EarthRangerEnumResolver
        params:
          method: get_event_type_choices
  properties:
    event_type:
      $ref: "#/$defs/EventTypeEnum"
```

### `all_geometry_are_none` skip condition for layer tasks

Layer creation tasks (`create_point_layer`, `create_polyline_layer`) should skip if all geometry values are null. Add this condition alongside the standard defaults:

```yaml
- id: obs_layer
  task: create_point_layer
  skipif:
    conditions:
      - any_is_empty_df
      - any_dependency_skipped
      - all_geometry_are_none
```

When you provide your own `skipif` block, it overrides `task-instance-defaults` entirely — you must repeat all conditions you want to keep.

### Tile layer standard defaults

```yaml
tile_layers:
  - layer_name: "TERRAIN"
  - layer_name: "SATELLITE"
    opacity: 0.5
```

### skipif defaults

Put this at the top level to auto-skip tasks whose inputs are empty or whose dependencies failed:

```yaml
task-instance-defaults:
  skipif:
    conditions:
      - any_is_empty_df
      - any_dependency_skipped
```

### `gather_dashboard` — include `groupers` for ungrouped workflows

Both the tutorial and the `understanding-spec.md` examples include a `groupers` entry in `gather_dashboard`, even for ungrouped (single-view) workflows:

```yaml
- id: dashboard
  task: gather_dashboard
  partial:
    details: ${{ workflow.workflow_details.return }}
    widgets:
      - ${{ workflow.obs_map_widget.return }}
    groupers:
      - index_name: "All"
    time_range: ${{ workflow.time_range.return }}
```

`groupers: [{index_name: "All"}]` explicitly declares the AllGrouper (all data in one view). This field is likely optional (the AllGrouper is the default), but all SDK examples include it — add it for correctness.

### Widget pipeline — always use `persist_text` (required)

Every widget must follow this three-step pipeline:
1. Render task (`draw_ecomap`, `draw_time_series_bar_chart`, etc.) → produces an HTML string
2. `persist_text` → saves the HTML to a file in `${{ env.ECOSCOPE_WORKFLOWS_RESULTS }}` and returns a URL
3. `create_*_widget_single_view` → receives the URL and wraps it into a widget object

**Skipping `persist_text` embeds the full HTML inline in the results JSON**, inflating the file by 2–5 MB per widget and causing Desktop to stall or fail to load the dashboard. This is the correct pattern for all widget types (maps, charts, tables).

```yaml
# 1. Render
- id: obs_map
  task: draw_ecomap
  partial:
    geo_layers: ${{ workflow.obs_layer.return }}
    ...
  skipif:
    conditions:
      - never

# 2. Persist to file
- id: obs_map_html
  task: persist_text
  partial:
    root_path: ${{ env.ECOSCOPE_WORKFLOWS_RESULTS }}
    text: ${{ workflow.obs_map.return }}

# 3. Widget wraps the URL
- id: obs_map_widget
  task: create_map_widget_single_view
  partial:
    title: "My Map"
    view: null
    data: ${{ workflow.obs_map_html.return }}
  skipif:
    conditions:
      - never
```

Note `view: null` on ungrouped widget tasks — required per the SDK docs.

Source: `doc/platform-sdk/content/tutorials/widgets.md`

### Grouped workflows (multi-view dashboards)

For workflows that split data by category, month, etc. and show a view dropdown in the dashboard, replace the single-view pipeline with `split_groups` → `mapvalues` → `merge_widget_views`. See `doc/platform-sdk/content/tutorials/groupers.md` for the full pattern.

Available grouper types:
- `AllGrouper` — Single view (all data), the default
- `ValueGrouper` — One view per distinct value in a categorical column
- `TemporalGrouper` — One view per time period (Year, Month, YearMonth, Date, DayOfWeek, etc.)
- `SpatialGrouper` — One view per geographic region (EarthRanger spatial feature groups only)

---

## Production workflow examples

The [ecoscope-platform-workflows-releases](https://github.com/ecoscope-platform-workflows-releases) GitHub org hosts open-source production workflows. Study these as reference for real-world patterns:

| Workflow | Widgets | Key patterns |
|----------|---------|--------------|
| `events` | 4 | Groupers, bar/pie/map widgets |
| `event-details` | 7 | Conditional skipif, summary tables, single-value widgets |
| `subject-tracking` | 10 | Relocations→trajectories, time density, speed profiles |
| `patrols` | 9 | Dual data streams, dynamic categorization, `rjsf-overrides` |

The `patrols` workflow is most similar to CCFN usage. Look at its `spec.yaml` and `layout.json` as a reference.

---

## compile.sh

The canonical compile script. Always use this — never run `wt-compiler` directly.

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/ecoscope-workflows-<name>-workflow"
PIXI_TOML="$OUTPUT_DIR/pixi.toml"

echo "Compiling workflow…"
~/.pixi/bin/wt-compiler compile \
    --spec="$SCRIPT_DIR/spec.yaml" \
    --pkg-name-prefix=ecoscope-workflows \
    --results-env-var=ECOSCOPE_WORKFLOWS_RESULTS \
    --no-progress \
    --clobber

echo "Installing pixi environment to generate pixi.lock (WSL path in effect)…"
~/.pixi/bin/pixi install --manifest-path="$PIXI_TOML"

echo "Removing .pixi/ env…"
rm -rf "$OUTPUT_DIR/.pixi"

echo "Patching WSL path → Windows path in pixi.toml…"
sed -i 's|path = "/mnt/c/|path = "C:/|g' "$PIXI_TOML"

echo "Done. Compiled output: $OUTPUT_DIR"
```

**Why each step is needed:**

| Step | Why |
|------|-----|
| `wt-compiler compile` | Generates the Python DAG package from spec.yaml |
| `pixi install` | Generates `pixi.lock` (Desktop requires this file for validation) |
| `rm -rf .pixi/` | Removes symlinks — Windows can't copy them without elevated permissions, causing Desktop import to fail |
| `sed` path patch | Replaces WSL path (`/mnt/c/`) with Windows path (`C:/`) so Desktop's pixi can resolve the local package |

**`--pkg-name-prefix=ecoscope-workflows` is mandatory.** Desktop validates that the compiled subfolder name starts with `ecoscope-workflows-` or `ecoscope_workflows_`. Any other prefix causes "This folder doesn't contain a valid workflow template."

**Run from Windows terminal:**
```bash
wsl -e bash -c "bash /mnt/c/Users/Michael/Documents/Codex/ecoscope/projects/ccfn/workflows/ccfn-smart-download-dev/compile.sh"
```

**Trigger a recompile any time:**
- `spec.yaml` changes
- `tasks/ccfn_tasks/` code changes
- `tasks/pyproject.toml` dependency changes

---

## Desktop Import

### What Desktop validates (`validateFolderPath`)

When you select a local folder in EcoScope Desktop, it checks:
1. A subfolder exists whose name starts with `ecoscope-workflows-` or `ecoscope_workflows_`
2. That subfolder contains `pixi.toml`
3. That subfolder contains `pixi.lock`

→ The folder you select is the **workflow folder** (e.g., `ccfn-smart-download-dev/`), not the compiled subfolder.

### What Desktop imports (`addLocalFolderWorkflowTemplate`)

It copies the entire workflow folder to AppData using Node.js `fs.cp`. This will fail silently if `.pixi/` exists because Windows cannot copy directory symlinks without elevated permissions — which is why `compile.sh` removes `.pixi/` after `pixi install`.

### Import troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| "This folder doesn't contain a valid workflow template" | Compiled subfolder name doesn't start with `ecoscope-workflows-` | Use `--pkg-name-prefix=ecoscope-workflows` in compile.sh |
| "This folder doesn't contain a valid workflow template" | `pixi.lock` missing | Run `pixi install` before importing (compile.sh does this) |
| "Could not import local template" | `.pixi/` directory with symlinks in the workflow folder | Remove `.pixi/` before importing (compile.sh does this) |
| "Could not import local template" | Old compiled folder with `.pixi/` also present | Delete any stale compiled folders from the workflow directory |

---

## Development Workflow

### First build

1. Write task functions in `tasks/<project>_tasks/__init__.py`
2. Write connection class in `tasks/<project>_tasks/_connection.py`
3. Write `tasks/pyproject.toml`
4. Write `spec.yaml`, `layout.json`, `metadata.json`, `pixi.toml` in `workflows/<name>-dev/`
5. Write `compile.sh` in `workflows/<name>-dev/`
6. Run compile.sh
7. Import the `workflows/<name>-dev/` folder in EcoScope Desktop

### Iterating

- **Task code change** → recompile → re-import in Desktop (delete old template first)
- **spec.yaml change** → recompile → re-import in Desktop
- **Connection/env var change** → no recompile needed; Desktop reads env vars at run time

### Promoting to production

Duplicate `workflows/<name>-dev/` as `workflows/<name>/` once the workflow is stable. Update `metadata.json` version and dates.

---

## Things That Don't Work / Gotchas

- **Relative paths in spec.yaml** (`path: ".."`) — not supported by wt-compiler, must use absolute WSL path.
- **YAML folded scalars (`>`) for description strings** — embed literal newlines that break ruff format in the generated Python (exit code 2 from wt-compiler). Always use quoted single-line strings.
- **Running wt-compiler directly without pixi install** — produces no `pixi.lock`; Desktop import fails validation.
- **Running wt-compiler with wrong `--pkg-name-prefix`** — compiled folder name won't match Desktop's validation regex.
- **Leaving `.pixi/` in the workflow folder** — Node.js `fs.cp` on Windows fails copying symlinks → "Could not import local template".
- **Multiple compiled folders in the workflow dir** — if an old compiled folder with `.pixi/` exists alongside a new one, the copy still fails.
- **`wsl -e bash -c "compile.sh"`** — Git Bash doesn't resolve the script path correctly. Use `wsl -e bash -c "bash /mnt/c/full/path/to/compile.sh"` instead.
- **`draw_ecomap` "must be object" blocking error in the config form** — Caused by `NorthArrowStyle.style: Dict[str, Any]` which the form can't render. Fix: pre-fill `north_arrow_style` and `legend_style` in `partial:` in spec.yaml. Same principle applies to any `Dict[str, Any]` typed parameter — always pre-fill it.
- **"Advanced Configurations" sections in the form** — These appear when optional sub-objects (`layer_style`, etc.) are not pre-filled. Pre-fill them in `partial:` using the defaults from the generated `params.py` to eliminate these sections.
- **"must be object" / "must be string" blocking errors on Optional fields** — The Desktop form pre-validates all visible fields, including optional ones. Complex types like `LegendDefinition` (contains a `Sort` enum), `PlotStyle` (contains `Union[int, List[int]]`), and `BarLayoutStyle` all produce blocking errors even when left untouched. Fix: pre-fill them as `null` in spec.yaml — e.g., `legend: null`, `plot_style: null`, `layout_style: null`, `color_column: null`. This removes the section from the form entirely. **Rule of thumb: pre-fill every optional parameter on every task step with either a concrete value or `null` — the goal is for the form to show only `workflow_details` (name) and `time_range`.**
- **"Why is my widget empty?"** — Most likely caused by the skip system. Check `any_is_empty_df` (no data in time range), `all_geometry_are_none` (null geometry), or connection errors. Runtime error details are available under **View Metadata** in the kebab menu for the failed workflow row.
- **Maps and charts stall or fail to load in Desktop** — Caused by passing the raw HTML string directly from `draw_ecomap`/`draw_time_series_bar_chart` to `create_*_widget_single_view`. The full HTML (2–5 MB per widget, including Lonboard/Plotly JS) gets embedded inline in the results JSON, which Desktop cannot efficiently load. Fix: always insert a `persist_text` step between the render task and the widget task. See "Widget pipeline" section above.
- **"Cannot read properties of undefined (reading 'name')" JS error on form load** — The Desktop JS reads `formData.workflow_details.name` at startup to display the workflow run name. If `workflow_details.name` is pre-filled in spec.yaml, the field disappears from the formdata schema entirely, causing the JS to get `undefined.name`. Fix: **never pre-fill `workflow_details.name`** — always leave it for the user to enter. Pre-filling `description` only is fine. This also follows the pattern of all built-in Desktop workflows, where users name their own runs.
