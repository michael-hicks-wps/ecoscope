"""
CCFN custom tasks for EcoScope Desktop.

Registered tasks:
    download_smart_observations    — download all "All *" patrolobservation queries
    download_smart_patrols         — download all patrolquery tracks, filtered by length
    draw_species_map               — Folium map with dual species + conservancy filter panels (+ optional boundary layer)
    draw_species_chart             — Plotly stacked bar chart with species + conservancy filters
    draw_patrol_map                — Folium map with patrol polylines, conservancy + date filters (+ optional boundary layer)
    draw_patrol_chart              — Plotly stacked bar chart of patrol effort with conservancy + date filters
    draw_lcc_map                   — Folium map of land cover change polygons with boundary + conservancy + label + date filters
    estimate_conservancy_boundary  — derive per-conservancy boundary polygons from observation/patrol footprint
"""
from __future__ import annotations

import json
import logging
import re
from typing import Annotated, Optional, cast

import folium
import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from shapely.geometry import box as shapely_box
from shapely.ops import unary_union
from pydantic import Field
from wt_registry import register

from ecoscope.platform.annotations import AnyGeoDataFrame
from ecoscope.platform.tasks.filter._filter import TimeRange

from ._connection import CCFNConnectionParam

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

TARGET_FOLDERS = [
    "Fixed Route Kavango & Zambezi [NRWG]",
]

STUDY_AREA_BOUNDS = {
    "min_lon": 17.0, "max_lon": 27.0,
    "min_lat": -23.0, "max_lat": -16.5,
}

KEY_SPECIES = [
    "Black Rhino",
    "Cheetah",
    "Elephant",
    "Ground Hornbill",
    "Hippopotamus",
    "Impala black-faced",
    "Lion",
    "Pangolin",
    "Tsessebe",
    "Wild Dog",
    "Zebra Mountain",
]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _date_strings(time_range: TimeRange) -> tuple[str, str]:
    fmt = "%Y-%m-%d %H:%M:%S"
    return (
        time_range.since.strftime(fmt),
        time_range.until.strftime(fmt),
    )


def _filter_aberrant(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    n = len(gdf)
    b = STUDY_AREA_BOUNDS
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    c = gdf.to_crs(epsg=32734).geometry.centroid.to_crs(epsg=4326)
    gdf = gdf[
        (c.x >= b["min_lon"]) & (c.x <= b["max_lon"]) &
        (c.y >= b["min_lat"]) & (c.y <= b["max_lat"]) &
        ~((c.x.abs() < 0.01) & (c.y.abs() < 0.01))
    ]
    dropped = n - len(gdf)
    if dropped:
        log.warning("%s: dropped %d record(s) with aberrant geometry.", label, dropped)
    return gdf.reset_index(drop=True)


def _clip_to_study_area(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    """Clip geometries to the study area bounding box.

    Unlike _filter_aberrant (which drops whole rows by centroid), this trims
    individual aberrant vertices from LineStrings so a single GPS glitch doesn't
    extend a track into the ocean.
    """
    b = STUDY_AREA_BOUNDS
    bbox = shapely_box(b["min_lon"], b["min_lat"], b["max_lon"], b["max_lat"])
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.intersection(bbox)
    empty = gdf.geometry.is_empty
    if empty.any():
        log.warning("%s: clipping removed %d geometry(s) entirely.", label, empty.sum())
    return gdf[~empty].reset_index(drop=True)


def _slim(gdf: gpd.GeoDataFrame, keep: list[str]) -> gpd.GeoDataFrame:
    """Drop all columns except geometry and those in `keep` that actually exist."""
    cols = ["geometry"] + [c for c in keep if c in gdf.columns]
    return gdf[cols]


def _resolve_duplicate_column(gdf: gpd.GeoDataFrame, col: str) -> gpd.GeoDataFrame:
    """If `col` appears more than once, collapse duplicates to the first non-blank value."""
    mask = gdf.columns == col
    if mask.sum() <= 1:
        return gdf
    combined = (
        gdf.loc[:, mask]
        .replace("", pd.NA)
        .bfill(axis=1)
        .iloc[:, 0]
    )
    gdf = gdf.loc[:, ~mask].copy()
    gdf[col] = combined
    return gdf


def _filter_patrol_length(gdf: gpd.GeoDataFrame, min_km: float) -> gpd.GeoDataFrame:
    gdf_m = gdf.to_crs(epsg=32734)
    mask = gdf_m.geometry.length / 1000 >= min_km
    dropped = (~mask).sum()
    if dropped:
        log.info("Patrol length filter (>= %.1f km): dropped %d patrol(s).", min_km, dropped)
    return gdf[mask].reset_index(drop=True)


def _normalise_column(gdf: gpd.GeoDataFrame, candidates: list[str], rename_to: str) -> gpd.GeoDataFrame:
    """Find the first matching column (case- and separator-insensitive) and rename it."""
    def key(s: str) -> str:
        return s.lower().replace(" ", "_").replace("-", "_")
    target_key = key(rename_to)
    col_map = {key(c): c for c in gdf.columns}
    for cand in candidates:
        k = key(cand)
        if k in col_map:
            actual = col_map[k]
            if actual != rename_to:
                gdf = gdf.rename(columns={actual: rename_to})
            return gdf
    log.warning("Column not found in data. Tried: %s. Available: %s", candidates, list(gdf.columns))
    return gdf


def _run_queries(
    connection: CCFNConnectionParam,
    queries: list[dict],
    start_date: str,
    end_date: str,
) -> gpd.GeoDataFrame:
    client = connection.get_client()
    ca_uuid = connection.ca_uuid
    gdfs = []
    for i, q in enumerate(queries, 1):
        log.info("  [%d/%d] %s — %s", i, len(queries), q.get("_conservancy"), q.get("name"))
        gdf = client.run_query_as_geodataframe(q["uuid"], start_date, end_date, ca_uuid=ca_uuid)
        if gdf is None or gdf.empty:
            log.info("          → no data, skipped.")
            continue
        gdf["conservancy"] = q.get("_conservancy", "unknown")
        gdf["query_name"] = q.get("name", "unknown")
        gdfs.append(gdf)
    if not gdfs:
        return gpd.GeoDataFrame()
    return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")


# ── Registered tasks ──────────────────────────────────────────────────────────

@register(
    description=(
        "Download all wildlife observation records from the CCFN SMART Connect server "
        "for the selected time range. Connection credentials are read from environment "
        "variables following the EcoScope connection naming convention."
    )
)
def download_smart_observations(
    connection: CCFNConnectionParam,
    time_range: Annotated[TimeRange, Field(description="Date range for data download")],
) -> Annotated[AnyGeoDataFrame, Field()]:
    """Download all animal observation data from CCFN SMART Connect."""
    client = connection.get_client()
    ca_uuid = connection.ca_uuid
    start_date, end_date = _date_strings(time_range)

    all_queries = client.get_queries_in_folders(
        folder_names=TARGET_FOLDERS,
        ca_uuid=ca_uuid,
        spatial_only=True,
    )
    obs_queries = [
        q for q in all_queries
        if q.get("typeKey", "").lower() == "patrolobservation"
        and "all" in q.get("name", "").lower()
    ]
    log.info("Downloading %d observation queries …", len(obs_queries))
    gdf = _run_queries(connection, obs_queries, start_date, end_date)

    if not gdf.empty:
        log.info("Observation query columns: %s", list(gdf.columns))
        gdf = _filter_aberrant(gdf, "Animal observations")
        gdf = _resolve_duplicate_column(gdf, "Species")
        if "Waypoint_Date" in gdf.columns:
            gdf["Waypoint_Date"] = pd.to_datetime(gdf["Waypoint_Date"], errors="coerce")
        gdf = _slim(gdf, ["Waypoint_Date", "conservancy", "Species"])

    return cast(AnyGeoDataFrame, gdf)


@register(
    description=(
        "Download all patrol track data from the CCFN SMART Connect server "
        "for the selected time range, filtered to patrols of at least the "
        "specified minimum length."
    )
)
def download_smart_patrols(
    connection: CCFNConnectionParam,
    time_range: Annotated[TimeRange, Field(description="Date range for data download")],
    min_patrol_km: Annotated[
        float,
        Field(
            default=5.0,
            ge=0.0,
            description="Minimum patrol track length in kilometres. Shorter patrols are excluded.",
        ),
    ] = 5.0,
) -> Annotated[AnyGeoDataFrame, Field()]:
    """Download all patrol effort tracks from CCFN SMART Connect."""
    client = connection.get_client()
    ca_uuid = connection.ca_uuid
    start_date, end_date = _date_strings(time_range)

    all_queries = client.get_queries_in_folders(
        folder_names=TARGET_FOLDERS,
        ca_uuid=ca_uuid,
        spatial_only=True,
    )
    patrol_queries = [
        q for q in all_queries
        if q.get("typeKey", "").lower() == "patrolquery"
    ]
    log.info("Downloading %d patrol queries …", len(patrol_queries))
    gdf = _run_queries(connection, patrol_queries, start_date, end_date)

    if not gdf.empty:
        log.info("Patrol query columns: %s", list(gdf.columns))
        gdf = _filter_aberrant(gdf, "Patrol effort")
        gdf = _clip_to_study_area(gdf, "Patrol effort")
        gdf = _filter_patrol_length(gdf, min_patrol_km)
        if "Start_Date" in gdf.columns:
            gdf["Start_Date"] = pd.to_datetime(gdf["Start_Date"], errors="coerce")
        gdf = _normalise_column(
            gdf,
            ["Transport Mode", "Transport_Mode", "TransportMode", "transport_mode", "Mode_of_Transport"],
            "Transport Mode",
        )
        gdf = _normalise_column(gdf, ["Members", "Member", "Team_Members"], "Members")
        # Simplify patrol geometries to reduce map render time (5m tolerance)
        gdf["geometry"] = gdf.to_crs(epsg=32734).geometry.simplify(5).to_crs(epsg=4326)
        gdf = _slim(gdf, ["Start_Date", "conservancy", "query_name", "Transport Mode", "Members"])

    return cast(AnyGeoDataFrame, gdf)


# ── Colour palettes ───────────────────────────────────────────────────────────

_SP_PALETTE = [
    "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
    "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
    "#dcbeff", "#9a6324", "#800000", "#aaffc3", "#ffd8b1",
]

_CON_PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#17becf", "#bcbd22", "#393b79",
    "#637939", "#8c6d31", "#843c39", "#7b4173", "#3182bd",
    "#e6550d", "#31a354", "#756bb1", "#636363", "#6baed6",
]


def _sp_cols(vals: list[str]) -> dict[str, str]:
    return {v: _SP_PALETTE[i % len(_SP_PALETTE)] for i, v in enumerate(sorted(vals))}


def _con_cols(vals: list[str]) -> dict[str, str]:
    return {v: _CON_PALETTE[i % len(_CON_PALETTE)] for i, v in enumerate(sorted(vals))}


# ── Shared filter-panel helpers ───────────────────────────────────────────────

_PANEL_CSS = """\
<style>
.leaflet-div-icon { background:none !important; border:none !important; }
.ccfn-panel {
  position:fixed; z-index:99999;
  background:rgba(255,255,255,0.95);
  border:1px solid #bbb; border-radius:6px;
  padding:8px 10px; font-size:12px; font-family:sans-serif;
  max-height:300px; overflow-y:auto;
  box-shadow:2px 2px 8px rgba(0,0,0,.25);
}
.ccfn-panel h4 { margin:0 0 5px; font-size:13px; }
.ccfn-panel label {
  display:flex; align-items:center; gap:5px;
  margin:2px 0; cursor:pointer; white-space:nowrap;
}
.ccfn-toggler {
  font-size:11px; color:#555; cursor:pointer;
  text-decoration:underline; margin-left:4px;
}
.ccfn-row { display:flex; align-items:center; margin:2px 0; }
.ccfn-row label { flex:1; display:flex; align-items:center; gap:5px; cursor:pointer; white-space:nowrap; }
.ccfn-only {
  font-size:10px; color:#999; cursor:pointer;
  text-decoration:underline; padding-left:6px; opacity:0; white-space:nowrap;
}
.ccfn-row:hover .ccfn-only { opacity:1; }
</style>"""

# Shared panel JS: handles All/None toggle, per-item "only" selection,
# and checkbox changes. Calls window._ccfnOnFilter() for map/chart-specific work.
_PANEL_JS = """\
<script>
(function () {
  function apply() {
    if (typeof window._ccfnOnFilter === 'function') window._ccfnOnFilter();
    if (typeof window._ccfnBroadcast === 'function') window._ccfnBroadcast();
  }
  document.addEventListener('change', function (e) {
    if (e.target.classList.contains('ccfn-sp') ||
        e.target.classList.contains('ccfn-con') ||
        e.target.classList.contains('ccfn-lbl')) apply();
  });
  document.addEventListener('click', function (e) {
    var t = e.target;
    /* All / None toggle */
    if (t.dataset.ccfnToggle) {
      var cls = t.dataset.ccfnToggle;
      var nowOn = t.dataset.on === '1';
      document.querySelectorAll('.' + cls).forEach(function (cb) {
        cb.checked = !nowOn;
      });
      t.dataset.on = nowOn ? '0' : '1';
      t.textContent = nowOn ? 'All' : 'None';
      apply();
      return;
    }
    /* "only" link */
    if (t.classList.contains('ccfn-only')) {
      var cls2 = t.dataset.cls;
      var val  = t.dataset.val;
      document.querySelectorAll('.' + cls2).forEach(function (cb) {
        cb.checked = (cb.value === val);
      });
      apply();
    }
  });
})();
</script>"""


def _panel_html(
    panel_id: str,
    title: str,
    cb_class: str,
    items: list[str],
    colours: dict[str, str] | None,
    top: str,
    left: str,
) -> str:
    rows = []
    for item in sorted(items):
        swatch = ""
        if colours and item in colours:
            swatch = (
                '<span style="display:inline-block;width:10px;height:10px;'
                f'border-radius:2px;background:{colours[item]};flex-shrink:0"></span>'
            )
        safe = item.replace("&", "&amp;").replace('"', "&quot;")
        only_btn = (
            f'<span class="ccfn-only" data-cls="{cb_class}" data-val="{safe}">only</span>'
        )
        rows.append(
            f'<div class="ccfn-row">'
            f'<label><input type="checkbox" class="{cb_class}" value="{safe}" checked>'
            f" {swatch} {item}</label>"
            f"{only_btn}</div>"
        )
    toggler = (
        f'<span class="ccfn-toggler" data-ccfn-toggle="{cb_class}" data-on="1">None</span>'
    )
    return (
        f'<div id="{panel_id}" class="ccfn-panel" style="top:{top};left:{left};">'
        f"<h4>{title} {toggler}</h4>"
        + "\n".join(rows)
        + "</div>"
    )


def _sidebar_section(
    section_id: str,
    title: str,
    cb_class: str,
    items: list[str],
    colours: dict[str, str] | None,
    collapsible: bool = False,
    key_items: list[str] | None = None,
) -> str:
    """Filter section for sidebar layout (block flow, not floating).

    When collapsible=True the section renders as a <details> dropdown.
    When key_items is provided the species list is split into a "Key Species"
    group (checked by default) and an "Other" group (unchecked by default),
    separated by a divider.  All checkboxes keep class cb_class so existing
    filter/broadcast logic is unaffected.
    """
    def _row(item: str, extra_class: str, checked: bool) -> str:
        swatch = ""
        if colours and item in colours:
            swatch = (
                '<span style="display:inline-block;width:10px;height:10px;'
                f'border-radius:2px;background:{colours[item]};flex-shrink:0"></span>'
            )
        safe = item.replace("&", "&amp;").replace('"', "&quot;")
        only_btn = f'<span class="ccfn-only" data-cls="{cb_class}" data-val="{safe}">only</span>'
        chk = "checked" if checked else ""
        return (
            f'<div class="ccfn-row">'
            f'<label><input type="checkbox" class="{cb_class} {extra_class}" value="{safe}" {chk}>'
            f" {swatch} {item}</label>"
            f"{only_btn}</div>"
        )

    if key_items is not None:
        key_set = set(key_items)
        present_keys = [k for k in key_items if k in items]
        other_items = sorted(i for i in items if i not in key_set)

        key_cls = f"{cb_class}-key"
        other_cls = f"{cb_class}-other"
        key_toggler = f'<span class="ccfn-toggler" data-ccfn-toggle="{key_cls}" data-on="1">None</span>'
        other_toggler = f'<span class="ccfn-toggler" data-ccfn-toggle="{other_cls}" data-on="0">All</span>'

        key_rows = "\n".join(_row(i, key_cls, checked=True) for i in present_keys)
        other_rows = "\n".join(_row(i, other_cls, checked=False) for i in other_items)

        subgroup_style = (
            "font-size:0.75rem;font-weight:600;text-transform:uppercase;"
            "letter-spacing:.05em;color:#888;margin:6px 0 2px 0;display:flex;"
            "align-items:center;gap:6px;"
        )
        divider_style = "border:none;border-top:1px solid #ddd;margin:6px 0;"

        items_html = (
            f'<div style="{subgroup_style}">Key Species {key_toggler}</div>'
            + key_rows
            + f'<hr style="{divider_style}">'
            + f'<div style="{subgroup_style}">Other {other_toggler}</div>'
            + other_rows
        )
    else:
        rows = []
        for item in sorted(items):
            rows.append(_row(item, "", checked=True))
        toggler = f'<span class="ccfn-toggler" data-ccfn-toggle="{cb_class}" data-on="1">None</span>'
        items_html = toggler + "\n".join(rows)

    if collapsible:
        return (
            f'<details id="{section_id}" class="sidebar-section ccfn-details">'
            f"<summary>{title}</summary>"
            f'<div class="ccfn-details-body">{items_html}</div>'
            f"</details>"
        )
    return (
        f'<div id="{section_id}" class="sidebar-section">'
        f"<h4>{title}</h4>"
        + items_html
        + "</div>"
    )


def _date_range_map_panel_html(min_month: str, max_month: str) -> str:
    """Floating date-range panel injected into Folium map HTML (bottom-left)."""
    return (
        '<div id="ccfn-date-panel" class="ccfn-panel"'
        ' style="top:auto;bottom:10px;left:10px;max-height:none;">'
        "<h4>Date Range</h4>"
        '<div style="display:flex;flex-direction:column;gap:4px;">'
        '<label style="display:flex;align-items:center;gap:5px;white-space:nowrap;">'
        f'From&nbsp;<input type="month" id="ccfn-since" value="{min_month}"'
        f' min="{min_month}" max="{max_month}"'
        ' style="font-size:11px;padding:1px 3px;"></label>'
        '<label style="display:flex;align-items:center;gap:5px;white-space:nowrap;">'
        f'To&nbsp;<input type="month" id="ccfn-until" value="{max_month}"'
        f' min="{min_month}" max="{max_month}"'
        ' style="font-size:11px;padding:1px 3px;"></label>'
        '<span id="ccfn-date-all"'
        ' style="font-size:10px;color:#555;cursor:pointer;text-decoration:underline;">All</span>'
        "</div></div>"
    )


def _date_range_sidebar_html(min_month: str, max_month: str) -> str:
    """Date-range section for chart sidebar."""
    return (
        '<div class="sidebar-section">'
        '<h4>Date Range</h4>'
        '<div style="display:flex;flex-direction:column;gap:5px;">'
        '<label style="font-size:12px;">From<br>'
        f'<input type="month" id="ccfn-since" value="{min_month}"'
        f' min="{min_month}" max="{max_month}"'
        ' style="font-size:11px;width:100%;"></label>'
        '<label style="font-size:12px;">To<br>'
        f'<input type="month" id="ccfn-until" value="{max_month}"'
        f' min="{min_month}" max="{max_month}"'
        ' style="font-size:11px;width:100%;"></label>'
        '<span id="ccfn-date-all"'
        ' style="font-size:10px;color:#555;cursor:pointer;text-decoration:underline;">All</span>'
        "</div></div>"
    )


# ── Shared sidebar CSS for chart tasks ───────────────────────────────────────

_SIDEBAR_CSS = """\
<style>
html, body { margin:0; padding:0; height:100%; font-family:sans-serif; font-size:12px; }
#ccfn-layout { display:flex; height:100vh; overflow:hidden; }
#ccfn-sidebar {
  width:185px; min-width:185px; padding:8px 10px;
  overflow-y:auto; border-right:1px solid #ddd; background:#fafafa;
}
#ccfn-plot { flex:1; min-width:0; height:100vh; }
#ccfn-plot .js-plotly-plot,
#ccfn-plot .plot-container { width:100% !important; height:100% !important; }
.sidebar-section { margin-bottom:14px; }
.sidebar-section h4 {
  margin:0 0 5px; font-size:13px; font-weight:600;
  border-bottom:1px solid #e0e0e0; padding-bottom:3px;
}
.ccfn-row { display:flex; align-items:center; margin:2px 0; }
.ccfn-row label {
  flex:1; display:flex; align-items:center; gap:5px;
  cursor:pointer; white-space:nowrap;
}
.ccfn-toggler {
  font-size:11px; color:#555; cursor:pointer;
  text-decoration:underline; margin-left:4px;
}
.ccfn-only {
  font-size:10px; color:#aaa; cursor:pointer;
  text-decoration:underline; padding-left:6px; opacity:0;
}
.ccfn-row:hover .ccfn-only { opacity:1; }
.ccfn-details { margin-bottom:14px; }
.ccfn-details > summary {
  font-size:13px; font-weight:600; cursor:pointer;
  border-bottom:1px solid #e0e0e0; padding-bottom:3px;
  list-style:none; display:flex; align-items:center;
  user-select:none;
}
.ccfn-details > summary::-webkit-details-marker { display:none; }
.ccfn-details > summary::after { content:" ▶"; font-size:9px; color:#999; margin-left:auto; }
.ccfn-details[open] > summary::after { content:" ▼"; }
.ccfn-details-body { padding-top:4px; }
</style>"""


# ── Shared conservancy-bbox helpers ──────────────────────────────────────────

def _conservancy_bboxes(
    boundary_wgs: "gpd.GeoDataFrame | None",
    fallback_gdf: "gpd.GeoDataFrame | None" = None,
) -> "tuple[dict, list | None]":
    """Compute per-conservancy bounding boxes for map auto-zoom.

    Priority: boundary_wgs (from estimate_conservancy_boundary) > fallback_gdf
    (the raw patrol/observation GeoDataFrame, grouped by 'conservancy' column).

    Returns
    -------
    bbox_dict  : {con_name: [[south, west], [north, east]], ...}
    full_bounds: [[south_min, west_min], [north_max, east_max]] or None
    """
    bbox: dict = {}

    if boundary_wgs is not None and not boundary_wgs.empty and "name" in boundary_wgs.columns:
        for name, group in boundary_wgs.groupby("name"):
            b = group.geometry.total_bounds  # [west, south, east, north]
            bbox[str(name)] = [[float(b[1]), float(b[0])], [float(b[3]), float(b[2])]]
    elif fallback_gdf is not None and not fallback_gdf.empty and "conservancy" in fallback_gdf.columns:
        gdf_4326 = fallback_gdf.to_crs(4326) if fallback_gdf.crs and fallback_gdf.crs.to_epsg() != 4326 else fallback_gdf
        for con, group in gdf_4326.groupby("conservancy"):
            valid = group[group.geometry.notna() & ~group.geometry.is_empty]
            if valid.empty:
                continue
            b = valid.geometry.total_bounds
            bbox[str(con)] = [[float(b[1]), float(b[0])], [float(b[3]), float(b[2])]]

    if not bbox:
        return {}, None

    all_s = min(v[0][0] for v in bbox.values())
    all_w = min(v[0][1] for v in bbox.values())
    all_n = max(v[1][0] for v in bbox.values())
    all_e = max(v[1][1] for v in bbox.values())
    return bbox, [[all_s, all_w], [all_n, all_e]]


def _zoom_js(map_var: str, bbox_dict: dict, full_bounds: "list | None") -> str:
    """Return the CONSERVANCY_BBOX / _FULL_BOUNDS declarations + zoom helper function.

    The zoom helper `window._ccfnZoom(cCon, aCon)` is called at the end of each
    map's _ccfnOnFilter.  cCon is the {con: 1} dict of checked conservancies;
    aCon is true when all are checked.
    """
    if not bbox_dict or full_bounds is None:
        # No bbox data — return empty stubs so the rest of the JS still works
        return """\
<script>
window._ccfnZoom = function () {};
</script>"""

    bbox_json  = json.dumps(bbox_dict)
    full_json  = json.dumps(full_bounds)
    return f"""\
<script>
var CONSERVANCY_BBOX = {bbox_json};
var _FULL_BOUNDS     = {full_json};
var _map_ref = window['{map_var}'];

window._ccfnZoom = function (cCon, aCon) {{
  if (!_map_ref) return;
  if (aCon) {{
    _map_ref.fitBounds(_FULL_BOUNDS, {{padding: [30, 30], animate: true}});
    return;
  }}
  var checked = Object.keys(cCon);
  if (checked.length === 0) return;
  var zb = null;
  checked.forEach(function (con) {{
    var bb = CONSERVANCY_BBOX[con];
    if (!bb) return;
    if (!zb) {{ zb = L.latLngBounds(bb[0], bb[1]); }}
    else {{ zb.extend(bb[0]); zb.extend(bb[1]); }}
  }});
  if (zb) _map_ref.fitBounds(zb, {{padding: [30, 30], animate: true}});
}};
</script>"""


# ── Shared BroadcastChannel JS helper ────────────────────────────────────────

def _bc_js(min_month: str, max_month: str, has_species: bool = False) -> str:
    """Return the BroadcastChannel script block for a widget.

    Broadcasts full state (date + conservancy + optionally species) on any
    date or conservancy/species change.  On receive, applies incoming state
    (intersection with what exists locally) and re-runs the widget filter.

    has_species — True for obs map/chart widgets, False for patrol/lcc widgets.
    """
    sp_collect = (
        "Array.from(document.querySelectorAll('.ccfn-sp:checked')).map(function(cb){return cb.value;})"
        if has_species else "[]"
    )
    sp_apply = """\
    if (evt.data.species) {
      var spSet = {};
      evt.data.species.forEach(function(v){ spSet[v]=1; });
      document.querySelectorAll('.ccfn-sp').forEach(function(cb){ cb.checked = !!spSet[cb.value]; });
    }""" if has_species else ""

    return f"""\
<script>
(function () {{
  var _ch = null;
  try {{ _ch = new BroadcastChannel('ccfn-filter'); }} catch(e) {{}}
  var s = document.getElementById('ccfn-since');
  var u = document.getElementById('ccfn-until');
  var a = document.getElementById('ccfn-date-all');

  function collectState() {{
    return {{
      since: s ? s.value : '',
      until: u ? u.value : '',
      conservancy: Array.from(document.querySelectorAll('.ccfn-con:checked')).map(function(cb){{return cb.value;}}),
      species: {sp_collect}
    }};
  }}

  window._ccfnBroadcast = function () {{
    if (!_ch) return;
    try {{ _ch.postMessage(collectState()); }} catch(e) {{}}
  }};

  function run() {{ if (typeof window._ccfnOnFilter === 'function') window._ccfnOnFilter(); }}

  function onDateChange(broadcast) {{
    run();
    if (broadcast) window._ccfnBroadcast();
  }}

  if (s) s.addEventListener('change', function () {{ onDateChange(true); }});
  if (u) u.addEventListener('change', function () {{ onDateChange(true); }});
  if (a) a.addEventListener('click', function () {{
    if (s) s.value = '{min_month}';
    if (u) u.value = '{max_month}';
    onDateChange(true);
  }});

  if (_ch) _ch.onmessage = function (evt) {{
    /* Apply date */
    if (s && evt.data.since !== undefined) s.value = evt.data.since;
    if (u && evt.data.until !== undefined) u.value = evt.data.until;
    /* Apply conservancy (intersect with local checkboxes) */
    if (evt.data.conservancy) {{
      var conSet = {{}};
      evt.data.conservancy.forEach(function(v){{ conSet[v]=1; }});
      document.querySelectorAll('.ccfn-con').forEach(function(cb){{ cb.checked = !!conSet[cb.value]; }});
    }}
    /* Apply species if this widget has them */{sp_apply}
    run();
  }};
}})();
</script>"""


# ── draw_species_map ──────────────────────────────────────────────────────────

@register(
    description=(
        "Render animal observation points on an interactive map. "
        "Two floating panels let users filter simultaneously by species AND conservancy. "
        "A date-range panel at the bottom-left filters markers by observation month."
    )
)
def draw_species_map(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="Observation GeoDataFrame (geometry, Species, conservancy, Waypoint_Date)"),
    ],
    boundary: Annotated[
        Optional[AnyGeoDataFrame],
        Field(
            default=None,
            description=(
                "Optional conservancy boundary polygons from estimate_conservancy_boundary. "
                "Each row must have a 'name' column. Displayed as a static light-blue reference layer."
            ),
        ),
    ] = None,
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full date filter span to match other widgets.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Interactive Folium map with species + conservancy + date-range filter panels."""
    gdf = geodataframe
    if gdf is None or gdf.empty:
        return "<p>No observation data available.</p>"

    species_vals = sorted(gdf["Species"].dropna().unique().tolist()) if "Species" in gdf.columns else []
    con_vals = sorted(gdf["conservancy"].dropna().unique().tolist()) if "conservancy" in gdf.columns else []
    sp_colours = _sp_cols(species_vals)

    # Compute date range for the date-filter panel
    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
    elif "Waypoint_Date" in gdf.columns:
        dates = pd.to_datetime(gdf["Waypoint_Date"], errors="coerce").dropna()
        min_month = dates.min().strftime("%Y-%m") if not dates.empty else ""
        max_month = dates.max().strftime("%Y-%m") if not dates.empty else ""
    else:
        min_month = max_month = ""

    bounds = gdf.total_bounds  # [west, south, east, north]
    sw, ne = [bounds[1], bounds[0]], [bounds[3], bounds[2]]
    centre = [(sw[0] + ne[0]) / 2, (sw[1] + ne[1]) / 2]
    m = folium.Map(location=centre, tiles="CartoDB positron")
    m.fit_bounds([sw, ne], padding_top_left=[40, 40], padding_bottom_right=[40, 40])

    # Per-conservancy boundary layers (filterable)
    boundary_meta_sp: list[dict] = []
    if boundary is not None and not boundary.empty:
        bwgs = boundary.to_crs(4326)
        bwgs = bwgs[bwgs.geometry.notna() & ~bwgs.geometry.is_empty]
        for _, brow in bwgs.iterrows():
            con_name = str(brow.get("name") or "Unknown")
            gj = folium.GeoJson(
                brow.geometry.__geo_interface__,
                style_function=lambda _f: {
                    "fillColor": "#4a90d9",
                    "fillOpacity": 0.15,
                    "color": "#4a90d9",
                    "weight": 2,
                    "dashArray": "6 4",
                },
                tooltip=con_name,
            )
            gj.add_to(m)
            boundary_meta_sp.append({"name": gj.get_name(), "con": con_name})

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        sp = str(row.get("Species") or "Unknown")
        con = str(row.get("conservancy") or "Unknown")
        date_val = row.get("Waypoint_Date", "")
        date_str = str(date_val.date()) if hasattr(date_val, "date") else str(date_val or "")
        month_str = date_val.strftime("%Y-%m") if hasattr(date_val, "strftime") else ""
        color = sp_colours.get(sp, "#888888")

        # HTML-encode attribute values to prevent breaking data-* attributes
        sp_attr = sp.replace("&", "&amp;").replace('"', "&quot;")
        con_attr = con.replace("&", "&amp;").replace('"', "&quot;")

        icon_html = (
            f'<div class="ccfn-obs" data-sp="{sp_attr}" data-con="{con_attr}"'
            f' data-month="{month_str}"'
            f' style="width:8px;height:8px;border-radius:50%;background:{color};'
            f'border:1px solid rgba(0,0,0,.4);box-sizing:border-box;"></div>'
        )
        popup_html = (
            f"<b>{sp}</b>"
            f"<br><span style='color:#666'>Conservancy:</span> {con}"
            f"<br><span style='color:#666'>Date:</span> {date_str}"
        )
        folium.Marker(
            [geom.y, geom.x],
            icon=folium.DivIcon(html=icon_html, icon_size=(8, 8), icon_anchor=(4, 4)),
            popup=folium.Popup(popup_html, max_width=200),
        ).add_to(m)

    map_var = m.get_name()
    html = m.get_root().render()

    # Per-conservancy bounding boxes for auto-zoom
    boundary_wgs_sp = boundary.to_crs(4326) if (boundary is not None and not boundary.empty) else None
    bbox_dict, full_bounds = _conservancy_bboxes(boundary_wgs_sp, fallback_gdf=gdf)
    zoom_js = _zoom_js(map_var, bbox_dict, full_bounds)

    # Build sidebar (same structure as chart tasks)
    date_section = _date_range_sidebar_html(min_month, max_month) if min_month else ""
    con_section  = _sidebar_section("ccfn-con-sec", "Conservancy", "ccfn-con", con_vals, colours=None)
    sp_section   = _sidebar_section("ccfn-sp-sec",  "Species",     "ccfn-sp",  species_vals, colours=sp_colours, collapsible=True, key_items=KEY_SPECIES)
    sidebar_html = (
        '<div id="ccfn-sidebar">'
        + (date_section or "")
        + con_section
        + sp_section
        + "</div>"
    )

    # CSS that turns the Folium body into a sidebar + map flex row
    map_layout_css = """\
<style>
html, body { margin:0 !important; padding:0 !important; height:100% !important; overflow:hidden !important; }
body { display:flex !important; flex-direction:row !important; height:100vh !important; align-items:stretch !important; }
.folium-map { flex:1 !important; height:100vh !important; min-width:0 !important; }
</style>"""

    # Map-specific filter: show/hide markers + boundary polygons.
    boundary_meta_sp_json = json.dumps(boundary_meta_sp)
    map_filter_js = f"""\
<script>
var BOUNDARY_META_SP = {boundary_meta_sp_json};
var _map_sp = window['{map_var}'];
window._ccfnOnFilter = function () {{
  var cSp = {{}}, cCon = {{}};
  document.querySelectorAll('.ccfn-sp:checked').forEach(function (cb) {{ cSp[cb.value] = 1; }});
  document.querySelectorAll('.ccfn-con:checked').forEach(function (cb) {{ cCon[cb.value] = 1; }});
  var aSp  = Object.keys(cSp).length  === document.querySelectorAll('.ccfn-sp').length;
  var aCon = Object.keys(cCon).length === document.querySelectorAll('.ccfn-con').length;
  var sinceEl = document.getElementById('ccfn-since');
  var untilEl = document.getElementById('ccfn-until');
  var since = sinceEl ? sinceEl.value : '';
  var until = untilEl ? untilEl.value : '';
  document.querySelectorAll('.ccfn-obs').forEach(function (el) {{
    var monthOk = true;
    if (el.dataset.month) {{
      if (since && el.dataset.month < since) monthOk = false;
      if (until && el.dataset.month > until) monthOk = false;
    }}
    var show = (aSp  || cSp[el.dataset.sp]) &&
               (aCon || cCon[el.dataset.con]) &&
               monthOk;
    var node = el.parentElement;
    while (node && !node.classList.contains('leaflet-marker-icon')) {{
      node = node.parentElement;
    }}
    if (node) node.style.display = show ? '' : 'none';
  }});
  BOUNDARY_META_SP.forEach(function (meta) {{
    var layer = window[meta.name];
    if (!layer || !_map_sp) return;
    if (aCon || !!cCon[meta.con]) {{
      if (!_map_sp.hasLayer(layer)) layer.addTo(_map_sp);
    }} else {{
      if (_map_sp.hasLayer(layer)) _map_sp.removeLayer(layer);
    }}
  }});
  if (typeof window._ccfnZoom === 'function') window._ccfnZoom(cCon, aCon);
}};
</script>"""

    date_bc_js = _bc_js(min_month, max_month, has_species=True)

    inject_head = "\n".join(filter(None, [_SIDEBAR_CSS, map_layout_css]))
    inject_js   = "\n".join(filter(None, [zoom_js, map_filter_js, date_bc_js, _PANEL_JS]))
    html = html.replace("</head>", inject_head + "\n</head>", 1)
    html = re.sub(r'(<body[^>]*>)', lambda m2: m2.group(0) + "\n" + sidebar_html, html, count=1)
    return html.replace("</html>", inject_js + "\n</html>")


# ── draw_species_chart ────────────────────────────────────────────────────────

@register(
    description=(
        "Render a combined monthly bar chart of animal observations stacked by species. "
        "A sidebar lets users filter by conservancy AND species; bar heights and floating "
        "totals update to reflect the filtered selection. A date-range section zooms the x-axis."
    )
)
def draw_species_chart(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="Observation GeoDataFrame (Waypoint_Date, Species, conservancy)"),
    ],
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full x-axis span so all widgets share the same date axis.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Stacked bar chart (x=month, colour=species) with sidebar species+conservancy+date filters."""
    gdf = geodataframe
    if gdf is None or gdf.empty:
        return "<p>No observation data available.</p>"

    df = pd.DataFrame(gdf.drop(columns="geometry", errors="ignore"))

    required = {"Waypoint_Date", "Species", "conservancy"}
    if not required.issubset(df.columns):
        return f"<p>Required columns not found: {required - set(df.columns)}</p>"

    df["Waypoint_Date"] = pd.to_datetime(df["Waypoint_Date"], errors="coerce")
    df = df.dropna(subset=["Waypoint_Date", "Species", "conservancy"])
    df["month"] = df["Waypoint_Date"].dt.to_period("M").dt.to_timestamp()

    species_vals = sorted(df["Species"].unique().tolist())
    con_vals = sorted(df["conservancy"].unique().tolist())
    sp_colours = _sp_cols(species_vals)

    # Full month spine: use the configured time range when available so empty
    # months still appear on the x-axis (keeping all chart widgets in sync).
    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
        all_months = [
            p.to_timestamp()
            for p in pd.period_range(time_range.since, time_range.until, freq="M")
        ]
    else:
        min_month = df["Waypoint_Date"].min().strftime("%Y-%m")
        max_month = df["Waypoint_Date"].max().strftime("%Y-%m")
        all_months = sorted(df["month"].unique().tolist())
    month_keys = [m.strftime("%Y-%m-%d") for m in all_months]

    # Per-(species, conservancy) monthly counts for JS aggregation
    agg = (
        df.groupby(["month", "Species", "conservancy"])
        .size()
        .reset_index(name="count")
    )

    # data_matrix[sp][con] = list of counts aligned to all_months
    data_matrix: dict[str, dict[str, list[int]]] = {}
    for sp in species_vals:
        data_matrix[sp] = {}
        for con in con_vals:
            sub = agg[(agg["Species"] == sp) & (agg["conservancy"] == con)]
            lookup = {row["month"].strftime("%Y-%m-%d"): int(row["count"]) for _, row in sub.iterrows()}
            data_matrix[sp][con] = [lookup.get(mk, 0) for mk in month_keys]

    n_months = len(all_months)
    n_species = len(species_vals)

    # One trace per species, coloured by species colour (matching the observation map)
    fig = go.Figure()
    for sp in species_vals:
        color = sp_colours[sp]
        y_init = [sum(data_matrix[sp][con][i] for con in con_vals) for i in range(n_months)]
        fig.add_trace(go.Bar(
            x=all_months,
            y=y_init,
            name=sp,
            marker_color=color,
        ))

    # Text trace: total observation count above each monthly stack (index = n_species)
    total_obs_init = [
        sum(data_matrix[sp][con][i] for sp in species_vals for con in con_vals)
        for i in range(n_months)
    ]
    fig.add_trace(go.Scatter(
        x=all_months,
        y=total_obs_init,
        mode="text",
        text=[str(v) if v > 0 else "" for v in total_obs_init],
        textposition="top center",
        textfont=dict(size=10, color="#444"),
        showlegend=False,
        hoverinfo="none",
    ))

    fig.update_layout(
        barmode="stack",
        title="Animal Observations by Month",
        xaxis_title="Month",
        yaxis_title="Observations",
        showlegend=False,
        margin=dict(l=60, r=30, t=60, b=40),
        autosize=True,
        height=None,
    )

    chart_frag = fig.to_html(full_html=False, include_plotlyjs=True,
                              default_width="100%", default_height="100%")

    match = re.search(r'Plotly\.newPlot\s*\(\s*["\']([^"\']+)["\']', chart_frag)
    div_id = match.group(1) if match else "ccfn-chart"

    # Date range at top; conservancy (no swatches, bars coloured by species);
    # species collapsible with swatches matching the observation map.
    date_section = _date_range_sidebar_html(min_month, max_month)
    con_section  = _sidebar_section("ccfn-con-sec", "Conservancy", "ccfn-con", con_vals, colours=None)
    sp_section   = _sidebar_section("ccfn-sp-sec",  "Species",     "ccfn-sp",  species_vals, colours=sp_colours, collapsible=True, key_items=KEY_SPECIES)

    data_matrix_json = json.dumps(data_matrix)
    con_vals_json    = json.dumps(con_vals)
    sp_vals_json     = json.dumps(species_vals)

    chart_filter_js = f"""\
<script>
(function () {{
  var DATA    = {data_matrix_json};
  var CONS    = {con_vals_json};
  var SPECIES = {sp_vals_json};
  var N_SPECIES = {n_species};  /* index of the text-total trace */
  var gd = document.getElementById('{div_id}');

  window._ccfnOnFilter = function () {{
    var cSp = {{}}, cCon = {{}};
    document.querySelectorAll('.ccfn-sp:checked').forEach(function (cb)  {{ cSp[cb.value]  = 1; }});
    document.querySelectorAll('.ccfn-con:checked').forEach(function (cb) {{ cCon[cb.value] = 1; }});
    var aSp  = Object.keys(cSp).length  === document.querySelectorAll('.ccfn-sp').length;
    var aCon = Object.keys(cCon).length === document.querySelectorAll('.ccfn-con').length;
    var newY = [], newVis = [], barIdx = [];
    SPECIES.forEach(function (sp, i) {{
      var visible = aSp || !!cSp[sp];
      newVis.push(visible);
      var nMonths = DATA[sp][CONS[0]].length;
      var y = new Array(nMonths).fill(0);
      if (visible) {{
        CONS.forEach(function (con) {{
          if (aCon || cCon[con]) {{
            var vals = DATA[sp][con];
            for (var j = 0; j < vals.length; j++) y[j] += vals[j];
          }}
        }});
      }}
      newY.push(y);
      barIdx.push(i);
    }});
    Plotly.restyle(gd, {{y: newY, visible: newVis}}, barIdx);

    /* Recalculate total count (y-position) and label for the text trace */
    var nM = DATA[SPECIES[0]][CONS[0]].length;
    var textY = new Array(nM).fill(0), textLabels = [];
    for (var k = 0; k < nM; k++) {{
      SPECIES.forEach(function (sp) {{
        if (aSp || cSp[sp]) {{
          CONS.forEach(function (con) {{
            if (aCon || cCon[con]) textY[k] += DATA[sp][con][k];
          }});
        }}
      }});
      textLabels.push(textY[k] > 0 ? String(textY[k]) : '');
    }}
    Plotly.restyle(gd, {{y: [textY], text: [textLabels]}}, [N_SPECIES]);
  }};

  /* Date range: zoom x-axis via Plotly.relayout + BroadcastChannel full-state sync */
  var _ch = null;
  try {{ _ch = new BroadcastChannel('ccfn-filter'); }} catch(e) {{}}
  var sinceEl = document.getElementById('ccfn-since');
  var untilEl = document.getElementById('ccfn-until');
  var dateAllEl = document.getElementById('ccfn-date-all');
  function applyDateRange() {{
    var d1str = sinceEl && sinceEl.value ? sinceEl.value : null;
    var d2str = untilEl && untilEl.value ? untilEl.value : null;
    if (d1str && d2str) {{
      var d1 = new Date(d1str + '-01'); d1.setDate(d1.getDate() - 16);
      var d2 = new Date(d2str + '-01'); d2.setMonth(d2.getMonth() + 1); d2.setDate(d2.getDate() + 15);
      Plotly.relayout(gd, {{'xaxis.range': [d1.toISOString().slice(0,10), d2.toISOString().slice(0,10)]}});
    }} else {{
      Plotly.relayout(gd, {{'xaxis.autorange': true}});
    }}
  }}
  window._ccfnBroadcast = function () {{
    if (!_ch) return;
    try {{
      _ch.postMessage({{
        since: sinceEl ? sinceEl.value : '',
        until: untilEl ? untilEl.value : '',
        conservancy: Array.from(document.querySelectorAll('.ccfn-con:checked')).map(function(cb){{return cb.value;}}),
        species:     Array.from(document.querySelectorAll('.ccfn-sp:checked')).map(function(cb){{return cb.value;}})
      }});
    }} catch(e) {{}}
  }};
  function onDR(broadcast) {{
    applyDateRange();
    window._ccfnOnFilter();
    if (broadcast) window._ccfnBroadcast();
  }}
  if (sinceEl) sinceEl.addEventListener('change', function () {{ onDR(true); }});
  if (untilEl) untilEl.addEventListener('change', function () {{ onDR(true); }});
  if (dateAllEl) dateAllEl.addEventListener('click', function () {{
    if (sinceEl) sinceEl.value = '{min_month}';
    if (untilEl) untilEl.value = '{max_month}';
    onDR(true);
  }});
  if (_ch) _ch.onmessage = function (evt) {{
    if (sinceEl && evt.data.since !== undefined) sinceEl.value = evt.data.since;
    if (untilEl && evt.data.until !== undefined) untilEl.value = evt.data.until;
    if (evt.data.conservancy) {{
      var conSet = {{}}; evt.data.conservancy.forEach(function(v){{conSet[v]=1;}});
      document.querySelectorAll('.ccfn-con').forEach(function(cb){{cb.checked=!!conSet[cb.value];}});
    }}
    if (evt.data.species) {{
      var spSet = {{}}; evt.data.species.forEach(function(v){{spSet[v]=1;}});
      document.querySelectorAll('.ccfn-sp').forEach(function(cb){{cb.checked=!!spSet[cb.value];}});
    }}
    applyDateRange();
    window._ccfnOnFilter();
  }};
}})();
</script>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{_SIDEBAR_CSS}</head>
<body>
<div id="ccfn-layout">
  <div id="ccfn-sidebar">
    {date_section}
    {con_section}
    {sp_section}
  </div>
  <div id="ccfn-plot">{chart_frag}</div>
</div>
{chart_filter_js}
{_PANEL_JS}
</body>
</html>"""


# ── draw_patrol_map ───────────────────────────────────────────────────────────

@register(
    description=(
        "Render patrol effort tracks on an interactive map. "
        "A floating panel lets users filter by conservancy; a date-range panel "
        "at the bottom-left filters tracks by patrol start month."
    )
)
def draw_patrol_map(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="Patrol GeoDataFrame (LineString geometry, conservancy, query_name, Start_Date, Transport Mode, Members)"),
    ],
    boundary: Annotated[
        Optional[AnyGeoDataFrame],
        Field(
            default=None,
            description=(
                "Optional conservancy boundary polygons from estimate_conservancy_boundary. "
                "Each row must have a 'name' column. Displayed as a static light-blue reference layer."
            ),
        ),
    ] = None,
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full date filter span to match other widgets.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Interactive Folium map with patrol polylines filtered by conservancy and date."""
    gdf = geodataframe
    if gdf is None or gdf.empty:
        return "<p>No patrol data available.</p>"

    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    if gdf.empty:
        return "<p>No patrol tracks with valid geometry.</p>"

    con_vals = sorted(gdf["conservancy"].dropna().unique().tolist()) if "conservancy" in gdf.columns else []
    con_colours = _con_cols(con_vals)

    bounds = gdf.total_bounds  # [west, south, east, north]
    sw, ne = [bounds[1], bounds[0]], [bounds[3], bounds[2]]
    centre = [(sw[0] + ne[0]) / 2, (sw[1] + ne[1]) / 2]
    m = folium.Map(location=centre, tiles="CartoDB positron")
    m.fit_bounds([sw, ne], padding_top_left=[40, 40], padding_bottom_right=[40, 40])

    # Per-conservancy boundary layers (filterable)
    boundary_meta_pm: list[dict] = []
    if boundary is not None and not boundary.empty:
        bwgs = boundary.to_crs(4326)
        bwgs = bwgs[bwgs.geometry.notna() & ~bwgs.geometry.is_empty]
        for _, brow in bwgs.iterrows():
            con_name = str(brow.get("name") or "Unknown")
            gj = folium.GeoJson(
                brow.geometry.__geo_interface__,
                style_function=lambda _f: {
                    "fillColor": "#4a90d9",
                    "fillOpacity": 0.15,
                    "color": "#4a90d9",
                    "weight": 2,
                    "dashArray": "6 4",
                },
                tooltip=con_name,
            )
            gj.add_to(m)
            boundary_meta_pm.append({"name": gj.get_name(), "con": con_name})

    # Add each polyline directly to the map; record metadata for per-polyline JS filtering
    patrol_meta: list[dict] = []
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        con = str(row.get("conservancy") or "Unknown")
        color = con_colours.get(con, "#888888")

        if geom.geom_type == "LineString":
            coords = [[c[1], c[0]] for c in geom.coords]
        elif geom.geom_type == "MultiLineString":
            coords = [[c[1], c[0]] for line in geom.geoms for c in line.coords]
        else:
            continue

        date_val = row.get("Start_Date", "")
        date_str = str(date_val.date()) if hasattr(date_val, "date") else str(date_val or "")
        month_str = date_val.strftime("%Y-%m") if hasattr(date_val, "strftime") else ""
        transport = str(row.get("Transport Mode") or "")
        members = str(row.get("Members") or "")
        query_name = str(row.get("query_name") or "")

        popup_lines = [f"<b>Conservancy:</b> {con}"]
        if query_name:
            popup_lines.append(f"<b>Query:</b> {query_name}")
        if date_str:
            popup_lines.append(f"<b>Date:</b> {date_str}")
        if transport:
            popup_lines.append(f"<b>Transport:</b> {transport}")
        if members:
            popup_lines.append(f"<b>Members:</b> {members}")
        popup_html = "<br>".join(popup_lines)

        tooltip_text = query_name if query_name else con
        pl = folium.PolyLine(
            coords,
            color=color,
            weight=3,
            opacity=0.85,
            tooltip=tooltip_text,
            popup=folium.Popup(popup_html, max_width=250),
        )
        pl.add_to(m)
        patrol_meta.append({"name": pl.get_name(), "con": con, "month": month_str})

    map_var = m.get_name()
    html = m.get_root().render()

    # Date range for the sidebar date-filter section
    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
    else:
        months = [pm["month"] for pm in patrol_meta if pm["month"]]
        min_month = min(months) if months else ""
        max_month = max(months) if months else ""

    # Build sidebar (same structure as chart tasks)
    date_section = _date_range_sidebar_html(min_month, max_month) if min_month else ""
    con_section  = _sidebar_section("ccfn-con-sec", "Conservancy", "ccfn-con", con_vals, colours=con_colours)
    sidebar_html = (
        '<div id="ccfn-sidebar">'
        + (date_section or "")
        + con_section
        + "</div>"
    )

    map_layout_css = """\
<style>
html, body { margin:0 !important; padding:0 !important; height:100% !important; overflow:hidden !important; }
body { display:flex !important; flex-direction:row !important; height:100vh !important; align-items:stretch !important; }
.folium-map { flex:1 !important; height:100vh !important; min-width:0 !important; }
</style>"""

    # Per-conservancy bounding boxes for auto-zoom
    boundary_wgs_pm = boundary.to_crs(4326) if (boundary is not None and not boundary.empty) else None
    bbox_dict, full_bounds = _conservancy_bboxes(boundary_wgs_pm, fallback_gdf=gdf)
    zoom_js = _zoom_js(map_var, bbox_dict, full_bounds)

    patrol_meta_json = json.dumps(patrol_meta)
    boundary_meta_pm_json = json.dumps(boundary_meta_pm)
    map_filter_js = f"""\
<script>
var PATROL_META = {patrol_meta_json};
var BOUNDARY_META_PM = {boundary_meta_pm_json};
var _map = window['{map_var}'];

window._ccfnOnFilter = function () {{
  var cCon = {{}};
  document.querySelectorAll('.ccfn-con:checked').forEach(function (cb) {{ cCon[cb.value] = 1; }});
  var aCon = Object.keys(cCon).length === document.querySelectorAll('.ccfn-con').length;
  var sinceEl = document.getElementById('ccfn-since');
  var untilEl = document.getElementById('ccfn-until');
  var since = sinceEl ? sinceEl.value : '';
  var until = untilEl ? untilEl.value : '';
  PATROL_META.forEach(function (meta) {{
    var pl = window[meta.name];
    if (!pl) return;
    var conOk = aCon || !!cCon[meta.con];
    var monthOk = true;
    if (meta.month) {{
      if (since && meta.month < since) monthOk = false;
      if (until && meta.month > until) monthOk = false;
    }}
    if (conOk && monthOk) {{
      if (!_map.hasLayer(pl)) pl.addTo(_map);
    }} else {{
      if (_map.hasLayer(pl)) _map.removeLayer(pl);
    }}
  }});
  BOUNDARY_META_PM.forEach(function (meta) {{
    var layer = window[meta.name];
    if (!layer || !_map) return;
    if (aCon || !!cCon[meta.con]) {{
      if (!_map.hasLayer(layer)) layer.addTo(_map);
    }} else {{
      if (_map.hasLayer(layer)) _map.removeLayer(layer);
    }}
  }});
  if (typeof window._ccfnZoom === 'function') window._ccfnZoom(cCon, aCon);
}};
</script>"""

    date_bc_js = _bc_js(min_month, max_month, has_species=False)

    inject_head = "\n".join(filter(None, [_SIDEBAR_CSS, map_layout_css]))
    inject_js   = "\n".join(filter(None, [zoom_js, map_filter_js, date_bc_js, _PANEL_JS]))
    html = html.replace("</head>", inject_head + "\n</head>", 1)
    html = re.sub(r'(<body[^>]*>)', lambda m2: m2.group(0) + "\n" + sidebar_html, html, count=1)
    return html.replace("</html>", inject_js + "\n</html>")


# ── draw_patrol_chart ─────────────────────────────────────────────────────────

@register(
    description=(
        "Render a monthly bar chart of patrol effort stacked by conservancy. "
        "A sidebar lets users filter by conservancy; colours match those used in the patrol map. "
        "A floating km total appears above each bar; a date-range section zooms the x-axis."
    )
)
def draw_patrol_chart(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="Patrol GeoDataFrame (Start_Date, conservancy)"),
    ],
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full x-axis span so all widgets share the same date axis.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Stacked bar chart (x=month, colour=conservancy) with conservancy + date filter sidebar."""
    gdf = geodataframe
    if gdf is None or gdf.empty:
        return "<p>No patrol data available.</p>"

    # Compute per-patrol km before dropping geometry
    gdf_m = gdf.to_crs(epsg=32734)
    df = pd.DataFrame(gdf.drop(columns="geometry", errors="ignore"))
    df["_length_km"] = gdf_m.geometry.length.values / 1000

    required = {"Start_Date", "conservancy"}
    if not required.issubset(df.columns):
        return f"<p>Required columns not found: {required - set(df.columns)}</p>"

    df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce")
    df = df.dropna(subset=["Start_Date", "conservancy"])
    df["month"] = df["Start_Date"].dt.to_period("M").dt.to_timestamp()

    con_vals = sorted(df["conservancy"].unique().tolist())
    con_colours = _con_cols(con_vals)

    # Full month spine: use the configured time range when available so empty
    # months still appear on the x-axis (keeping all chart widgets in sync).
    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
        all_months = [
            p.to_timestamp()
            for p in pd.period_range(time_range.since, time_range.until, freq="M")
        ]
    else:
        min_month = df["Start_Date"].min().strftime("%Y-%m")
        max_month = df["Start_Date"].max().strftime("%Y-%m")
        all_months = sorted(df["month"].unique().tolist())
    month_keys = [m.strftime("%Y-%m-%d") for m in all_months]

    count_agg = df.groupby(["month", "conservancy"]).size().reset_index(name="count")
    dist_agg  = df.groupby(["month", "conservancy"])["_length_km"].sum().reset_index()

    # count_matrix[con] = patrol counts per month (bar heights)
    # dist_matrix[con]  = km totals per month (for annotation labels)
    count_matrix: dict[str, list[int]]   = {}
    dist_matrix:  dict[str, list[float]] = {}
    for con in con_vals:
        c_sub = count_agg[count_agg["conservancy"] == con]
        d_sub = dist_agg[dist_agg["conservancy"] == con]
        c_lkp = {r["month"].strftime("%Y-%m-%d"): int(r["count"]) for _, r in c_sub.iterrows()}
        d_lkp = {r["month"].strftime("%Y-%m-%d"): float(r["_length_km"]) for _, r in d_sub.iterrows()}
        count_matrix[con] = [c_lkp.get(mk, 0)   for mk in month_keys]
        dist_matrix[con]  = [d_lkp.get(mk, 0.0) for mk in month_keys]

    n_months = len(all_months)
    total_count_init = [sum(count_matrix[c][i] for c in con_vals) for i in range(n_months)]
    total_km_init    = [sum(dist_matrix[c][i]  for c in con_vals) for i in range(n_months)]

    fig = go.Figure()
    for con in con_vals:
        fig.add_trace(go.Bar(
            x=all_months,
            y=count_matrix[con],
            name=con,
            marker_color=con_colours[con],
        ))

    # Transparent scatter trace that sits at the top of each stack and shows km total
    fig.add_trace(go.Scatter(
        x=all_months,
        y=total_count_init,
        mode="text",
        text=[f"{v:.0f} km" for v in total_km_init],
        textposition="top center",
        textfont=dict(size=10, color="#444"),
        showlegend=False,
        hoverinfo="none",
    ))

    fig.update_layout(
        barmode="stack",
        title="Patrol Effort by Month",
        xaxis_title="Month",
        yaxis_title="Patrols",
        showlegend=False,
        margin=dict(l=60, r=30, t=60, b=40),
        autosize=True,
        height=None,
    )

    chart_frag = fig.to_html(full_html=False, include_plotlyjs=True,
                              default_width="100%", default_height="100%")

    match = re.search(r'Plotly\.newPlot\s*\(\s*["\']([^"\']+)["\']', chart_frag)
    div_id = match.group(1) if match else "ccfn-patrol-chart"

    date_section = _date_range_sidebar_html(min_month, max_month)
    con_section  = _sidebar_section("ccfn-con-sec", "Conservancy", "ccfn-con", con_vals, colours=con_colours)

    count_matrix_json = json.dumps(count_matrix)
    dist_matrix_json  = json.dumps(dist_matrix)
    con_vals_json     = json.dumps(con_vals)

    chart_filter_js = f"""\
<script>
(function () {{
  var COUNT = {count_matrix_json};
  var DIST  = {dist_matrix_json};
  var CONS  = {con_vals_json};
  var N_CONS = CONS.length;          /* km text trace is at index N_CONS */
  var gd    = document.getElementById('{div_id}');

  window._ccfnOnFilter = function () {{
    var cCon = {{}};
    document.querySelectorAll('.ccfn-con:checked').forEach(function (cb) {{ cCon[cb.value] = 1; }});
    var aCon = Object.keys(cCon).length === document.querySelectorAll('.ccfn-con').length;

    var newY = [], newVis = [], barIdx = [];
    CONS.forEach(function (con, i) {{
      var visible = aCon || !!cCon[con];
      newVis.push(visible);
      newY.push(visible ? COUNT[con] : new Array(COUNT[con].length).fill(0));
      barIdx.push(i);
    }});
    Plotly.restyle(gd, {{y: newY, visible: newVis}}, barIdx);

    /* Recalculate total count (y-position) and total km (label) per month */
    var nM = COUNT[CONS[0]].length;
    var textY = [], textLabels = [];
    for (var i = 0; i < nM; i++) {{
      var totCount = 0, totKm = 0;
      CONS.forEach(function (con) {{
        if (aCon || cCon[con]) {{
          totCount += COUNT[con][i];
          totKm   += DIST[con][i];
        }}
      }});
      textY.push(totCount);
      textLabels.push(totKm.toFixed(0) + ' km');
    }}
    Plotly.restyle(gd, {{y: [textY], text: [textLabels]}}, [N_CONS]);
  }};

  /* Date range: zoom x-axis via Plotly.relayout + BroadcastChannel full-state sync */
  var _ch = null;
  try {{ _ch = new BroadcastChannel('ccfn-filter'); }} catch(e) {{}}
  var sinceEl = document.getElementById('ccfn-since');
  var untilEl = document.getElementById('ccfn-until');
  var dateAllEl = document.getElementById('ccfn-date-all');
  function applyDateRange() {{
    var d1str = sinceEl && sinceEl.value ? sinceEl.value : null;
    var d2str = untilEl && untilEl.value ? untilEl.value : null;
    if (d1str && d2str) {{
      var d1 = new Date(d1str + '-01'); d1.setDate(d1.getDate() - 16);
      var d2 = new Date(d2str + '-01'); d2.setMonth(d2.getMonth() + 1); d2.setDate(d2.getDate() + 15);
      Plotly.relayout(gd, {{'xaxis.range': [d1.toISOString().slice(0,10), d2.toISOString().slice(0,10)]}});
    }} else {{
      Plotly.relayout(gd, {{'xaxis.autorange': true}});
    }}
  }}
  window._ccfnBroadcast = function () {{
    if (!_ch) return;
    try {{
      _ch.postMessage({{
        since: sinceEl ? sinceEl.value : '',
        until: untilEl ? untilEl.value : '',
        conservancy: Array.from(document.querySelectorAll('.ccfn-con:checked')).map(function(cb){{return cb.value;}}),
        species: []
      }});
    }} catch(e) {{}}
  }};
  function onDR(broadcast) {{
    applyDateRange();
    window._ccfnOnFilter();
    if (broadcast) window._ccfnBroadcast();
  }}
  if (sinceEl) sinceEl.addEventListener('change', function () {{ onDR(true); }});
  if (untilEl) untilEl.addEventListener('change', function () {{ onDR(true); }});
  if (dateAllEl) dateAllEl.addEventListener('click', function () {{
    if (sinceEl) sinceEl.value = '{min_month}';
    if (untilEl) untilEl.value = '{max_month}';
    onDR(true);
  }});
  if (_ch) _ch.onmessage = function (evt) {{
    if (sinceEl && evt.data.since !== undefined) sinceEl.value = evt.data.since;
    if (untilEl && evt.data.until !== undefined) untilEl.value = evt.data.until;
    if (evt.data.conservancy) {{
      var conSet = {{}}; evt.data.conservancy.forEach(function(v){{conSet[v]=1;}});
      document.querySelectorAll('.ccfn-con').forEach(function(cb){{cb.checked=!!conSet[cb.value];}});
    }}
    applyDateRange();
    window._ccfnOnFilter();
  }};
}})();
</script>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{_SIDEBAR_CSS}</head>
<body>
<div id="ccfn-layout">
  <div id="ccfn-sidebar">
    {date_section}
    {con_section}
  </div>
  <div id="ccfn-plot">{chart_frag}</div>
</div>
{chart_filter_js}
{_PANEL_JS}
</body>
</html>"""


# ── draw_lcc_map ─────────────────────────────────────────────────────────────

def _lcc_cols(labels: list[str]) -> dict[str, str]:
    return {v: _SP_PALETTE[i % len(_SP_PALETTE)] for i, v in enumerate(sorted(labels))}


@register(
    description=(
        "Render land cover change detection polygons on an interactive map. "
        "Displays conservancy boundary polygons as a static light-blue reference layer. "
        "Each detected change polygon is coloured by change type (e.g. Trees → Crops). "
        "A sidebar lets users filter by conservancy and change type; a date-range section "
        "filters polygons by detection month (last day of the reporting window). "
        "The date filter is connected to other CCFN dashboard widgets via BroadcastChannel."
    )
)
def draw_lcc_map(
    lcc_events: Annotated[
        AnyGeoDataFrame,
        Field(
            description=(
                "Land cover change events GeoDataFrame from get_land_cover_change_events. "
                "Expected columns: geometry, conservancy, detected_date, change_label, "
                "from_class, to_class, area_ha, reporting_period, baseline_period."
            )
        ),
    ],
    boundary: Annotated[
        Optional[AnyGeoDataFrame],
        Field(
            default=None,
            description=(
                "Conservancy boundary polygons from estimate_conservancy_boundary. "
                "Each row has a 'name' column. Displayed as a static light-blue reference layer."
            )
        ),
    ] = None,
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full date filter span to match other widgets.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Interactive Folium map of land cover change polygons with conservancy + label + date filters."""
    import json as _json
    from shapely.geometry import mapping as _mapping

    # ── Determine map bounds ─────────────────────────────────────────────────

    has_events = lcc_events is not None and not lcc_events.empty
    has_boundary = boundary is not None and not boundary.empty

    if not has_events and not has_boundary:
        return "<p>No land cover change data available.</p>"

    if has_events:
        events_wgs = lcc_events[
            lcc_events.geometry.notna() & ~lcc_events.geometry.is_empty
        ].copy()
        has_events = not events_wgs.empty

    if has_boundary:
        boundary_wgs = boundary.to_crs(4326)
        boundary_wgs = boundary_wgs[
            boundary_wgs.geometry.notna() & ~boundary_wgs.geometry.is_empty
        ]
        has_boundary = not boundary_wgs.empty

    # Use whichever data source is available for centering
    if has_events:
        bounds = events_wgs.total_bounds
    else:
        bounds = boundary_wgs.total_bounds

    sw  = [bounds[1], bounds[0]]
    ne  = [bounds[3], bounds[2]]
    centre = [(sw[0] + ne[0]) / 2, (sw[1] + ne[1]) / 2]

    m = folium.Map(location=centre, tiles="CartoDB positron")
    m.fit_bounds([sw, ne], padding_top_left=[40, 40], padding_bottom_right=[40, 40])

    # ── Per-conservancy boundary layers (filterable) ─────────────────────────

    boundary_meta_lcc: list[dict] = []
    if has_boundary:
        for _, brow in boundary_wgs.iterrows():
            con_name = str(brow.get("name") or "Unknown")
            gj = folium.GeoJson(
                brow.geometry.__geo_interface__,
                style_function=lambda _f: {
                    "fillColor": "#4a90d9",
                    "fillOpacity": 0.15,
                    "color": "#4a90d9",
                    "weight": 2,
                    "dashArray": "6 4",
                },
                tooltip=con_name,
            )
            gj.add_to(m)
            boundary_meta_lcc.append({"name": gj.get_name(), "con": con_name})

    # ── Change event polygon layers ──────────────────────────────────────────

    lcc_meta: list[dict] = []
    con_vals: list[str] = []
    label_vals: list[str] = []

    if has_events:
        con_vals = sorted(events_wgs["conservancy"].dropna().unique().tolist()) if "conservancy" in events_wgs.columns else []
        label_vals = sorted(events_wgs["change_label"].dropna().unique().tolist()) if "change_label" in events_wgs.columns else []
        label_colours = _lcc_cols(label_vals)

        for _, row in events_wgs.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue

            con = str(row.get("conservancy") or "Unknown")
            label = str(row.get("change_label") or "Unknown")
            color = label_colours.get(label, "#888888")
            area_ha = row.get("area_ha", 0.0)
            rep_period = row.get("reporting_period", "")
            base_period = row.get("baseline_period", "")

            # detected_date is a datetime.date; format as YYYY-MM for month filtering
            det_date = row.get("detected_date", None)
            if hasattr(det_date, "strftime"):
                month_str = det_date.strftime("%Y-%m")
                date_display = det_date.strftime("%d %b %Y")
            else:
                month_str = str(det_date or "")[:7]
                date_display = str(det_date or "")

            popup_html = (
                f"<b>{label}</b>"
                f"<br><span style='color:#666'>Conservancy:</span> {con}"
                f"<br><span style='color:#666'>Area:</span> {area_ha:.2f} ha"
                f"<br><span style='color:#666'>Detected:</span> {date_display}"
                f"<br><span style='color:#666'>Reporting:</span> {rep_period}"
                f"<br><span style='color:#666'>Baseline:</span> {base_period}"
            )

            gj = folium.GeoJson(
                {"type": "Feature", "geometry": _mapping(geom), "properties": {}},
                style_function=lambda _f, c=color: {
                    "fillColor": c,
                    "fillOpacity": 0.65,
                    "color": c,
                    "weight": 1.0,
                },
                tooltip=f"{label} — {area_ha:.2f} ha",
                popup=folium.Popup(popup_html, max_width=280),
            )
            gj.add_to(m)
            # Bounding box for table-row → map zoom (Leaflet fitBounds format)
            w, s, e, n = geom.bounds
            lcc_meta.append({
                "name": gj.get_name(),
                "con": con,
                "label": label,
                "month": month_str,
                "bbox": [[round(s, 6), round(w, 6)], [round(n, 6), round(e, 6)]],
            })

    map_var = m.get_name()
    html = m.get_root().render()

    # ── Date range ───────────────────────────────────────────────────────────

    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
    else:
        months = [item["month"] for item in lcc_meta if item["month"]]
        min_month = min(months) if months else ""
        max_month = max(months) if months else ""

    # ── Sidebar ──────────────────────────────────────────────────────────────

    date_section  = _date_range_sidebar_html(min_month, max_month) if min_month else ""
    con_section   = _sidebar_section("ccfn-con-sec",   "Conservancy",   "ccfn-con",   con_vals,   colours=None)
    label_section = _sidebar_section("ccfn-lbl-sec",   "Change Type",   "ccfn-lbl",   label_vals, colours=_lcc_cols(label_vals) if label_vals else None, collapsible=True)

    sidebar_html = (
        '<div id="ccfn-sidebar">'
        + (date_section or "")
        + con_section
        + label_section
        + "</div>"
    )

    # ── Map layout CSS ───────────────────────────────────────────────────────

    map_layout_css = """\
<style>
html, body { margin:0 !important; padding:0 !important; height:100% !important; overflow:hidden !important; }
body { display:flex !important; flex-direction:row !important; height:100vh !important; align-items:stretch !important; }
.folium-map { flex:1 !important; height:100vh !important; min-width:0 !important; }
</style>"""

    # ── Filter JS ───────────────────────────────────────────────────────────

    # Per-conservancy bounding boxes for auto-zoom
    boundary_wgs_lcc = boundary_wgs if has_boundary else None
    bbox_dict, full_bounds = _conservancy_bboxes(boundary_wgs_lcc)
    zoom_js = _zoom_js(map_var, bbox_dict, full_bounds)

    lcc_meta_json = _json.dumps(lcc_meta)
    boundary_meta_lcc_json = _json.dumps(boundary_meta_lcc)
    map_filter_js = f"""\
<script>
var LCC_META = {lcc_meta_json};
var BOUNDARY_META_LCC = {boundary_meta_lcc_json};
var _map = window['{map_var}'];

window._ccfnOnFilter = function () {{
  var cCon = {{}}, cLbl = {{}};
  document.querySelectorAll('.ccfn-con:checked').forEach(function (cb) {{ cCon[cb.value] = 1; }});
  document.querySelectorAll('.ccfn-lbl:checked').forEach(function (cb) {{ cLbl[cb.value] = 1; }});
  var aCon = Object.keys(cCon).length === document.querySelectorAll('.ccfn-con').length;
  var aLbl = Object.keys(cLbl).length === document.querySelectorAll('.ccfn-lbl').length;
  var sinceEl = document.getElementById('ccfn-since');
  var untilEl = document.getElementById('ccfn-until');
  var since = sinceEl ? sinceEl.value : '';
  var until = untilEl ? untilEl.value : '';
  LCC_META.forEach(function (meta) {{
    var layer = window[meta.name];
    if (!layer) return;
    var conOk = aCon || !!cCon[meta.con];
    var lblOk = aLbl || !!cLbl[meta.label];
    var monthOk = true;
    if (meta.month) {{
      if (since && meta.month < since) monthOk = false;
      if (until && meta.month > until) monthOk = false;
    }}
    if (conOk && lblOk && monthOk) {{
      if (!_map.hasLayer(layer)) layer.addTo(_map);
    }} else {{
      if (_map.hasLayer(layer)) _map.removeLayer(layer);
    }}
  }});
  BOUNDARY_META_LCC.forEach(function (meta) {{
    var layer = window[meta.name];
    if (!layer || !_map) return;
    if (aCon || !!cCon[meta.con]) {{
      if (!_map.hasLayer(layer)) layer.addTo(_map);
    }} else {{
      if (_map.hasLayer(layer)) _map.removeLayer(layer);
    }}
  }});
  if (typeof window._ccfnZoom === 'function') window._ccfnZoom(cCon, aCon);
}};
</script>"""

    date_bc_js = _bc_js(min_month, max_month, has_species=False)

    zoom_receive_js = f"""\
<script>
(function () {{
  var _zch = null;
  try {{ _zch = new BroadcastChannel('ccfn-zoom'); }} catch(e) {{}}
  var _map_lcc = window['{map_var}'];
  if (_zch) _zch.onmessage = function (evt) {{
    if (evt.data.zoom_bbox && _map_lcc) {{
      _map_lcc.fitBounds(evt.data.zoom_bbox, {{padding: [40, 40], animate: true}});
    }}
  }};
}})();
</script>"""

    # ── Inject into HTML ─────────────────────────────────────────────────────

    inject_head = "\n".join(filter(None, [_SIDEBAR_CSS, map_layout_css]))
    inject_js   = "\n".join(filter(None, [zoom_js, map_filter_js, date_bc_js, zoom_receive_js, _PANEL_JS]))
    html = html.replace("</head>", inject_head + "\n</head>", 1)
    html = re.sub(r'(<body[^>]*>)', lambda m2: m2.group(0) + "\n" + sidebar_html, html, count=1)
    return html.replace("</html>", inject_js + "\n</html>")


# ── draw_lcc_stat ─────────────────────────────────────────────────────────────

@register(
    description=(
        "Render a stat card showing total land cover change area in hectares. "
        "The figure updates dynamically when conservancy or date range filters change "
        "via the ccfn-filter BroadcastChannel, staying in sync with all other CCFN widgets."
    )
)
def draw_lcc_stat(
    lcc_events: Annotated[
        AnyGeoDataFrame,
        Field(
            description=(
                "Land cover change events GeoDataFrame from get_land_cover_change_events. "
                "Expected columns: conservancy, detected_date, area_ha."
            )
        ),
    ],
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full date filter span to match other widgets.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Stat card: total LCC area in ha, updated by ccfn-filter BroadcastChannel."""
    import json as _json

    if lcc_events is None or lcc_events.empty:
        events_data: list[dict] = []
        con_vals: list[str] = []
    else:
        df = pd.DataFrame(lcc_events.drop(columns="geometry", errors="ignore"))
        df["detected_date"] = pd.to_datetime(df["detected_date"], errors="coerce")
        con_vals = sorted(df["conservancy"].dropna().unique().tolist()) if "conservancy" in df.columns else []
        events_data = []
        for _, row in df.iterrows():
            det_date = row.get("detected_date")
            month_str = det_date.strftime("%Y-%m") if pd.notna(det_date) and hasattr(det_date, "strftime") else ""
            events_data.append({
                "con":   str(row.get("conservancy") or ""),
                "month": month_str,
                "area":  float(row.get("area_ha") or 0.0),
            })

    total = sum(e["area"] for e in events_data)
    total_str = f"{total:,.1f}"

    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
    else:
        months = [e["month"] for e in events_data if e["month"]]
        min_month = min(months) if months else ""
        max_month = max(months) if months else ""

    events_json  = _json.dumps(events_data)
    con_vals_json = _json.dumps(con_vals)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
html, body {{
  margin:0; padding:0; height:100%;
  display:flex; align-items:center; justify-content:center;
  font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background:#fff;
}}
#stat-wrap {{ text-align:center; padding:20px; }}
#stat-value {{ font-size:40px; font-weight:700; color:#2c3e50; line-height:1.1; letter-spacing:-1px; }}
#stat-label {{ font-size:13px; color:#888; margin-top:6px; text-transform:uppercase; letter-spacing:0.5px; }}
</style>
</head>
<body>
<div id="stat-wrap">
  <div id="stat-value">{total_str}</div>
  <div id="stat-label">hectares changed</div>
</div>
<script>
(function () {{
  var EVENTS   = {events_json};
  var ALL_CONS = {con_vals_json};
  var valEl    = document.getElementById('stat-value');
  var _ch = null;
  try {{ _ch = new BroadcastChannel('ccfn-filter'); }} catch(e) {{}}

  function recalc(since, until, cons) {{
    var aCon = !cons || cons.length === 0 || cons.length === ALL_CONS.length;
    var total = 0;
    EVENTS.forEach(function (e) {{
      var conOk   = aCon || cons.indexOf(e.con) !== -1;
      var monthOk = true;
      if (e.month) {{
        if (since && e.month < since) monthOk = false;
        if (until && e.month > until) monthOk = false;
      }}
      if (conOk && monthOk) total += e.area;
    }});
    valEl.textContent = total.toLocaleString(undefined, {{minimumFractionDigits:1, maximumFractionDigits:1}});
  }}

  if (_ch) _ch.onmessage = function (evt) {{
    recalc(evt.data.since || '', evt.data.until || '', evt.data.conservancy || null);
  }};
}})();
</script>
</body>
</html>"""


# ── draw_lcc_table ────────────────────────────────────────────────────────────

@register(
    description=(
        "Render land cover change events as an interactive filterable table. "
        "Columns: Conservancy, Detected Date, Change Type, Area (ha), Reporting Period, Baseline Period. "
        "A sidebar filters rows by conservancy and date range, synced with other CCFN widgets via "
        "BroadcastChannel. Clicking a row broadcasts a zoom event on the ccfn-zoom channel so the "
        "Land Cover Change Map pans and zooms to that polygon."
    )
)
def draw_lcc_table(
    lcc_events: Annotated[
        AnyGeoDataFrame,
        Field(
            description=(
                "Land cover change events GeoDataFrame from get_land_cover_change_events. "
                "Expected columns: geometry, conservancy, detected_date, change_label, "
                "area_ha, reporting_period, baseline_period."
            )
        ),
    ],
    time_range: Annotated[
        Optional[TimeRange],
        Field(
            default=None,
            description="Workflow time range — sets the full date filter span to match other widgets.",
        ),
    ] = None,
) -> Annotated[str, Field(description="Self-contained HTML string")]:
    """Filterable HTML table of LCC events with BroadcastChannel sync and row-click map zoom."""
    import json as _json

    if lcc_events is None or lcc_events.empty:
        return "<p>No land cover change data available.</p>"

    df = pd.DataFrame(lcc_events.drop(columns="geometry", errors="ignore"))

    required = {"conservancy", "detected_date", "change_label", "area_ha"}
    if not required.issubset(df.columns):
        return f"<p>Required columns not found: {required - set(df.columns)}</p>"

    df["detected_date"] = pd.to_datetime(df["detected_date"], errors="coerce")
    df = df.sort_values("detected_date", ascending=False).reset_index(drop=True)

    con_vals = sorted(lcc_events["conservancy"].dropna().unique().tolist())

    if time_range is not None:
        min_month = time_range.since.strftime("%Y-%m")
        max_month = time_range.until.strftime("%Y-%m")
    else:
        valid_dates = df["detected_date"].dropna()
        min_month = valid_dates.min().strftime("%Y-%m") if not valid_dates.empty else ""
        max_month = valid_dates.max().strftime("%Y-%m") if not valid_dates.empty else ""

    # Build one <tr> per event; embed bbox as data attribute for row-click zoom
    rows_html: list[str] = []
    for i, row in df.iterrows():
        con = str(row.get("conservancy") or "")
        det_date = row.get("detected_date")
        if pd.notna(det_date) and hasattr(det_date, "strftime"):
            month_str   = det_date.strftime("%Y-%m")
            date_display = det_date.strftime("%b %Y")
        else:
            month_str    = ""
            date_display = str(det_date or "")

        label      = str(row.get("change_label") or "")
        area       = float(row.get("area_ha") or 0.0)
        rep_period = str(row.get("reporting_period") or "")
        base_period= str(row.get("baseline_period") or "")

        bbox_attr = ""
        geom = lcc_events.geometry.iloc[i] if i < len(lcc_events) else None
        if geom is not None and not geom.is_empty:
            w, s, e, n = geom.bounds
            bbox_json = _json.dumps([[round(s, 6), round(w, 6)], [round(n, 6), round(e, 6)]])
            bbox_attr = f' data-bbox=\'{bbox_json}\''

        rows_html.append(
            f'<tr data-con="{con}" data-month="{month_str}"{bbox_attr}>'
            f'<td>{con}</td><td>{date_display}</td><td>{label}</td>'
            f'<td style="text-align:right">{area:.2f}</td>'
            f'<td>{rep_period}</td><td>{base_period}</td>'
            f'</tr>'
        )

    total = len(rows_html)
    rows_str = "\n".join(rows_html)
    con_vals_json = _json.dumps(con_vals)

    date_section = _date_range_sidebar_html(min_month, max_month) if min_month else ""
    con_section  = _sidebar_section("ccfn-con-sec", "Conservancy", "ccfn-con", con_vals, colours=None)

    table_extra_css = """\
<style>
#ccfn-table-area {
  flex: 1; min-width: 0; display: flex; flex-direction: column; overflow: hidden;
}
#ccfn-row-count {
  font-size: 11px; color: #888; padding: 5px 12px;
  border-bottom: 1px solid #e8e8e8; background: #fafafa; flex-shrink: 0;
}
#ccfn-table-wrapper { flex: 1; overflow-y: auto; }
#ccfn-tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
#ccfn-tbl thead th {
  position: sticky; top: 0; background: #f4f6fb; z-index: 1;
  text-align: left; padding: 7px 10px;
  border-bottom: 2px solid #ddd; font-weight: 600; color: #333; white-space: nowrap;
}
#ccfn-tbl tbody td { padding: 5px 10px; border-bottom: 1px solid #f0f0f0; color: #444; }
#ccfn-tbl tbody tr[data-bbox] { cursor: pointer; }
#ccfn-tbl tbody tr[data-bbox]:hover td { background: #e8f0fc; }
#ccfn-tbl tbody tr.ccfn-selected td { background: #c8d8f5; }
#ccfn-tbl tbody tr.ccfn-row-hidden { display: none; }
</style>"""

    filter_js = f"""\
<script>
(function () {{
  var _ch  = null, _zch = null;
  try {{ _ch  = new BroadcastChannel('ccfn-filter'); }} catch(e) {{}}
  try {{ _zch = new BroadcastChannel('ccfn-zoom');   }} catch(e) {{}}

  var sinceEl   = document.getElementById('ccfn-since');
  var untilEl   = document.getElementById('ccfn-until');
  var dateAllEl = document.getElementById('ccfn-date-all');
  var countEl   = document.getElementById('ccfn-row-count');
  var tbody     = document.querySelector('#ccfn-tbl tbody');

  window._ccfnOnFilter = function () {{
    var cCon = {{}};
    document.querySelectorAll('.ccfn-con:checked').forEach(function (cb) {{ cCon[cb.value] = 1; }});
    var aCon = Object.keys(cCon).length === document.querySelectorAll('.ccfn-con').length;
    var since = sinceEl ? sinceEl.value : '';
    var until = untilEl ? untilEl.value : '';
    var visible = 0;
    tbody.querySelectorAll('tr').forEach(function (row) {{
      var conOk = aCon || !!cCon[row.dataset.con];
      var monthOk = true;
      if (row.dataset.month) {{
        if (since && row.dataset.month < since) monthOk = false;
        if (until && row.dataset.month > until) monthOk = false;
      }}
      if (conOk && monthOk) {{ row.classList.remove('ccfn-row-hidden'); visible++; }}
      else                  {{ row.classList.add('ccfn-row-hidden'); }}
    }});
    if (countEl) countEl.textContent = visible + ' of {total} events';
  }};

  window._ccfnBroadcast = function () {{
    if (!_ch) return;
    try {{
      _ch.postMessage({{
        since: sinceEl ? sinceEl.value : '',
        until: untilEl ? untilEl.value : '',
        conservancy: Array.from(document.querySelectorAll('.ccfn-con:checked')).map(function(cb){{return cb.value;}}),
        species: []
      }});
    }} catch(e) {{}}
  }};

  function onDR(broadcast) {{
    window._ccfnOnFilter();
    if (broadcast) window._ccfnBroadcast();
  }}

  if (sinceEl) sinceEl.addEventListener('change', function () {{ onDR(true); }});
  if (untilEl) untilEl.addEventListener('change', function () {{ onDR(true); }});
  if (dateAllEl) dateAllEl.addEventListener('click', function () {{
    if (sinceEl) sinceEl.value = '{min_month}';
    if (untilEl) untilEl.value = '{max_month}';
    onDR(true);
  }});

  /* Row click → zoom LCC map */
  tbody.addEventListener('click', function (evt) {{
    var row = evt.target.closest('tr[data-bbox]');
    if (!row) return;
    tbody.querySelectorAll('tr.ccfn-selected').forEach(function (r) {{ r.classList.remove('ccfn-selected'); }});
    row.classList.add('ccfn-selected');
    if (_zch) {{
      try {{ _zch.postMessage({{ zoom_bbox: JSON.parse(row.dataset.bbox) }}); }} catch(e) {{}}
    }}
  }});

  /* Receive broadcast from other widgets */
  if (_ch) _ch.onmessage = function (evt) {{
    if (sinceEl && evt.data.since !== undefined) sinceEl.value = evt.data.since;
    if (untilEl && evt.data.until !== undefined) untilEl.value = evt.data.until;
    if (evt.data.conservancy) {{
      var conSet = {{}};
      evt.data.conservancy.forEach(function (v) {{ conSet[v] = 1; }});
      document.querySelectorAll('.ccfn-con').forEach(function (cb) {{ cb.checked = !!conSet[cb.value]; }});
    }}
    window._ccfnOnFilter();
  }};

  window._ccfnOnFilter();
}})();
</script>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{_SIDEBAR_CSS}{table_extra_css}</head>
<body>
<div id="ccfn-layout">
  <div id="ccfn-sidebar">
    {date_section}
    {con_section}
  </div>
  <div id="ccfn-table-area">
    <div id="ccfn-row-count">{total} of {total} events</div>
    <div id="ccfn-table-wrapper">
      <table id="ccfn-tbl">
        <thead>
          <tr>
            <th>Conservancy</th><th>Detected</th><th>Change</th>
            <th style="text-align:right">Area (ha)</th>
            <th>Reporting Period</th><th>Baseline Period</th>
          </tr>
        </thead>
        <tbody>
{rows_str}
        </tbody>
      </table>
    </div>
  </div>
</div>
{filter_js}
{_PANEL_JS}
</body>
</html>"""


# ── Boundary estimation ───────────────────────────────────────────────────────

def _extract_vertices_m(geom_series_m) -> "list[tuple[float, float]]":
    """Extract all coordinate vertices (in metric CRS) from a GeoSeries.

    Handles Point, MultiPoint, LineString, MultiLineString, Polygon, and their
    Multi variants.  Returns a flat list of (x, y) tuples.
    """
    from shapely.geometry import (
        LineString, MultiLineString, MultiPoint, MultiPolygon,
        Point, Polygon,
    )

    pts: list[tuple[float, float]] = []
    for geom in geom_series_m:
        if geom is None or geom.is_empty:
            continue
        if isinstance(geom, Point):
            pts.append((geom.x, geom.y))
        elif isinstance(geom, MultiPoint):
            pts.extend((g.x, g.y) for g in geom.geoms)
        elif isinstance(geom, LineString):
            pts.extend(geom.coords)
        elif isinstance(geom, MultiLineString):
            for line in geom.geoms:
                pts.extend(line.coords)
        elif isinstance(geom, Polygon):
            pts.extend(geom.exterior.coords)
        elif isinstance(geom, MultiPolygon):
            for poly in geom.geoms:
                pts.extend(poly.exterior.coords)
    return pts


def _outlier_aware_hull(pts_m: "list[tuple[float, float]]", percentile: float):
    """Return a convex hull of the inlier points (within `percentile`-th distance from centroid).

    Works in metric coordinates.  Returns a shapely geometry.
    """
    import numpy as np
    from shapely.geometry import MultiPoint

    if not pts_m:
        return None

    arr = np.array(pts_m)
    centroid = arr.mean(axis=0)
    dists = np.linalg.norm(arr - centroid, axis=1)
    threshold = np.percentile(dists, percentile)
    inliers = arr[dists <= threshold]

    if len(inliers) < 3:
        # Not enough inliers to form a hull — fall back to all points
        inliers = arr

    return MultiPoint([tuple(p) for p in inliers]).convex_hull


@register(
    description=(
        "Estimate conservancy boundary polygons from the spatial footprint of "
        "observation or patrol data. Groups input rows by the 'conservancy' column "
        "and computes a separate outlier-aware convex hull per group, then expands each "
        "hull outward by the specified buffer distance. "
        "Outlier filtering: only vertices within the hull_percentile-th percentile of "
        "distance from the group centroid contribute to the hull — GPS glitches and "
        "stray tracks beyond that threshold are excluded. "
        "Works with point (observation) and linestring (patrol track) geometries. "
        "Returns one boundary polygon per conservancy with a 'name' column, suitable "
        "as input for get_land_cover_change_events. "
        "Falls back to a single combined hull if the input has no 'conservancy' column."
    )
)
def estimate_conservancy_boundary(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(
            description=(
                "Observation or patrol GeoDataFrame from SMART download tasks. "
                "Both point (observations) and linestring (patrol tracks) geometries are supported."
            )
        ),
    ],
    buffer_km: Annotated[
        float,
        Field(
            default=3.0,
            ge=0.0,
            description=(
                "Distance in kilometres to expand each convex hull outward. "
                "Compensates for rangers not patrolling right up to the actual boundary. "
                "Increase if patrols are concentrated in the interior of the conservancy."
            ),
        ),
    ] = 3.0,
    hull_percentile: Annotated[
        float,
        Field(
            default=95.0,
            ge=50.0,
            le=100.0,
            description=(
                "Percentile of point-to-centroid distances to include in the convex hull. "
                "Points beyond this percentile are treated as outliers (GPS glitches, stray "
                "patrols) and excluded before computing the hull. "
                "95 means the outermost 5% of vertices by distance are ignored. "
                "Set to 100 to disable outlier filtering and use all data."
            ),
        ),
    ] = 95.0,
) -> AnyGeoDataFrame:
    """Derive per-conservancy boundary polygons with outlier-aware hull computation."""
    if geodataframe is None or geodataframe.empty:
        raise ValueError("Cannot estimate boundary: input GeoDataFrame is empty.")

    gdf_m = geodataframe.to_crs(epsg=32734)
    rows = []

    def _build_row(con_name: str, group_m) -> dict | None:
        valid = group_m[~group_m.geometry.is_empty & group_m.geometry.notna()]
        if valid.empty:
            log.warning("Conservancy '%s': no valid geometries, skipping.", con_name)
            return None
        pts = _extract_vertices_m(valid.geometry)
        if not pts:
            log.warning("Conservancy '%s': no vertices extracted, skipping.", con_name)
            return None
        hull = _outlier_aware_hull(pts, hull_percentile)
        if hull is None or hull.is_empty:
            log.warning("Conservancy '%s': hull computation produced empty geometry.", con_name)
            return None
        buffered = hull.buffer(buffer_km * 1000)
        n_total   = len(pts)
        import numpy as np
        arr = np.array(pts)
        centroid = arr.mean(axis=0)
        dists = np.linalg.norm(arr - centroid, axis=1)
        import numpy as _np
        threshold = _np.percentile(dists, hull_percentile)
        n_inliers = int((dists <= threshold).sum())
        log.info(
            "Conservancy '%s': hull from %d / %d vertices (%.0f%% percentile, %.1f km buffer).",
            con_name, n_inliers, n_total, hull_percentile, buffer_km,
        )
        return {"name": con_name, "geometry": buffered}

    if "conservancy" in gdf_m.columns:
        for con_name, group in gdf_m.groupby("conservancy", sort=True):
            row = _build_row(str(con_name), group)
            if row:
                rows.append(row)
    else:
        log.warning(
            "Input GeoDataFrame has no 'conservancy' column; producing a single combined boundary."
        )
        row = _build_row("Conservancy", gdf_m)
        if row:
            rows.append(row)

    if not rows:
        raise ValueError("No valid geometries found; cannot estimate any boundary.")

    result = gpd.GeoDataFrame(rows, crs="EPSG:32734").to_crs("EPSG:4326")
    log.info(
        "Estimated %d conservancy boundary polygon(s). Total area ≈ %.0f km².",
        len(result),
        result.to_crs(epsg=32734).geometry.area.sum() / 1e6,
    )
    return cast(AnyGeoDataFrame, result)
