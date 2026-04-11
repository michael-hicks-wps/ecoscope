"""
CCFN SMART Connect — data download and visualisation workflow.

Downloads animal observation and patrol effort data from the Fixed Route
Kavango & Zambezi [NRWG] folder for the last 12 months and writes HTML
outputs to projects/ccfn/outputs/:

    animal_obs_map.html         — point map of all animal observations
    animal_obs_by_month.html    — stacked bar chart: conservancy × month
    patrol_effort_map.html      — line map of all patrol tracks
    patrol_effort_by_month.html — stacked bar chart: conservancy × month

Query selection
---------------
    Animal observations : typeKey=patrolobservation, name contains "All"
                          (one combined query per conservancy)
    Patrol effort       : typeKey=patrolquery, all queries
                          (Casual, Fixed Route, and WC tracks)

Configuration — all values read from environment variables
----------------------------------------------------------
    CCFN_SMART_SERVER    Base server URL
    CCFN_SMART_USERNAME  SMART Connect username
    CCFN_SMART_PASSWORD  SMART Connect password
    CCFN_SMART_CA_UUID   Conservation Area UUID

Run
---
    python -m projects.ccfn.workflows.smart_download
"""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Output directory ──────────────────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent.parent / "outputs"

# ── Target folders ────────────────────────────────────────────────────────────
TARGET_FOLDERS = [
    "Fixed Route Kavango & Zambezi [NRWG]",
]


def _load_config() -> dict:
    required = {
        "server":   "CCFN_SMART_SERVER",
        "username": "CCFN_SMART_USERNAME",
        "password": "CCFN_SMART_PASSWORD",
        "ca_uuid":  "CCFN_SMART_CA_UUID",
    }
    config = {}
    missing = []
    for key, env_var in required.items():
        val = os.environ.get(env_var, "")
        if not val:
            missing.append(env_var)
        config[key] = val
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)
    return config


def _select_queries(all_queries: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Split queries into:
      - obs_queries:    patrolobservation where name contains "all"
      - patrol_queries: all patrolquery entries
    """
    obs_queries    = []
    patrol_queries = []
    for q in all_queries:
        tk   = q.get("typeKey", "").lower()
        name = q.get("name", "").lower()
        if tk == "patrolobservation" and "all" in name:
            obs_queries.append(q)
        elif tk == "patrolquery":
            patrol_queries.append(q)
    return obs_queries, patrol_queries


def _run_queries(
    client,
    queries: list[dict],
    start_date: str,
    end_date: str,
    ca_uuid: str,
    label: str,
) -> gpd.GeoDataFrame:
    """Run a list of queries and return a single combined GeoDataFrame."""
    gdfs = []
    total = len(queries)
    for i, q in enumerate(queries, 1):
        name         = q.get("name", "unnamed")
        conservancy  = q.get("_conservancy", "unknown")
        log.info("  [%d/%d] %s — %s", i, total, conservancy, name)
        gdf = client.run_query_as_geodataframe(
            q["uuid"], start_date, end_date, ca_uuid=ca_uuid
        )
        if gdf is None or gdf.empty:
            log.info("          → no data, skipped.")
            continue
        gdf["conservancy"] = conservancy
        gdf["query_name"]  = name
        log.info("          → %d features.", len(gdf))
        gdfs.append(gdf)

    if not gdfs:
        return gpd.GeoDataFrame()

    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")
    log.info("%s total: %d features across %d queries.", label, len(combined), len(gdfs))
    return combined


# 13 visually distinct colours for up to 13 conservancies
_CONSERVANCY_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a",
]


def _inject_colorway(html: str, colors: list[str]) -> str:
    """Inject a Plotly colorway into a chart HTML string."""
    colorway_js = (
        "<script>"
        "document.addEventListener('DOMContentLoaded',function(){"
        "var divs=document.querySelectorAll('[id^=\"\"]');"
        f"var cw={colors!r};"
        "Plotly.relayout(document.querySelector('.plotly-graph-div'),{colorway:cw});"
        "});"
        "</script>"
    )
    return html.replace("</body>", colorway_js + "</body>")


# Expected bounding box for the Kavango-Zambezi study area
# Anything outside this is almost certainly a GPS error
STUDY_AREA_BOUNDS = {
    "min_lon": 17.0,
    "max_lon": 27.0,
    "min_lat": -23.0,
    "max_lat": -14.0,
}



def _filter_aberrant_geometry(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    """
    Remove records with clearly erroneous geometry:
      - null or empty geometries
      - point/line coordinates at or near (0, 0)
      - coordinates outside the KAZA study area bounding box
    """
    n_start = len(gdf)

    # 1. Null / empty geometry
    mask = gdf.geometry.notna() & ~gdf.geometry.is_empty
    gdf = gdf[mask]

    # 2. Bounding box check — use the centroid for lines
    centroids = gdf.geometry.centroid
    in_bounds = (
        (centroids.x >= STUDY_AREA_BOUNDS["min_lon"]) &
        (centroids.x <= STUDY_AREA_BOUNDS["max_lon"]) &
        (centroids.y >= STUDY_AREA_BOUNDS["min_lat"]) &
        (centroids.y <= STUDY_AREA_BOUNDS["max_lat"])
    )
    gdf = gdf[in_bounds]

    # 3. Zero / near-zero coordinates (GPS failure)
    centroids = gdf.geometry.centroid
    not_zero = ~((centroids.x.abs() < 0.01) & (centroids.y.abs() < 0.01))
    gdf = gdf[not_zero]

    n_dropped = n_start - len(gdf)
    if n_dropped:
        log.warning(
            "%s: dropped %d of %d records with aberrant geometry.",
            label, n_dropped, n_start,
        )
    return gdf.reset_index(drop=True)


def _filter_patrols_by_length(gdf: gpd.GeoDataFrame, min_km: float = 5.0) -> gpd.GeoDataFrame:
    """Drop patrol tracks shorter than min_km kilometres."""
    # Reproject to UTM zone 34S (EPSG:32734) — covers Namibia/Zambia well
    gdf_m = gdf.to_crs(epsg=32734)
    lengths_km = gdf_m.geometry.length / 1000
    mask = lengths_km >= min_km
    n_dropped = (~mask).sum()
    if n_dropped:
        log.info(
            "Patrol length filter (>= %.1f km): dropped %d of %d patrols.",
            min_km, n_dropped, len(gdf),
        )
    return gdf[mask].reset_index(drop=True)


def _detect_date_column(gdf: gpd.GeoDataFrame) -> str | None:
    candidates = ["Waypoint_Date", "waypoint_date", "Start_Date", "start_date",
                  "date", "datetime", "time", "fixtime"]
    for col in candidates:
        if col in gdf.columns:
            return col
    for col in gdf.columns:
        if "date" in col.lower() or "time" in col.lower():
            return col
    return None


def run() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = _load_config()

    # ── Deferred imports ──────────────────────────────────────────────────────
    from ecoscope.platform.tasks.config._workflow_details import set_workflow_details
    from ecoscope.platform.tasks.results._ecomap import (
        LegendDefinition,
        PointLayerStyle,
        PolylineLayerStyle,
        TileLayer,
        create_point_layer,
        create_polyline_layer,
        draw_ecomap,
    )
    from ecoscope.platform.tasks.results._ecoplot import (
        AxisStyle,
        BarLayoutStyle,
        draw_time_series_bar_chart,
    )
    from projects.ccfn.tasks._smart_connect_client import SMARTConnectClient
    from projects.ccfn.tasks._smart_data import add_count_column, normalize_time_column

    details = set_workflow_details(
        name="CCFN SMART Data Download",
        description=(
            "Download animal observation and patrol effort data from CCFN SMART "
            "Connect for the last 12 months and visualise by conservancy and month."
        ),
    )
    log.info("Workflow: %s", details.name)

    # ── Date range ────────────────────────────────────────────────────────────
    today = datetime.now(tz=timezone.utc).date()
    start = today - timedelta(days=365)
    start_date = f"{start} 00:00:00"
    end_date   = f"{today} 23:59:59"
    log.info("Date range: %s → %s", start_date, end_date)

    # ── Connect ───────────────────────────────────────────────────────────────
    log.info("Connecting to SMART Connect …")
    client = SMARTConnectClient(
        server=cfg["server"],
        username=cfg["username"],
        password=cfg["password"],
    )

    # ── Discover queries ──────────────────────────────────────────────────────
    log.info("Discovering queries …")
    all_queries = client.get_queries_in_folders(
        folder_names=TARGET_FOLDERS,
        ca_uuid=cfg["ca_uuid"],
        spatial_only=True,
    )
    # Tag each query with its conservancy (first-level subfolder name)
    # This is stored by the client during tree traversal
    obs_queries, patrol_queries = _select_queries(all_queries)
    log.info(
        "Selected: %d observation queries, %d patrol queries.",
        len(obs_queries), len(patrol_queries),
    )

    # ── Download ──────────────────────────────────────────────────────────────
    log.info("Downloading animal observations …")
    obs_gdf = _run_queries(
        client, obs_queries, start_date, end_date, cfg["ca_uuid"],
        label="Animal observations",
    )
    if not obs_gdf.empty:
        obs_gdf = _filter_aberrant_geometry(obs_gdf, "Animal observations")

    log.info("Downloading patrol effort …")
    patrol_gdf = _run_queries(
        client, patrol_queries, start_date, end_date, cfg["ca_uuid"],
        label="Patrol effort",
    )
    if not patrol_gdf.empty:
        patrol_gdf = _filter_aberrant_geometry(patrol_gdf, "Patrol effort")
        patrol_gdf = _filter_patrols_by_length(patrol_gdf, min_km=5.0)

    # ═════════════════════════════════════════════════════════════════════════
    # Animal Observations — point map + monthly bar chart
    # ═════════════════════════════════════════════════════════════════════════
    if not obs_gdf.empty:
        date_col = _detect_date_column(obs_gdf)
        tooltip_cols = [c for c in ["conservancy", "query_name", date_col, "Species",
                                     "Observation_Category_0"] if c and c in obs_gdf.columns]

        obs_layer = create_point_layer(
            geodataframe=obs_gdf,
            layer_style=PointLayerStyle(
                get_fill_color=[0, 150, 80, 200],
                get_radius=5,
                radius_units="pixels",
            ),
            legend=LegendDefinition(label_column="conservancy"),
            tooltip_columns=tooltip_cols or None,
            zoom=True,
        )
        obs_map_html = draw_ecomap(
            geo_layers=obs_layer,
            tile_layers=[
                TileLayer(layer_name="SATELLITE", opacity=0.7),
                TileLayer(layer_name="TERRAIN", opacity=0.5),
            ],
            title="CCFN Animal Observations — Last 12 Months",
        )
        out = OUTPUT_DIR / "animal_obs_map.html"

        out.write_text(obs_map_html, encoding="utf-8")
        log.info("Saved: %s", out)

        if date_col:
            chart_df = add_count_column(obs_gdf)
            chart_df = normalize_time_column(chart_df, time_col=date_col)
            chart_df[date_col] = pd.to_datetime(chart_df[date_col], errors="coerce")
            chart_df = chart_df.dropna(subset=[date_col])
            if not chart_df.empty:
                chart_html = draw_time_series_bar_chart(
                    dataframe=chart_df,
                    x_axis=date_col,
                    y_axis="count",
                    category="conservancy",
                    agg_function="sum",
                    time_interval="month",
                    layout_style=BarLayoutStyle(
                        title="Animal Observations by Conservancy and Month",
                        xaxis=AxisStyle(title="Month"),
                        yaxis=AxisStyle(title="Count"),
                    ),
                )
                out = OUTPUT_DIR / "animal_obs_by_month.html"
                out.write_text(_inject_colorway(chart_html, _CONSERVANCY_COLORS), encoding="utf-8")
                log.info("Saved: %s", out)
        else:
            log.warning("No date column found in observations — bar chart skipped.")
    else:
        log.warning("No animal observation data returned.")

    # ═════════════════════════════════════════════════════════════════════════
    # Patrol Effort — line map + monthly bar chart
    # ═════════════════════════════════════════════════════════════════════════
    if not patrol_gdf.empty:
        date_col = _detect_date_column(patrol_gdf)
        tooltip_cols = [c for c in ["conservancy", date_col, "Mandate", "Leader",
                                     "Transport_Mode"] if c and c in patrol_gdf.columns]

        patrol_layer = create_polyline_layer(
            geodataframe=patrol_gdf,
            layer_style=PolylineLayerStyle(
                get_color=[220, 120, 0, 200],
                get_width=2,
                width_units="pixels",
            ),
            legend=LegendDefinition(label_column="conservancy"),
            tooltip_columns=tooltip_cols or None,
            zoom=True,
        )
        patrol_map_html = draw_ecomap(
            geo_layers=patrol_layer,
            tile_layers=[
                TileLayer(layer_name="SATELLITE", opacity=0.7),
                TileLayer(layer_name="TERRAIN", opacity=0.5),
            ],
            title="CCFN Patrol Effort — Last 12 Months",
        )
        out = OUTPUT_DIR / "patrol_effort_map.html"
        out.write_text(patrol_map_html, encoding="utf-8")
        log.info("Saved: %s", out)

        if date_col:
            chart_df = add_count_column(patrol_gdf)
            chart_df = normalize_time_column(chart_df, time_col=date_col)
            chart_df[date_col] = pd.to_datetime(chart_df[date_col], errors="coerce")
            chart_df = chart_df.dropna(subset=[date_col])
            if not chart_df.empty:
                chart_html = draw_time_series_bar_chart(
                    dataframe=chart_df,
                    x_axis=date_col,
                    y_axis="count",
                    category="conservancy",
                    agg_function="sum",
                    time_interval="month",
                    layout_style=BarLayoutStyle(
                        title="Patrol Effort by Conservancy and Month",
                        xaxis=AxisStyle(title="Month"),
                        yaxis=AxisStyle(title="Patrol Count"),
                    ),
                )
                out = OUTPUT_DIR / "patrol_effort_by_month.html"
                out.write_text(_inject_colorway(chart_html, _CONSERVANCY_COLORS), encoding="utf-8")
                log.info("Saved: %s", out)
        else:
            log.warning("No date column found in patrol data — bar chart skipped.")
    else:
        log.warning("No patrol effort data returned.")

    log.info("Done. Outputs in: %s", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    run()
