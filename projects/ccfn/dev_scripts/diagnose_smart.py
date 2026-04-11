"""
Diagnostic script — prints raw API responses for the first query found
in the target folders, to help debug why no spatial data is returned.

Run:
    & "<python>" projects/ccfn/diagnose_smart.py
    Output is written to projects/ccfn/outputs/diagnose_output.txt
"""
import os
import sys
from pathlib import Path

import requests
import urllib3

# ── Redirect all output to a file ────────────────────────────────────────────
OUTPUT_FILE = Path(__file__).parent / "outputs" / "diagnose_output.txt"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
_tee = open(OUTPUT_FILE, "w", encoding="utf-8")

class _Tee:
    def __init__(self, *streams):
        self._streams = streams
    def write(self, data):
        for s in self._streams:
            s.write(data)
    def flush(self):
        for s in self._streams:
            s.flush()

sys.stdout = _Tee(sys.__stdout__, _tee)
sys.stderr = _Tee(sys.__stderr__, _tee)
print(f"Output also saved to: {OUTPUT_FILE}\n")

urllib3.disable_warnings()

SERVER   = os.environ["CCFN_SMART_SERVER"].rstrip("/")
USERNAME = os.environ["CCFN_SMART_USERNAME"]
PASSWORD = os.environ["CCFN_SMART_PASSWORD"]
CA_UUID  = os.environ["CCFN_SMART_CA_UUID"]

TARGET_FOLDERS = [
    "Fixed Route Kavango & Zambezi [NRWG]",
    "Central Conservancies Patrol [NRWG]",
]

DATE_FILTERS = [
    "patrolstart",
    "waypointdate",
    "observationdate",
    "missiontrackdate",
]

session = requests.Session()
session.verify = False

# ── Auth ──────────────────────────────────────────────────────────────────────
print("Authenticating …")
session.get(f"{SERVER}/", timeout=10)
r = session.post(
    f"{SERVER}/j_security_check",
    data={"j_username": USERNAME, "j_password": PASSWORD},
    timeout=15,
    allow_redirects=False,
)
print(f"  Auth response: HTTP {r.status_code}  Location: {r.headers.get('Location', '—')}")
if r.status_code not in (302, 303):
    print("  AUTH FAILED — stopping.")
    sys.exit(1)

# ── Query tree ────────────────────────────────────────────────────────────────
print(f"\nFetching query tree for CA {CA_UUID} …")
r = session.get(
    f"{SERVER}/connect/query/api/tree",
    headers={"Accept": "application/json"},
    params={"ca": CA_UUID},
    timeout=30,
)
print(f"  HTTP {r.status_code}  content-type: {r.headers.get('content-type','')}")

tree = r.json()
print(f"  Top-level folders: {[f.get('name') for f in tree]}")

TARGET_TYPE_KEYS = {"patrolobservation", "patrolquery"}

def collect_all(folder):
    results = list(folder.get("items", []))
    for sub in folder.get("subFolders", []):
        results.extend(collect_all(sub))
    return results

def conservancy_name(folder, target_folder_name):
    """Extract conservancy subfolder name from path."""
    for sub in folder.get("subFolders", []):
        queries = collect_all(sub)
        if queries:
            return sub.get("name", "unknown")
    return "unknown"

# ── Collect and print only target typeKeys ────────────────────────────────────
obs_queries    = []
patrol_queries = []

for folder in tree:
    if folder.get("name") in TARGET_FOLDERS:
        for sub in folder.get("subFolders", []):       # conservancy level
            conservancy = sub.get("name", "?")
            all_q = collect_all(sub)
            for q in all_q:
                tk = q.get("typeKey", "").lower()
                q["_conservancy"] = conservancy
                if tk == "patrolobservation":
                    obs_queries.append(q)
                elif tk == "patrolquery":
                    patrol_queries.append(q)

print(f"\n{'='*60}")
print(f"patrolobservation queries ({len(obs_queries)} total):")
for q in obs_queries:
    print(f"  [{q['_conservancy']}]  {q.get('name')}  uuid={q.get('uuid')}")

print(f"\n{'='*60}")
print(f"patrolquery queries ({len(patrol_queries)} total):")
for q in patrol_queries:
    print(f"  [{q['_conservancy']}]  {q.get('name')}  uuid={q.get('uuid')}")

target_query = obs_queries[0] if obs_queries else None

# Override with a known-good UUID for direct testing
KNOWN_UUID = "2d0e5ba2-4445-405e-8672-f9d168e85bec"
target_query = {"name": "Bamunu-WC-patrolquery", "typeKey": "patrolquery", "uuid": KNOWN_UUID}
print(f"\nOverriding with known UUID: {KNOWN_UUID}")

if not target_query:
    print("\nNo queries found — check folder names match exactly.")
    sys.exit(1)

# ── Try running the query with each date_filter ───────────────────────────────
print(f"\nTest query: '{target_query.get('name')}'  ({target_query.get('typeKey')})")
print(f"UUID: {target_query.get('uuid')}\n")

url = f"{SERVER}/connect/query/api/{target_query['uuid']}"

# Try CSV format (used in the shareable URL)
for df in DATE_FILTERS:
    params = {
        "format": "csv",
        "start_date": "2025-04-09 00:00:00",
        "end_date": "2026-04-09 23:59:59",
        "date_filter": df,
        "cafilter": CA_UUID,
    }
    r = session.get(url, params=params, timeout=60)
    ct = r.headers.get("content-type", "")
    print(f"  [csv] date_filter={df:<20}  HTTP {r.status_code}  content-type: {ct[:50]}")
    if r.status_code == 200 and r.text.strip():
        lines = r.text.strip().splitlines()
        print(f"    → {len(lines)-1} rows")
        print(f"    columns: {lines[0]}")
        for row in lines[1:3]:
            print(f"    sample:  {row[:120]}")
        if len(lines) > 1:
            break
    else:
        print(f"    → {r.text[:200]!r}")

# Try all formats without srid
print()
for fmt in ["geojson", "json", "shapefile"]:
    params = {
        "format": fmt,
        "start_date": "2025-04-09 00:00:00",
        "end_date": "2026-04-09 23:59:59",
        "date_filter": "patrolstart",
        "cafilter": CA_UUID,
    }
    r = session.get(url, params=params, timeout=60)
    ct = r.headers.get("content-type", "")
    print(f"  [format={fmt}]  HTTP {r.status_code}  ct: {ct[:60]}")
    if "json" in ct and r.status_code == 200:
        data = r.json()
        features = data.get("features", []) if isinstance(data, dict) else []
        print(f"    → {len(features)} features")
        if features:
            geom  = features[0].get("geometry", {})
            props = features[0].get("properties", {})
            print(f"    geometry type: {geom.get('type')}  coords: {geom.get('coordinates')}")
            # Print key fields with values
            key_fields = ["ID", "Type", "Start_Date", "Waypoint_Date", "Station",
                          "Team", "Mandate", "Species", "Observation_Category_0",
                          "Observation_Category_1", "Comment"]
            for field in key_fields:
                if field in props:
                    print(f"    {field}: {props[field]}")
            # Also print any non-empty fields we didn't list
            for k, v in props.items():
                if k not in key_fields and v not in (None, "", "null"):
                    print(f"    {k}: {v}")
    elif r.status_code == 200:
        print(f"    → {len(r.content)} bytes")
