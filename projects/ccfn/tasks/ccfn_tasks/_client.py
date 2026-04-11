"""
SMART Connect Query API client for CCFN.

Uses the SMART Connect REST API (v7.5.7) with session-based form authentication,
matching the mechanism used by the SMART Connect web UI.

Key API endpoints used:
    GET /api/query/tree          — folder tree with names and nested queries
    GET /api/query/{uuid}        — run a saved query (format=geojson)

Authentication:
    POST /j_security_check       — Java EE form login, returns JSESSIONID cookie
"""
import logging
from datetime import date

import geopandas as gpd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

# Query typeKeys that are expected to return spatial (GeoJSON) data.
# Summary, pivot, and report-style queries are excluded.
SPATIAL_TYPE_KEYS = {
    "patrolobservation",
    "patrolquery",
    "patrol",
    "observation",
    "intelligence",
    "mission",
    "missiontrack",
    "waypoint",
}


class SMARTConnectClient:
    """
    Lightweight client for the SMART Connect Query API.

    Authenticates via the j_security_check form endpoint and maintains
    a requests.Session for all subsequent calls.

    Usage::

        import os
        client = SMARTConnectClient(
            server=os.environ["CCFN_SMART_SERVER"],
            username=os.environ["CCFN_SMART_USERNAME"],
            password=os.environ["CCFN_SMART_PASSWORD"],
        )
        queries = client.get_queries_in_folders(
            folder_names=[
                "Fixed Route Kavango & Zambezi [NRWG]",
                "Central Conservancies Patrol [NRWG]",
            ],
            ca_uuid=os.environ["CCFN_SMART_CA_UUID"],
        )
        gdf = client.run_query_as_geodataframe(queries[0]["uuid"], "2024-01-01", "2024-12-31")
    """

    def __init__(self, server: str, username: str, password: str):
        """
        Args:
            server:   Base server URL (no trailing slash),
                      e.g. https://namibiaconnect.smartconservationtools.org/server
            username: SMART Connect username.
            password: SMART Connect password.
        """
        self._server = server.rstrip("/")
        self._session = requests.Session()
        self._session.verify = False
        self._headers = {"Accept": "application/json"}
        self._authenticate(username, password)

    # ── Authentication ────────────────────────────────────────────────────────

    def _authenticate(self, username: str, password: str) -> None:
        """Establish a session via Java EE form-based authentication."""
        # Pre-request to obtain an initial JSESSIONID
        self._session.get(f"{self._server}/", timeout=10)
        r = self._session.post(
            f"{self._server}/j_security_check",
            data={"j_username": username, "j_password": password},
            timeout=15,
            allow_redirects=False,
        )
        if r.status_code not in (302, 303):
            raise RuntimeError(
                f"SMART Connect authentication failed (HTTP {r.status_code}). "
                "Verify your server URL, username, and password."
            )
        log.info("Authenticated with SMART Connect at %s", self._server)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_json(self, path: str, params: dict | None = None):
        url = f"{self._server}{path}"
        r = self._session.get(url, headers=self._headers, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # ── Query tree ────────────────────────────────────────────────────────────

    def get_query_tree(self, ca_uuid: str | None = None) -> list[dict]:
        """
        Return the full query folder tree.

        Each node in the list is a ``FolderProxyQueryProxy`` dict with keys:
            name (str), items (list[QueryProxy]), subFolders (list[FolderProxy])
        """
        params = {"ca": ca_uuid} if ca_uuid else {}
        return self._get_json("/connect/query/api/tree", params=params)

    def _collect_queries_recursive(
        self, folder: dict, conservancy: str | None = None
    ) -> list[dict]:
        """
        Depth-first collection of all QueryProxy dicts within a folder.
        Tags each query with ``_conservancy`` — the name of the first-level
        subfolder it lives under (i.e. the conservancy name).
        """
        # The conservancy is the first subfolder level under the target folder.
        # Once set, it propagates down to all nested subfolders.
        current_conservancy = conservancy or folder.get("name", "unknown")
        queries = []
        for q in folder.get("items", []):
            q = dict(q)  # shallow copy so we don't mutate the original
            q["_conservancy"] = current_conservancy
            queries.append(q)
        for sub in folder.get("subFolders", []):
            queries.extend(self._collect_queries_recursive(sub, current_conservancy))
        return queries

    def get_queries_in_folders(
        self,
        folder_names: list[str],
        ca_uuid: str | None = None,
        spatial_only: bool = True,
    ) -> list[dict]:
        """
        Return all queries (recursively) from the named top-level folders.

        Each query dict is tagged with ``_conservancy`` — the name of its
        first-level subfolder (e.g. "Bamunu", "Dzoti").

        Args:
            folder_names:  List of top-level folder names to include.
            ca_uuid:       Optional CA UUID to scope the tree query.
            spatial_only:  If True, only return queries whose typeKey is in
                           SPATIAL_TYPE_KEYS (skips summary/pivot queries).

        Returns:
            List of QueryProxy dicts with uuid, name, typeKey, _conservancy.
        """
        tree = self.get_query_tree(ca_uuid=ca_uuid)
        results = []
        for folder in tree:
            if folder.get("name", "") in folder_names:
                # Collect from each conservancy subfolder so the tag is correct
                all_queries = []
                for sub in folder.get("subFolders", []):
                    all_queries.extend(
                        self._collect_queries_recursive(sub, sub.get("name", "unknown"))
                    )
                # Also grab any items directly in the top-level folder
                for q in folder.get("items", []):
                    q = dict(q)
                    q["_conservancy"] = folder.get("name", "unknown")
                    all_queries.append(q)
                if spatial_only:
                    all_queries = [
                        q for q in all_queries
                        if q.get("typeKey", "").lower() in SPATIAL_TYPE_KEYS
                    ]
                log.info(
                    "Folder '%s': %d queries collected%s.",
                    folder["name"],
                    len(all_queries),
                    " (spatial only)" if spatial_only else "",
                )
                results.extend(all_queries)
        return results

    # ── Query execution ───────────────────────────────────────────────────────

    def run_query_as_geodataframe(
        self,
        query_uuid: str,
        start_date: str | date,
        end_date: str | date,
        ca_uuid: str | None = None,
        date_filter: str = "patrolstart",
    ) -> gpd.GeoDataFrame | None:
        """
        Execute a saved query and return the result as a GeoDataFrame.

        Args:
            query_uuid:  UUID of the saved query.
            start_date:  Start of the date window (yyyy-MM-dd or date object).
            end_date:    End of the date window (yyyy-MM-dd or date object).
            ca_uuid:     Optional CA UUID to restrict results to one CA.
            date_filter: The date field to filter on. One of:
                         waypointdate, patrolstart, patrolend,
                         missiontrackdate, missionstartdate, missionenddate.

        Returns:
            GeoDataFrame on success; None if the query is not spatial,
            returned no features, or failed.
        """
        params = {
            "format": "geojson",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "date_filter": date_filter,
        }
        if ca_uuid:
            params["cafilter"] = ca_uuid

        url = f"{self._server}/connect/query/api/{query_uuid}"
        try:
            r = self._session.get(url, params=params, timeout=60)
            r.raise_for_status()

            content_type = r.headers.get("content-type", "")
            if "json" not in content_type:
                log.debug("Query %s: non-JSON response (%s), skipping.", query_uuid, content_type)
                return None

            data = r.json()
            if not isinstance(data, dict) or not data.get("features"):
                return None

            gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
            return gdf if not gdf.empty else None

        except Exception as exc:
            log.debug("Query %s failed: %s", query_uuid, exc)
            return None
