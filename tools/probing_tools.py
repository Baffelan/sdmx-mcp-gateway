"""
Query probing and bounded recovery for SDMX data.

These tools address the gap between query validation (syntax/codes are valid)
and query usefulness (the exact query returns observations).
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from config import get_constraint_strategy
from utils import SDMX_NAMESPACES

if TYPE_CHECKING:
    from sdmx_progressive_client import SDMXProgressiveClient

logger = logging.getLogger(__name__)

# Dimension IDs commonly used for geography across SDMX providers
GEO_DIMENSION_IDS = frozenset({
    "GEO", "GEO_PICT", "REF_AREA", "REGION", "COUNTRY", "LOCATION",
    "COUNTERPART_AREA", "ECONOMY",
})

# Dimension IDs commonly used for time
TIME_DIMENSION_IDS = frozenset({
    "TIME_PERIOD", "TIME", "PERIOD",
})


def parse_sdmx_data_url(url: str) -> dict[str, Any] | None:
    """Parse an SDMX data URL into its components.

    Expected pattern: {base}/data/{flow_ref}/{key}?{params}

    Returns None if the URL does not match the expected pattern.
    Returns a dict with keys:
        base_url, dataflow_id, key, key_parts,
        start_period, end_period, params (dict of other query params)
    """
    parsed = urlparse(url)
    path = parsed.path

    # Find "/data/" segment in path
    data_idx = path.find("/data/")
    if data_idx == -1:
        return None

    base_path = path[:data_idx]
    remainder = path[data_idx + len("/data/"):]

    # Split remainder into flow_ref and key
    parts = remainder.split("/", 1)
    if not parts or not parts[0]:
        return None

    dataflow_id = parts[0]
    key = parts[1] if len(parts) > 1 else "all"
    # Strip trailing slash
    key = key.rstrip("/")

    # Parse query parameters
    qs = parse_qs(parsed.query, keep_blank_values=True)
    start_period = qs.get("startPeriod", [None])[0]
    end_period = qs.get("endPeriod", [None])[0]

    # Collect other params (flatten single-value lists)
    other_params: dict[str, str] = {}
    skip_keys = {"startPeriod", "endPeriod"}
    for k, v in qs.items():
        if k not in skip_keys:
            other_params[k] = v[0] if len(v) == 1 else ",".join(v)

    base_url = parsed.scheme + "://" + parsed.netloc + base_path

    key_parts = key.split(".") if key != "all" else ["all"]

    return {
        "base_url": base_url,
        "dataflow_id": dataflow_id,
        "key": key,
        "key_parts": key_parts,
        "start_period": start_period,
        "end_period": end_period,
        "params": other_params,
    }


def normalize_query_fingerprint(url: str) -> str:
    """Compute a deterministic SHA-256 fingerprint of a normalised SDMX data URL.

    Normalisation: lowercase scheme/host, sort query params,
    strip dimensionAtObservation (default value).
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    # Remove params that don't change the result
    for drop in ("dimensionAtObservation",):
        qs.pop(drop, None)

    # Flatten and sort
    flat: list[tuple[str, str]] = []
    for k in sorted(qs.keys()):
        for v in sorted(qs[k]):
            flat.append((k, v))

    normalised = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path,
        "",
        urlencode(flat),
        "",
    ))

    digest = hashlib.sha256(normalised.encode()).hexdigest()[:16]
    return "sha256:" + digest


def build_probe_url(url: str) -> str:
    """Add firstNObservations=1 to an SDMX data URL for lightweight probing.

    If the URL already has firstNObservations, it is left unchanged.
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    if "firstNObservations" not in qs:
        qs["firstNObservations"] = ["1"]

    flat: list[tuple[str, str]] = []
    for k in sorted(qs.keys()):
        for v in qs[k]:
            flat.append((k, v))

    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        "",
        urlencode(flat),
        "",
    ))


def parse_csv_probe_response(
    csv_text: str,
    sample_limit: int = 5,
    max_distinct_per_dim: int = 10,
) -> dict[str, Any]:
    """Parse an SDMX CSV response and return shape metadata.

    Returns a dict with:
        observation_count, series_count, time_period_count,
        dimensions (id -> {distinct_count, sample_values}),
        has_time_dimension, geo_dimension_id,
        sample_observations (list of {dimensions, value}).
    """
    reader = csv.reader(io.StringIO(csv_text))
    try:
        headers = next(reader)
    except StopIteration:
        return _empty_shape()

    headers = [h.strip() for h in headers]

    # Identify special columns
    skip_cols = {"DATAFLOW", "OBS_VALUE", "OBS_STATUS", "OBS_FLAG",
                 "CONF_STATUS", "DECIMALS", "UNIT_MULT", "COMMENT_OBS",
                 "COMMENT_TS"}
    obs_value_idx = headers.index("OBS_VALUE") if "OBS_VALUE" in headers else None

    # Find dimension columns (everything that isn't a skip col)
    dim_indices: list[tuple[int, str]] = []
    for i, h in enumerate(headers):
        if h not in skip_cols:
            dim_indices.append((i, h))

    # Identify time and geo dimensions
    time_dim_id: str | None = None
    geo_dim_id: str | None = None
    for _, h in dim_indices:
        if h in TIME_DIMENSION_IDS:
            time_dim_id = h
        if h in GEO_DIMENSION_IDS:
            geo_dim_id = h

    # Parse rows
    # Sample sets are capped at max_distinct_per_dim; true counts tracked separately
    dim_values: dict[str, set[str]] = {h: set() for _, h in dim_indices}
    dim_true_counts: dict[str, int] = {h: 0 for _, h in dim_indices}
    series_keys: set[tuple[str, ...]] = set()
    time_values: set[str] = set()
    sample_obs: list[dict[str, Any]] = []
    obs_count = 0

    for row in reader:
        if not row or all(c.strip() == "" for c in row):
            continue
        obs_count += 1

        # Collect dimension values
        row_dims: dict[str, str] = {}
        series_parts: list[str] = []
        for idx, dim_id in dim_indices:
            val = row[idx].strip() if idx < len(row) else ""
            row_dims[dim_id] = val
            if val not in dim_values[dim_id]:
                dim_true_counts[dim_id] += 1
                if len(dim_values[dim_id]) < max_distinct_per_dim:
                    dim_values[dim_id].add(val)
            if dim_id != time_dim_id:
                series_parts.append(val)

        series_keys.add(tuple(series_parts))

        if time_dim_id and time_dim_id in row_dims:
            time_values.add(row_dims[time_dim_id])

        # Sample observations
        if len(sample_obs) < sample_limit:
            obs_val: float | None = None
            if obs_value_idx is not None and obs_value_idx < len(row):
                raw = row[obs_value_idx].strip()
                if raw:
                    try:
                        obs_val = float(raw)
                    except ValueError:
                        obs_val = None
            sample_obs.append({"dimensions": row_dims, "value": obs_val})

    # Build dimension summaries (distinct_count uses true count, not capped set)
    dimensions: dict[str, dict[str, Any]] = {}
    for _, dim_id in dim_indices:
        vals = dim_values[dim_id]
        dimensions[dim_id] = {
            "distinct_count": dim_true_counts[dim_id],
            "sample_values": sorted(vals)[:max_distinct_per_dim],
        }

    return {
        "observation_count": obs_count,
        "series_count": len(series_keys),
        "time_period_count": len(time_values),
        "dimensions": dimensions,
        "has_time_dimension": time_dim_id is not None,
        "geo_dimension_id": geo_dim_id,
        "sample_observations": sample_obs,
    }


def _empty_shape() -> dict[str, Any]:
    """Return shape metadata for an empty or unparseable response."""
    return {
        "observation_count": 0,
        "series_count": 0,
        "time_period_count": 0,
        "dimensions": {},
        "has_time_dimension": False,
        "geo_dimension_id": None,
        "sample_observations": [],
    }


# Legacy module-level probe cache. This is a **fallback** for callers that
# don't pass an explicit probe_cache; session-scoped callers (the MCP
# handler in main_server) now pass their SessionState.probe_cache so one
# session's probe results never leak to another. The module-level cache
# stays for tests and direct-call paths.
_probe_cache: dict[str, dict[str, Any]] = {}
_PROBE_CACHE_MAX = 1000


async def probe_data_url(
    client: SDMXProgressiveClient,
    data_url: str | None = None,
    dataflow_id: str | None = None,
    filters: dict[str, str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    agency_id: str | None = None,
    sample_limit: int = 5,
    max_distinct_per_dim: int = 10,
    timeout_ms: int = 10000,
    probe_cache: dict[str, dict[str, Any]] | None = None,
    probe_cache_lock: Any | None = None,
) -> dict[str, Any]:
    """Probe an exact SDMX query and return shape metadata.

    Accepts either a data_url or structured input (dataflow_id + filters).
    If structured input is provided and data_url is None, the URL is built
    from the client's base_url.

    Args:
        agency_id: Owning agency for the dataflow (e.g. OECD sub-agencies like
            "OECD.STI.STP"). Only consulted when data_url is None and the URL
            is built from structured input. Defaults to the client's session
            agency, which is wrong for OECD flows owned by sub-agencies.
        probe_cache: Optional explicit cache dict. Session-scoped callers
            should pass their SessionState.probe_cache so results never
            leak across sessions (audit M1). Defaults to the legacy
            module-level cache, preserving behaviour for tests and
            direct-call paths.
        probe_cache_lock: Optional lock guarding probe_cache. Callers
            threading the SessionState should pass state._state_lock
            here; then every cache read/write is serialised. Safe to
            leave None (legacy callers don't bother).

    Returns a dict matching the ProbeResult schema.
    """
    cache = probe_cache if probe_cache is not None else _probe_cache
    # Build URL from structured input if needed
    if data_url is None:
        if dataflow_id is None:
            return _error_result("Either data_url or dataflow_id is required")
        data_url = await _build_url_from_parts(
            client=client,
            dataflow_id=dataflow_id,
            filters=filters,
            start_period=start_period,
            end_period=end_period,
            agency_id=agency_id,
        )

    fingerprint = normalize_query_fingerprint(data_url)
    cache_key = _make_probe_cache_key(
        fingerprint=fingerprint,
        sample_limit=sample_limit,
        max_distinct_per_dim=max_distinct_per_dim,
    )

    # Check cache
    with _maybe_lock(probe_cache_lock):
        cached = cache.get(cache_key)
    if cached is not None:
        return cached

    availability = await _preflight_with_availableconstraint(
        client=client,
        data_url=data_url,
        timeout_ms=timeout_ms,
    )
    if availability is not None and availability["status"] == "empty":
        result = _empty_shape()
        result["status"] = "empty"
        result["observation_count"] = availability.get("observation_count", 0)
        result["query_fingerprint"] = fingerprint
        result["notes"] = availability.get("notes", [])
        _cache_put(cache, cache_key, result, probe_cache_lock)
        return result

    shape = await _probe_via_csv(
        client=client,
        data_url=data_url,
        sample_limit=sample_limit,
        max_distinct_per_dim=max_distinct_per_dim,
        timeout_ms=timeout_ms,
    )

    if shape["status"] == "error":
        shape["query_fingerprint"] = fingerprint
        return shape

    if availability is not None and availability.get("observation_count") is not None:
        shape["observation_count"] = availability["observation_count"]
        shape["notes"].append(
            "Observation count sourced from exact availableconstraint; "
            "CSV probe used for sample shape only."
        )

    shape["query_fingerprint"] = fingerprint

    _cache_put(cache, cache_key, shape, probe_cache_lock)
    return shape


async def _probe_via_csv(
    client: SDMXProgressiveClient,
    data_url: str,
    sample_limit: int,
    max_distinct_per_dim: int,
    timeout_ms: int,
) -> dict[str, Any]:
    """Fetch lightweight CSV data and derive probe shape metadata."""
    probe_url = build_probe_url(data_url)
    timeout_s = timeout_ms / 1000.0

    status_code, csv_text = await client.fetch_data_probe(
        probe_url, timeout=timeout_s,
    )

    if status_code == 0:
        return _error_result("Network or transport error — provider unreachable")

    if status_code == 404:
        result = _empty_shape()
        result["status"] = "empty"
        result["notes"] = ["HTTP 404 — no data found for this query."]
        return result

    if status_code >= 400:
        result = _error_result(
            "Provider returned HTTP " + str(status_code)
        )
        result["notes"] = ["HTTP " + str(status_code) + " from provider."]
        return result

    shape = parse_csv_probe_response(
        csv_text,
        sample_limit=sample_limit,
        max_distinct_per_dim=max_distinct_per_dim,
    )

    if shape["observation_count"] == 0:
        shape["status"] = "empty"
        shape["notes"] = [
            "Query is syntactically valid but returned zero observations."
        ]
    else:
        shape["status"] = "nonempty"
        shape["notes"] = []

    return shape


def _make_probe_cache_key(
    fingerprint: str,
    sample_limit: int,
    max_distinct_per_dim: int,
) -> str:
    """Build a cache key that includes result-shaping parameters."""
    return (
        fingerprint
        + "|sample_limit="
        + str(sample_limit)
        + "|max_distinct_per_dim="
        + str(max_distinct_per_dim)
    )


def _maybe_lock(lock: Any | None):
    """Context manager that locks if non-None, else no-ops.

    Session-scoped callers pass a threading.Lock guarding their
    SessionState.probe_cache; module-cache callers pass None.
    """
    import contextlib

    if lock is None:
        return contextlib.nullcontext()
    return lock


def _cache_put(
    cache: dict[str, dict[str, Any]],
    cache_key: str,
    result: dict[str, Any],
    lock: Any | None,
) -> None:
    """Store a probe result into `cache`, evicting oldest entries if full."""
    with _maybe_lock(lock):
        if len(cache) >= _PROBE_CACHE_MAX:
            # Evict oldest entry (first key in insertion order — Python 3.7+)
            oldest = next(iter(cache))
            del cache[oldest]
        cache[cache_key] = result


async def _build_url_from_parts(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    filters: dict[str, str] | None,
    start_period: str | None,
    end_period: str | None,
    agency_id: str | None = None,
) -> str:
    """Build an SDMX data URL from structured components."""
    default_agency = agency_id or client.agency_id
    flow_ref = _parse_flow_ref(dataflow_id, default_agency)
    base = client.base_url.rstrip("/")

    if filters:
        structure = await client.get_structure_summary(
            dataflow_id=flow_ref["dataflow_id"],
            agency_id=flow_ref["agency_id"],
            version=flow_ref["version"],
        )
        key_parts: list[str] = []
        for dim in sorted(structure.dimensions, key=lambda d: d.position):
            if dim.type == "TimeDimension":
                continue
            key_parts.append(filters.get(dim.id, ""))
        key = ".".join(key_parts)
    else:
        key = "all"

    url = base + "/data/" + dataflow_id + "/" + key

    params: dict[str, str] = {}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period
    if params:
        url += "?" + urlencode(sorted(params.items()))

    return url


def _parse_flow_ref(flow_ref: str, default_agency_id: str) -> dict[str, str]:
    """Split an SDMX flow reference into agency, id, and version parts."""
    parts = flow_ref.split(",")
    if len(parts) >= 3:
        return {
            "agency_id": parts[0],
            "dataflow_id": parts[1],
            "version": parts[2],
        }
    if len(parts) == 2:
        return {
            "agency_id": parts[0],
            "dataflow_id": parts[1],
            "version": "latest",
        }
    return {
        "agency_id": default_agency_id,
        "dataflow_id": flow_ref,
        "version": "latest",
    }


def _error_result(message: str) -> dict[str, Any]:
    """Build an error probe result."""
    result = _empty_shape()
    result["status"] = "error"
    result["notes"] = [message]
    result["query_fingerprint"] = ""
    return result


async def _preflight_with_availableconstraint(
    client: SDMXProgressiveClient,
    data_url: str,
    timeout_ms: int,
) -> dict[str, Any] | None:
    """Use exact availableconstraint when supported to avoid unnecessary CSV fetches."""
    strategy = get_constraint_strategy(client.endpoint_key, "single_flow")
    if strategy != "availableconstraint":
        return None

    parsed = parse_sdmx_data_url(data_url)
    if parsed is None:
        return None

    url = (
        parsed["base_url"].rstrip("/")
        + "/availableconstraint/"
        + parsed["dataflow_id"]
        + "/"
        + parsed["key"]
        + "/all/all?mode=exact"
    )
    params: list[tuple[str, str]] = [("mode", "exact")]
    if parsed.get("start_period"):
        params.append(("startPeriod", parsed["start_period"]))
    if parsed.get("end_period"):
        params.append(("endPeriod", parsed["end_period"]))
    if params:
        url = (
            parsed["base_url"].rstrip("/")
            + "/availableconstraint/"
            + parsed["dataflow_id"]
            + "/"
            + parsed["key"]
            + "/all/all?"
            + urlencode(params)
        )

    try:
        session = await client._get_session()
        response = await session.get(
            url,
            headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            timeout=timeout_ms / 1000.0,
        )
    except Exception:
        return None

    if response.status_code != 200 or not response.content:
        return None

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return None

    constraint = root.find(".//str:ContentConstraint", SDMX_NAMESPACES)
    if constraint is None:
        return None

    parsed_query = parse_sdmx_data_url(data_url)
    observation_count = _extract_obs_count(constraint)
    time_range = _extract_constraint_time_range(constraint)
    series_count = _extract_series_count(constraint)
    time_period_count = _infer_time_period_count(
        observation_count=observation_count,
        series_count=series_count,
        parsed_query=parsed_query,
    )
    has_empty_sentinel = (
        time_range is not None
        and time_range["start"] == "9999-01-01"
        and time_range["end"] == "0001-12-31"
    )

    if observation_count == 0 or has_empty_sentinel:
        notes = [
            "Exact availableconstraint indicates zero observations for this query."
        ]
        if has_empty_sentinel:
            notes.append(
                "Provider returned the SPC empty-result sentinel time range "
                "(9999-01-01 to 0001-12-31)."
            )
        return {
            "status": "empty",
            "observation_count": 0,
            "series_count": 0,
            "time_period_count": 0,
            "notes": notes,
        }

    if observation_count is not None:
        return {
            "status": "nonempty",
            "observation_count": observation_count,
            "series_count": series_count,
            "time_period_count": time_period_count,
            "notes": [],
        }

    return None


def _extract_obs_count(constraint: ET.Element) -> int | None:
    """Extract provider obs_count annotation if present."""
    for annotation in constraint.findall(".//com:Annotation", SDMX_NAMESPACES):
        if annotation.get("id") != "obs_count":
            continue
        title = annotation.find("./com:AnnotationTitle", SDMX_NAMESPACES)
        if title is not None and title.text:
            try:
                return int(title.text)
            except ValueError:
                return None
    return None


def _extract_constraint_time_range(constraint: ET.Element) -> dict[str, str] | None:
    """Extract overall time range from a constraint."""
    starts: list[str] = []
    ends: list[str] = []
    for time_range in constraint.findall(".//com:TimeRange", SDMX_NAMESPACES):
        start_el = time_range.find("./com:StartPeriod", SDMX_NAMESPACES)
        end_el = time_range.find("./com:EndPeriod", SDMX_NAMESPACES)
        if start_el is not None and start_el.text:
            starts.append(start_el.text[:10])
        if end_el is not None and end_el.text:
            ends.append(end_el.text[:10])
    if not starts or not ends:
        return None
    return {"start": min(starts), "end": max(ends)}


def _extract_series_count(constraint: ET.Element) -> int:
    """Estimate series count from non-time dimension combinations in CubeRegions."""
    total = 0
    for cube_region in constraint.findall(".//str:CubeRegion", SDMX_NAMESPACES):
        if cube_region.get("include", "true") != "true":
            continue
        count = 1
        has_non_time_dimension = False
        for key_value in cube_region.findall("./com:KeyValue", SDMX_NAMESPACES):
            dim_id = key_value.get("id", "")
            if dim_id in TIME_DIMENSION_IDS:
                continue
            values = key_value.findall("./com:Value", SDMX_NAMESPACES)
            if not values:
                continue
            has_non_time_dimension = True
            count *= len(values)
        total += count if has_non_time_dimension else 0
    return total


def _infer_time_period_count(
    observation_count: int | None,
    series_count: int,
    parsed_query: dict[str, Any] | None,
) -> int:
    """Infer a conservative time-period count from exact availability metadata."""
    if observation_count is None or observation_count <= 0:
        return 0
    if parsed_query is not None:
        start_period = parsed_query.get("start_period")
        end_period = parsed_query.get("end_period")
        if start_period and end_period and start_period == end_period:
            return 1
    if series_count == 1:
        return observation_count
    return 0


def generate_relaxation_candidates(
    parsed_url: dict[str, Any],
    dimension_names: list[str],
    relax_dimensions: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Generate candidate URLs by relaxing one dimension or time range at a time.

    Args:
        parsed_url: Output of parse_sdmx_data_url().
        dimension_names: Ordered list of dimension IDs matching key_parts positions.
        relax_dimensions: If set, only relax these dimensions. None = all.

    Returns:
        List of candidate dicts, each with:
            url, changed_dimensions, change_summary, relaxation_level
        Ordered: single-dimension relaxations first, then time widening.
    """
    base_url = parsed_url["base_url"]
    dataflow_id = parsed_url["dataflow_id"]
    key_parts = list(parsed_url["key_parts"])
    start_period = parsed_url["start_period"]
    end_period = parsed_url["end_period"]
    extra_params = dict(parsed_url.get("params", {}))

    candidates: list[dict[str, Any]] = []

    # Single-dimension relaxations (set one key position to empty)
    for i, dim_id in enumerate(dimension_names):
        if i >= len(key_parts):
            break
        if key_parts[i] == "" or key_parts[i] == "all":
            # Already wildcarded — skip
            continue
        if relax_dimensions is not None and dim_id not in relax_dimensions:
            continue

        relaxed = list(key_parts)
        original_value = relaxed[i]
        relaxed[i] = ""
        new_key = ".".join(relaxed)

        url = _build_candidate_url(
            base_url, dataflow_id, new_key, start_period, end_period, extra_params,
        )
        candidates.append({
            "url": url,
            "changed_dimensions": [dim_id],
            "change_summary": "Relaxed " + dim_id + " from " + original_value + " to all values",
            "relaxation_level": 1,
        })

    # Time-range widening (drop startPeriod and endPeriod)
    if start_period or end_period:
        should_include = (
            relax_dimensions is None or "TIME_PERIOD" in relax_dimensions
        )
        if should_include:
            url = _build_candidate_url(
                base_url, dataflow_id, ".".join(key_parts), None, None, extra_params,
            )
            candidates.append({
                "url": url,
                "changed_dimensions": ["TIME_PERIOD"],
                "change_summary": "Removed time period filter (was "
                    + (start_period or "?") + " to " + (end_period or "?") + ")",
                "relaxation_level": 1,
            })

    return candidates


def _build_candidate_url(
    base_url: str,
    dataflow_id: str,
    key: str,
    start_period: str | None,
    end_period: str | None,
    extra_params: dict[str, str] | None = None,
) -> str:
    """Build a data URL from components for candidate generation."""
    url = base_url.rstrip("/") + "/data/" + dataflow_id + "/" + key
    params: list[tuple[str, str]] = []
    if extra_params:
        for key_name, value in sorted(extra_params.items()):
            params.append((key_name, value))
    if start_period:
        params.append(("startPeriod", start_period))
    if end_period:
        params.append(("endPeriod", end_period))
    if params:
        url += "?" + urlencode(params)
    return url


async def suggest_nonempty_queries(
    client: SDMXProgressiveClient,
    data_url: str,
    relax_dimensions: list[str] | None = None,
    max_suggestions: int = 5,
    max_probes: int = 20,
    intent_hint: str = "generic",
) -> dict[str, Any]:
    """Suggest nearby non-empty queries by bounded relaxation.

    If the original query is non-empty, returns immediately.
    Otherwise, generates single-dimension relaxations and probes them
    within a strict budget.

    Args:
        client: SDMX client for probing and DSD access.
        data_url: The exact SDMX data URL that may be empty.
        relax_dimensions: Only relax these dimensions (None = all).
        max_suggestions: Max number of suggestions to return.
        max_probes: Max number of HTTP probes to make.
        intent_hint: One of generic, kpi, timeseries, ranking, map.

    Returns a dict matching the SuggestionResult schema.
    """
    fingerprint = normalize_query_fingerprint(data_url)
    probes_used = 0

    # Step 1: Probe the original query
    original_result, calls_used = await _evaluate_query_for_suggestions(
        client=client,
        data_url=data_url,
    )
    probes_used += calls_used

    if original_result["status"] == "nonempty":
        return {
            "original_status": "nonempty",
            "original_query_fingerprint": fingerprint,
            "suggestions": [],
            "probes_used": probes_used,
            "notes": [],
        }

    # Step 2: Parse the URL and get DSD for dimension names
    parsed = parse_sdmx_data_url(data_url)
    if parsed is None:
        return {
            "original_status": original_result["status"],
            "original_query_fingerprint": fingerprint,
            "suggestions": [],
            "probes_used": probes_used,
            "notes": ["Could not parse data URL for relaxation."],
        }

    # Extract dataflow ID (strip agency/version if present)
    raw_flow = parsed["dataflow_id"]
    flow_ref = _parse_flow_ref(raw_flow, client.agency_id)
    dataflow_id = flow_ref["dataflow_id"]
    agency_id = flow_ref["agency_id"]
    version = flow_ref["version"]

    # Get dimension names from DSD
    structure = await client.get_structure_summary(
        dataflow_id=dataflow_id,
        agency_id=agency_id,
        version=version,
    )

    if structure is None:
        return {
            "original_status": original_result["status"],
            "original_query_fingerprint": fingerprint,
            "suggestions": [],
            "probes_used": probes_used,
            "notes": ["Could not fetch DSD for " + dataflow_id + "."],
        }

    # Build ordered dimension name list (excluding TimeDimension)
    if hasattr(structure, "dimensions"):
        dims = structure.dimensions
    else:
        dims = []

    dim_names: list[str] = []
    for d in sorted(dims, key=lambda x: getattr(x, "position", 0)):
        if getattr(d, "type", "") == "TimeDimension":
            continue
        dim_names.append(getattr(d, "id", ""))

    # Step 3: Generate candidates
    candidates = generate_relaxation_candidates(
        parsed, dim_names, relax_dimensions=relax_dimensions,
    )

    # Step 4: Rank candidates by intent
    candidates = _rank_candidates(candidates, intent_hint)

    # Step 5: Probe candidates within budget
    suggestions: list[dict[str, Any]] = []
    rank = 0

    for candidate in candidates:
        if probes_used >= max_probes:
            break
        if len(suggestions) >= max_suggestions:
            break

        probe_result, calls_used = await _evaluate_query_for_suggestions(
            client=client,
            data_url=candidate["url"],
        )
        probes_used += calls_used

        if probe_result["status"] == "nonempty":
            rank += 1
            suggestions.append({
                "rank": rank,
                "change_summary": candidate["change_summary"],
                "changed_dimensions": candidate["changed_dimensions"],
                "suggested_data_url": candidate["url"],
                "probe_result": {
                    "status": "nonempty",
                    "observation_count": probe_result["observation_count"],
                    "series_count": probe_result["series_count"],
                    "time_period_count": probe_result["time_period_count"],
                },
            })

    notes: list[str] = []
    if probes_used >= max_probes and len(suggestions) < max_suggestions:
        notes.append(
            "Probe budget exhausted (" + str(max_probes) + " probes). "
            "Not all candidates were tested."
        )

    return {
        "original_status": original_result["status"],
        "original_query_fingerprint": fingerprint,
        "suggestions": suggestions,
        "probes_used": probes_used,
        "notes": notes,
    }


async def _evaluate_query_for_suggestions(
    client: SDMXProgressiveClient,
    data_url: str,
) -> tuple[dict[str, Any], int]:
    """Evaluate a candidate query using availability first, then CSV fallback."""
    availability = await _preflight_with_availableconstraint(
        client=client,
        data_url=data_url,
        timeout_ms=10000,
    )
    if availability is not None:
        result = {
            "status": availability["status"],
            "observation_count": availability.get("observation_count", 0),
            "series_count": availability.get("series_count", 0),
            "time_period_count": availability.get("time_period_count", 0),
        }
        return result, 1

    result = await _probe_via_csv(
        client=client,
        data_url=data_url,
        sample_limit=0,
        max_distinct_per_dim=0,
        timeout_ms=10000,
    )
    return result, 1


def _rank_candidates(
    candidates: list[dict[str, Any]],
    intent_hint: str,
) -> list[dict[str, Any]]:
    """Reorder candidates to preserve intent.

    For map intent: defer geo relaxation (try other dims first).
    For timeseries intent: defer time relaxation.
    Default: single-dim before multi-dim, dimension order preserved.
    """
    defer_dims: set[str] = set()
    if intent_hint == "map":
        defer_dims = GEO_DIMENSION_IDS
    elif intent_hint == "timeseries":
        defer_dims = TIME_DIMENSION_IDS

    deferred: list[dict[str, Any]] = []
    prioritised: list[dict[str, Any]] = []

    for c in candidates:
        changed = set(c["changed_dimensions"])
        if changed & defer_dims:
            deferred.append(c)
        else:
            prioritised.append(c)

    return prioritised + deferred
