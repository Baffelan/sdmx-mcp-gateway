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
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

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
    dim_values: dict[str, set[str]] = {h: set() for _, h in dim_indices}
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

    # Build dimension summaries
    dimensions: dict[str, dict[str, Any]] = {}
    for _, dim_id in dim_indices:
        vals = dim_values[dim_id]
        dimensions[dim_id] = {
            "distinct_count": len(vals),
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


# Probe result cache: fingerprint -> result dict
_probe_cache: dict[str, dict[str, Any]] = {}


async def probe_data_url(
    client: SDMXProgressiveClient,
    data_url: str | None = None,
    dataflow_id: str | None = None,
    filters: dict[str, str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    sample_limit: int = 5,
    max_distinct_per_dim: int = 10,
    timeout_ms: int = 10000,
) -> dict[str, Any]:
    """Probe an exact SDMX query and return shape metadata.

    Accepts either a data_url or structured input (dataflow_id + filters).
    If structured input is provided and data_url is None, the URL is built
    from the client's base_url.

    Returns a dict matching the ProbeResult schema.
    """
    # Build URL from structured input if needed
    if data_url is None:
        if dataflow_id is None:
            return _error_result("Either data_url or dataflow_id is required")
        data_url = _build_url_from_parts(
            client.base_url, dataflow_id, filters, start_period, end_period,
        )

    fingerprint = normalize_query_fingerprint(data_url)

    # Check cache
    if fingerprint in _probe_cache:
        return _probe_cache[fingerprint]

    # Build lightweight probe URL
    probe_url = build_probe_url(data_url)
    timeout_s = timeout_ms / 1000.0

    status_code, csv_text = await client.fetch_data_probe(
        probe_url, timeout=timeout_s,
    )

    result: dict[str, Any]

    if status_code == 0:
        result = _error_result("Network or transport error — provider unreachable")
        result["query_fingerprint"] = fingerprint
        return result

    if status_code == 404:
        result = _empty_shape()
        result["status"] = "empty"
        result["query_fingerprint"] = fingerprint
        result["notes"] = ["HTTP 404 — no data found for this query."]
        _probe_cache[fingerprint] = result
        return result

    if status_code >= 400:
        result = _error_result(
            "Provider returned HTTP " + str(status_code)
        )
        result["query_fingerprint"] = fingerprint
        result["notes"] = ["HTTP " + str(status_code) + " from provider."]
        return result

    # Parse the CSV response
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

    shape["query_fingerprint"] = fingerprint

    _probe_cache[fingerprint] = shape
    return shape


def _build_url_from_parts(
    base_url: str,
    dataflow_id: str,
    filters: dict[str, str] | None,
    start_period: str | None,
    end_period: str | None,
) -> str:
    """Build an SDMX data URL from structured components."""
    base = base_url.rstrip("/")

    # Build key from filters (without DSD — just concatenate values)
    if filters:
        key = ".".join(filters.get(k, "") for k in sorted(filters.keys()))
    else:
        key = "all"

    url = base + "/data/" + dataflow_id + "/" + key

    params: list[str] = []
    if start_period:
        params.append("startPeriod=" + start_period)
    if end_period:
        params.append("endPeriod=" + end_period)
    if params:
        url += "?" + "&".join(params)

    return url


def _error_result(message: str) -> dict[str, Any]:
    """Build an error probe result."""
    result = _empty_shape()
    result["status"] = "error"
    result["notes"] = [message]
    result["query_fingerprint"] = ""
    return result


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
            base_url, dataflow_id, new_key, start_period, end_period,
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
                base_url, dataflow_id, ".".join(key_parts), None, None,
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
) -> str:
    """Build a data URL from components for candidate generation."""
    url = base_url.rstrip("/") + "/data/" + dataflow_id + "/" + key
    params: list[str] = []
    if start_period:
        params.append("startPeriod=" + start_period)
    if end_period:
        params.append("endPeriod=" + end_period)
    if params:
        url += "?" + "&".join(params)
    return url
