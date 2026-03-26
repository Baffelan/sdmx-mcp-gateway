"""
Query probing and bounded recovery for SDMX data.

These tools address the gap between query validation (syntax/codes are valid)
and query usefulness (the exact query returns observations).
"""

from __future__ import annotations

import hashlib
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
