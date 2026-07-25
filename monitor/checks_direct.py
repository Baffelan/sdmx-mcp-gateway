"""Direct-path checks: hit each provider's SDMx REST API without the gateway.

Success criteria:
- metadata: HTTP 200 and the body mentions a Dataflow element
- data: HTTP 200 and the body contains at least one observation
  (CSV data row, or an Obs element for XML responses)
- json: HTTP 200 and the body is really SDMx-JSON of the requested version
  (some providers answer a JSON Accept header with HTTP 200 and a
  different format entirely, so the body is parsed and checked)
"""

import csv
import io
import json
import time

import httpx

from checks_common import with_retry
from endpoints_config import Endpoint
from storage import CheckResult


def parse_csv_observations(text: str) -> tuple[int, str | None, str | None, str | None]:
    """Verify an SDMx-CSV body really carries observations.

    An HTTP 200 proves the request was answered, not that data came back:
    providers have been observed returning notices, wrong formats, and empty
    result sets with a 200. So require the SDMx-CSV OBS_VALUE column and at
    least one row whose value parses as a number.

    Empty values are normal in SDMx (confidential or unpublished points), so
    the rule is "some value in the slice is numeric", not "the first one is".

    Returns (numeric_count, sample_value, sample_period, error).
    """
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return 0, None, None, "empty response body"
    header = [cell.strip() for cell in rows[0]]
    if "OBS_VALUE" not in header:
        return 0, None, None, "response has no OBS_VALUE column (not SDMx-CSV)"
    value_index = header.index("OBS_VALUE")
    period_index = header.index("TIME_PERIOD") if "TIME_PERIOD" in header else None
    data_rows = [row for row in rows[1:] if any(cell.strip() for cell in row)]
    if not data_rows:
        return 0, None, None, "no observation rows in response"
    numeric_count = 0
    sample_value: str | None = None
    sample_period: str | None = None
    for row in data_rows:
        if len(row) <= value_index:
            continue
        raw = row[value_index].strip()
        if not raw:
            continue
        try:
            float(raw)
        except ValueError:
            continue
        numeric_count += 1
        if sample_value is None:
            sample_value = raw
            if period_index is not None and len(row) > period_index:
                sample_period = row[period_index].strip() or None
    if numeric_count == 0:
        return 0, None, None, "no numeric observation value in " + str(len(data_rows)) + " rows"
    return numeric_count, sample_value, sample_period, None


def looks_like_xml_data(text: str) -> bool:
    return "<Obs" in text or ":Obs " in text or ":Obs>" in text


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


async def _metadata_once(client: httpx.AsyncClient, ep: Endpoint) -> CheckResult:
    start = time.monotonic()
    try:
        resp = await client.get(ep.metadata_url, headers=ep.auth_headers())
        ok = resp.status_code == 200 and "Dataflow" in resp.text
        error = None
        if not ok:
            error = "HTTP " + str(resp.status_code)
            if resp.status_code == 200:
                error = "response contains no Dataflow element"
        return CheckResult(ep.key, "direct", "metadata", ok=ok, latency_ms=_ms(start),
                           http_status=resp.status_code, error=error)
    except Exception as exc:
        error_str = type(exc).__name__ + ": " + str(exc)
        return CheckResult(ep.key, "direct", "metadata", ok=False,
                           latency_ms=_ms(start), error=error_str[:300])


async def _data_once(client: httpx.AsyncClient, ep: Endpoint) -> CheckResult:
    start = time.monotonic()
    url = ep.data_url
    assert url is not None  # callers gate on ep.data_path
    try:
        headers = {"Accept": ep.data_accept, **ep.auth_headers()}
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return CheckResult(ep.key, "direct", "data", ok=False, latency_ms=_ms(start),
                               http_status=resp.status_code,
                               error="HTTP " + str(resp.status_code))
        content_type = resp.headers.get("content-type", "")
        if "xml" in content_type or resp.text.lstrip().startswith("<"):
            ok = looks_like_xml_data(resp.text)
            obs: int | None = None
            error = None if ok else "no observations in response"
            return CheckResult(ep.key, "direct", "data", ok=ok, latency_ms=_ms(start),
                               http_status=resp.status_code, obs_count=obs, error=error)
        else:
            obs, sample_value, sample_period, parse_error = parse_csv_observations(resp.text)
            return CheckResult(ep.key, "direct", "data", ok=parse_error is None,
                               latency_ms=_ms(start), http_status=resp.status_code,
                               obs_count=obs, sample_value=sample_value,
                               sample_period=sample_period, error=parse_error)
    except Exception as exc:
        error_str = type(exc).__name__ + ": " + str(exc)
        return CheckResult(ep.key, "direct", "data", ok=False,
                           latency_ms=_ms(start), error=error_str[:300])


def _declared_version(content_type: str) -> str | None:
    for part in content_type.split(";"):
        part = part.strip()
        if part.startswith("version="):
            return part[len("version="):].strip()
    return None


def verify_json_payload(text: str, content_type: str, requested_accept: str) -> str | None:
    """Check a JSON data response is really SDMx-JSON of the requested version.

    Providers have been observed answering a JSON request with HTTP 200 and a
    different format entirely (IMF returns XML, Stats NZ returns CSV), so the
    body is parsed and its shape checked rather than trusting the status code.
    Returns an error string, or None when the payload is good.
    """
    if "json" not in content_type.lower():
        return "response is not JSON (content-type: " + (content_type or "none") + ")"
    try:
        payload = json.loads(text)
    except ValueError as exc:
        return "response is not JSON: " + str(exc)[:120]
    if not isinstance(payload, dict):
        return "response is not JSON: top level is " + type(payload).__name__
    wanted = _declared_version(requested_accept)
    served = _declared_version(content_type)
    if wanted and served and served != wanted:
        return "served SDMx-JSON version " + served + ", requested " + wanted
    datasets = payload.get("dataSets")
    if not isinstance(datasets, list) or not datasets:
        return "SDMx-JSON payload has no dataSets"
    return None


async def _json_once(client: httpx.AsyncClient, ep: Endpoint) -> CheckResult:
    start = time.monotonic()
    try:
        url = ep.json_url
        assert url is not None and ep.json_accept is not None  # caller gates on json_accept
        headers = {"Accept": ep.json_accept, **ep.auth_headers()}
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return CheckResult(ep.key, "direct", "json", ok=False, latency_ms=_ms(start),
                               http_status=resp.status_code,
                               error="HTTP " + str(resp.status_code))
        error = verify_json_payload(
            resp.text, resp.headers.get("content-type", ""), ep.json_accept
        )
        return CheckResult(ep.key, "direct", "json", ok=error is None, latency_ms=_ms(start),
                           http_status=resp.status_code, error=error)
    except Exception as exc:
        return CheckResult(ep.key, "direct", "json", ok=False, latency_ms=_ms(start),
                           error=(type(exc).__name__ + ": " + str(exc))[:300])


async def run_direct_checks(client: httpx.AsyncClient, ep: Endpoint) -> list[CheckResult]:
    if ep.credentials_missing:
        reason = "skipped: " + str(ep.requires_env) + " not set"
        return [
            CheckResult(ep.key, "direct", "metadata", ok=False, skipped=True, error=reason),
            CheckResult(ep.key, "direct", "data", ok=False, skipped=True, error=reason),
            CheckResult(ep.key, "direct", "json", ok=False, skipped=True, error=reason),
        ]
    meta = await with_retry(lambda: _metadata_once(client, ep))
    if ep.data_path is None:
        data = CheckResult(ep.key, "direct", "data", ok=False, skipped=True,
                           error="skipped: no pinned data query")
    else:
        data = await with_retry(lambda: _data_once(client, ep))
    if ep.json_accept is None:
        reason = ep.json_unsupported_reason or "provider does not serve SDMx-JSON"
        json_result = CheckResult(ep.key, "direct", "json", ok=False, skipped=True,
                                  error="skipped: provider does not serve SDMx-JSON (" + reason + ")")
    else:
        json_result = await with_retry(lambda: _json_once(client, ep))
    return [meta, data, json_result]
