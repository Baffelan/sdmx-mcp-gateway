"""Direct-path checks: hit each provider's SDMx REST API without the gateway.

Success criteria:
- metadata: HTTP 200 and the body mentions a Dataflow element
- data: HTTP 200 and the body contains at least one observation
  (CSV data row, or an Obs element for XML responses)
"""

import time

import httpx

from checks_common import with_retry
from endpoints_config import Endpoint
from storage import CheckResult


def count_csv_observations(text: str) -> int:
    lines = [line for line in text.strip().splitlines()[1:] if line.strip()]
    return len(lines)


def looks_like_xml_data(text: str) -> bool:
    return "<Obs" in text or ":Obs " in text or ":Obs>" in text


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


async def _metadata_once(client: httpx.AsyncClient, ep: Endpoint) -> CheckResult:
    start = time.monotonic()
    try:
        resp = await client.get(ep.metadata_url, headers=ep.auth_headers())
    except httpx.HTTPError as exc:
        return CheckResult(ep.key, "direct", "metadata", ok=False,
                           latency_ms=_ms(start), error=str(exc)[:300])
    ok = resp.status_code == 200 and "Dataflow" in resp.text
    error = None
    if not ok:
        error = "HTTP " + str(resp.status_code)
        if resp.status_code == 200:
            error = "response contains no Dataflow element"
    return CheckResult(ep.key, "direct", "metadata", ok=ok, latency_ms=_ms(start),
                       http_status=resp.status_code, error=error)


async def _data_once(client: httpx.AsyncClient, ep: Endpoint) -> CheckResult:
    start = time.monotonic()
    url = ep.data_url
    assert url is not None  # callers gate on ep.data_path
    headers = {"Accept": ep.data_accept, **ep.auth_headers()}
    try:
        resp = await client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        return CheckResult(ep.key, "direct", "data", ok=False,
                           latency_ms=_ms(start), error=str(exc)[:300])
    if resp.status_code != 200:
        return CheckResult(ep.key, "direct", "data", ok=False, latency_ms=_ms(start),
                           http_status=resp.status_code,
                           error="HTTP " + str(resp.status_code))
    content_type = resp.headers.get("content-type", "")
    if "xml" in content_type or resp.text.lstrip().startswith("<"):
        ok = looks_like_xml_data(resp.text)
        obs: int | None = None
    else:
        obs = count_csv_observations(resp.text)
        ok = obs >= 1
    error = None if ok else "no observations in response"
    return CheckResult(ep.key, "direct", "data", ok=ok, latency_ms=_ms(start),
                       http_status=resp.status_code, obs_count=obs, error=error)


async def run_direct_checks(client: httpx.AsyncClient, ep: Endpoint) -> list[CheckResult]:
    if ep.credentials_missing:
        reason = "skipped: " + str(ep.requires_env) + " not set"
        return [
            CheckResult(ep.key, "direct", "metadata", ok=False, skipped=True, error=reason),
            CheckResult(ep.key, "direct", "data", ok=False, skipped=True, error=reason),
        ]
    meta = await with_retry(lambda: _metadata_once(client, ep))
    if ep.data_path is None:
        data = CheckResult(ep.key, "direct", "data", ok=False, skipped=True,
                           error="skipped: no pinned data query")
    else:
        data = await with_retry(lambda: _data_once(client, ep))
    return [meta, data]
