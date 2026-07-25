"""Read-time derivation of per-endpoint status from raw check rows.

Raw rows are stored; status is computed here on read, so the rules live in
one place and history can be re-interpreted if they change.
"""

STATUSES: list[str] = ["healthy", "degraded", "gateway_issue", "provider_down", "unknown"]


def _lookup(rows: list[dict], path: str, kind: str) -> bool | None:
    """True/False when the check ran, None when missing or skipped."""
    for row in rows:
        if row["path"] == path and row["kind"] == kind:
            if row.get("skipped"):
                return None
            return bool(row["ok"])
    return None


def derive_status(
    rows: list[dict], gateway_up: bool, contracts: list[dict] | None = None
) -> tuple[str, str]:
    if not gateway_up:
        status = "unknown"
        reason = "gateway unreachable; direct results shown for provider status"
    else:
        gm = _lookup(rows, "gateway", "metadata")
        gd = _lookup(rows, "gateway", "data")
        dm = _lookup(rows, "direct", "metadata")
        dd = _lookup(rows, "direct", "data")
        dj = _lookup(rows, "direct", "json")
        ran = [v for v in (gm, gd, dm, dd, dj) if v is not None]
        direct_ran = [v for v in (dm, dd, dj) if v is not None]
        direct_ok = bool(direct_ran) and all(direct_ran)
        failing = [
            name
            for name, value in [
                ("gateway metadata", gm),
                ("gateway data", gd),
                ("direct metadata", dm),
                ("direct data", dd),
                ("direct json", dj),
            ]
            if value is False
        ]
        if not ran:
            status, reason = "unknown", "no checks recorded"
        elif all(ran):
            status, reason = "healthy", "all checks passing"
        elif gm is False and dm is False:
            status, reason = (
                "provider_down", "metadata failing on both the gateway and the direct path"
            )
        elif direct_ok and (gm is False or gd is False):
            status, reason = "gateway_issue", "direct path OK; gateway path failing"
        else:
            status, reason = "degraded", "failing: " + ", ".join(failing)

    broken = [c["assertion"] for c in (contracts or []) if c.get("verdict") == "broken"]
    if broken and status == "healthy":
        return "degraded", "API contract broken: " + ", ".join(sorted(broken))
    if broken and status == "degraded":
        return status, reason + "; API contract broken: " + ", ".join(sorted(broken))
    return status, reason
