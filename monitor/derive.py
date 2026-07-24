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


def derive_status(rows: list[dict], gateway_up: bool) -> tuple[str, str]:
    if not gateway_up:
        return "unknown", "gateway unreachable; direct results shown for provider status"
    gm = _lookup(rows, "gateway", "metadata")
    gd = _lookup(rows, "gateway", "data")
    dm = _lookup(rows, "direct", "metadata")
    dd = _lookup(rows, "direct", "data")
    ran = [v for v in (gm, gd, dm, dd) if v is not None]
    if not ran:
        return "unknown", "no checks recorded"
    if all(ran):
        return "healthy", "all checks passing"
    if gm is False and dm is False:
        return "provider_down", "metadata failing on both the gateway and the direct path"
    direct_ran = [v for v in (dm, dd) if v is not None]
    direct_ok = bool(direct_ran) and all(direct_ran)
    if direct_ok and (gm is False or gd is False):
        return "gateway_issue", "direct path OK; gateway path failing"
    failing = [
        name
        for name, value in [
            ("gateway metadata", gm),
            ("gateway data", gd),
            ("direct metadata", dm),
            ("direct data", dd),
        ]
        if value is False
    ]
    return "degraded", "failing: " + ", ".join(failing)
