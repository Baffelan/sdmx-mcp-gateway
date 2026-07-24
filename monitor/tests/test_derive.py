from derive import derive_status


def _row(path: str, kind: str, ok: bool, skipped: bool = False) -> dict:
    return {"path": path, "kind": kind, "ok": ok, "skipped": skipped}


def _rows(gm=True, gd=True, dm=True, dd=True) -> list[dict]:
    return [
        _row("gateway", "metadata", gm),
        _row("gateway", "data", gd),
        _row("direct", "metadata", dm),
        _row("direct", "data", dd),
    ]


def test_all_passing_is_healthy():
    status, reason = derive_status(_rows(), gateway_up=True)
    assert status == "healthy"
    assert reason


def test_gateway_down_is_unknown_regardless_of_direct():
    status, _ = derive_status(_rows(), gateway_up=False)
    assert status == "unknown"


def test_both_metadata_failing_is_provider_down():
    status, _ = derive_status(_rows(gm=False, gd=False, dm=False, dd=False), gateway_up=True)
    assert status == "provider_down"


def test_direct_ok_gateway_failing_is_gateway_issue():
    # the live ECB case: gateway probe 406s, direct path fine
    status, reason = derive_status(_rows(gd=False), gateway_up=True)
    assert status == "gateway_issue"
    assert "gateway" in reason


def test_data_failing_on_both_paths_is_degraded():
    status, reason = derive_status(_rows(gd=False, dd=False), gateway_up=True)
    assert status == "degraded"
    assert "data" in reason


def test_direct_metadata_failing_alone_is_degraded():
    status, _ = derive_status(_rows(dm=False), gateway_up=True)
    assert status == "degraded"


def test_skipped_rows_are_ignored():
    rows = [
        _row("gateway", "metadata", True),
        _row("gateway", "data", False, skipped=True),
        _row("direct", "metadata", True),
        _row("direct", "data", False, skipped=True),
    ]
    status, _ = derive_status(rows, gateway_up=True)
    assert status == "healthy"


def test_no_rows_is_unknown():
    status, _ = derive_status([], gateway_up=True)
    assert status == "unknown"


def test_unrun_direct_side_is_not_reported_as_gateway_issue():
    rows = [
        _row("gateway", "metadata", False),
        _row("gateway", "data", True),
        _row("direct", "metadata", False, skipped=True),
        _row("direct", "data", False, skipped=True),
    ]
    status, reason = derive_status(rows, gateway_up=True)
    assert status == "degraded"
    assert "direct path OK" not in reason
