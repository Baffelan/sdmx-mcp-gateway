"""Tests for main_server._build_mismatch_hint."""

from __future__ import annotations

from session_manager import SessionState


def test_mismatch_hint_names_known_endpoint_when_dataflow_seen_elsewhere():
    from main_server import _build_mismatch_hint

    state = SessionState(session_id="s1", default_endpoint_key="SPC")
    state.register_dataflow("FBOS", "CPI")

    hint = _build_mismatch_hint(state, resolved_endpoint="SBS", dataflow_id="CPI")

    assert "CPI" in hint
    assert "SBS" in hint
    assert "FBOS" in hint
    assert "endpoint='FBOS'" in hint


def test_mismatch_hint_generic_when_dataflow_never_seen():
    from main_server import _build_mismatch_hint

    state = SessionState(session_id="s1", default_endpoint_key="SPC")
    hint = _build_mismatch_hint(state, resolved_endpoint="SPC", dataflow_id="UNKNOWN_DF")

    assert "UNKNOWN_DF" not in hint  # no false-specific pointing
    assert "SPC" in hint
    # Mentions valid keys so LLM can retry
    assert "SPC" in hint and "ECB" in hint


def test_mismatch_hint_no_dataflow_id_returns_generic():
    from main_server import _build_mismatch_hint

    state = SessionState(session_id="s1", default_endpoint_key="SPC")
    hint = _build_mismatch_hint(state, resolved_endpoint="SPC", dataflow_id=None)

    assert "endpoint" in hint.lower()
    assert "ECB" in hint  # lists all valid endpoints


def test_mismatch_hint_steers_toward_agency_for_sub_agency_id():
    """Dataflow IDs containing '@' (OECD's DSD@DF convention) must steer
    the caller toward agency_id=, not endpoint=. Before this branch existed,
    the generic 'Pass endpoint=<key>' phrasing misled LLMs into switching
    endpoints when the endpoint was already correct — they just didn't know
    the sub-agency owner."""
    from main_server import _build_mismatch_hint

    state = SessionState(session_id="s1", default_endpoint_key="OECD")
    hint = _build_mismatch_hint(
        state,
        resolved_endpoint="OECD",
        dataflow_id="DSD_RDS_GERD@DF_GERD_SOF",
    )

    assert "DSD_RDS_GERD@DF_GERD_SOF" in hint
    assert "OECD" in hint
    # Points to the right parameter
    assert "agency_id" in hint
    assert "list_dataflows" in hint
    # Does NOT emit the misleading "Pass endpoint=<key>" generic suggestion
    assert "Pass endpoint=<key>" not in hint
    # Does NOT repeat the full "Registered endpoints:" list — stays focused
    assert "Registered endpoints:" not in hint


def test_mismatch_hint_at_symbol_loses_to_known_elsewhere_priority():
    """If an '@'-flow is registered on another endpoint, the known-elsewhere
    branch wins — that's a stronger signal than the generic @-heuristic."""
    from main_server import _build_mismatch_hint

    state = SessionState(session_id="s1", default_endpoint_key="OECD")
    state.register_dataflow("ESTAT", "DSD_STRANGE@DF_EDGE_CASE")

    hint = _build_mismatch_hint(
        state,
        resolved_endpoint="OECD",
        dataflow_id="DSD_STRANGE@DF_EDGE_CASE",
    )

    # Known-elsewhere branch fires, not the @-branch
    assert "endpoint='ESTAT'" in hint
    assert "agency_id" not in hint
