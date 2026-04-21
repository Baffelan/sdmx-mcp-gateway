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
