"""The endpoint table is config, but config with invariants worth locking down:
pinned queries must stay tiny (observation caps) and keys must match the gateway's."""

from endpoints_config import ENDPOINT_KEYS, ENDPOINTS, SDMX_CSV

EXPECTED_KEYS = {
    "SPC", "FBOS", "SBS", "ECB", "UNICEF", "IMF",
    "OECD", "ESTAT", "ILO", "ABS", "BIS", "STATSNZ",
}


def test_all_gateway_endpoints_covered():
    assert set(ENDPOINT_KEYS) == EXPECTED_KEYS
    assert len(ENDPOINT_KEYS) == len(set(ENDPOINT_KEYS))


def test_metadata_urls_are_absolute_dataflow_urls():
    for ep in ENDPOINTS:
        assert ep.metadata_url.startswith("https://"), ep.key
        assert "/dataflow/" in ep.metadata_url, ep.key


def test_data_urls_are_observation_capped():
    """Every pinned data query must cap observations so a check can never
    download an unbounded payload from a provider."""
    for ep in ENDPOINTS:
        if ep.data_path is None:
            continue
        assert "NObservations=1" in ep.data_path, ep.key
        assert ep.data_url == ep.base_url + ep.data_path


def test_statsnz_is_credential_gated():
    (statsnz,) = [ep for ep in ENDPOINTS if ep.key == "STATSNZ"]
    assert statsnz.requires_env == "SDMX_STATSNZ_KEY"
    assert statsnz.auth_header == "Ocp-Apim-Subscription-Key"


def test_auth_headers_follow_env(monkeypatch):
    (statsnz,) = [ep for ep in ENDPOINTS if ep.key == "STATSNZ"]
    monkeypatch.delenv("SDMX_STATSNZ_KEY", raising=False)
    assert statsnz.credentials_missing is True
    assert statsnz.auth_headers() == {}
    monkeypatch.setenv("SDMX_STATSNZ_KEY", "abc123")
    assert statsnz.credentials_missing is False
    assert statsnz.auth_headers() == {"Ocp-Apim-Subscription-Key": "abc123"}


def test_ecb_uses_plain_csv_accept():
    (ecb,) = [ep for ep in ENDPOINTS if ep.key == "ECB"]
    assert ecb.data_accept == "text/csv"
    others = [ep for ep in ENDPOINTS if ep.key != "ECB" and ep.data_path is not None]
    assert all(ep.data_accept == SDMX_CSV for ep in others)


JSON_SUPPORTED = {"SPC", "FBOS", "SBS", "ECB", "UNICEF", "OECD", "ILO", "ABS", "BIS"}
JSON_UNSUPPORTED = {"IMF", "ESTAT", "STATSNZ"}


def test_json_support_matches_verified_matrix():
    by_key = {ep.key: ep for ep in ENDPOINTS}
    for key in JSON_SUPPORTED:
        assert by_key[key].json_accept is not None, key
        assert "+json" in by_key[key].json_accept, key
        assert by_key[key].json_url == by_key[key].data_url, key
    for key in JSON_UNSUPPORTED:
        assert by_key[key].json_accept is None, key
        assert by_key[key].json_unsupported_reason, key
        assert by_key[key].json_url is None, key


def test_ecb_uses_the_draft_json_variant():
    """Plain version=1.0.0 is a 406 from ECB; only the -wd draft works."""
    (ecb,) = [ep for ep in ENDPOINTS if ep.key == "ECB"]
    assert ecb.json_accept == "application/vnd.sdmx.data+json;version=1.0.0-wd"


def test_statsnz_now_has_a_pinned_data_query():
    (statsnz,) = [ep for ep in ENDPOINTS if ep.key == "STATSNZ"]
    assert statsnz.data_path is not None
    assert "AGR_AGR_001" in statsnz.data_path
