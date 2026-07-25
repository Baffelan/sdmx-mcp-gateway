from contracts_config import EXPECTATIONS, REFERENCE_PROBES
from endpoints_config import ENDPOINT_KEYS


def test_every_endpoint_has_expectations():
    assert set(EXPECTATIONS) == set(ENDPOINT_KEYS)


def test_reference_probes_cover_the_standard_keywords():
    assert REFERENCE_PROBES == (
        "none", "children", "parents", "parentsandsiblings",
        "descendants", "all", "contentconstraint",
    )


def test_every_expectation_covers_every_reference_probe():
    for key, exp in EXPECTATIONS.items():
        assert set(exp.references) == set(REFERENCE_PROBES), key


def test_estat_rejects_parents_and_all():
    """Verified 2026-07-25: Eurostat answers 400 with error 140 for these."""
    estat = EXPECTATIONS["ESTAT"]
    assert estat.references["parents"] is False
    assert estat.references["parentsandsiblings"] is False
    assert estat.references["all"] is False
    assert estat.references["contentconstraint"] is False
    assert estat.references["children"] is True
    assert estat.references["descendants"] is True


def test_known_constraint_endpoint_statuses():
    assert EXPECTATIONS["SPC"].availableconstraint_status == 200
    assert EXPECTATIONS["ECB"].availableconstraint_status == 404
    assert EXPECTATIONS["ESTAT"].availableconstraint_status == 405
    assert EXPECTATIONS["ILO"].availableconstraint_status == 500


def test_imf_missing_artefact_baseline_is_204():
    """IMF answers 204 where every other provider answers 404."""
    assert EXPECTATIONS["IMF"].missing_artefact_status == 204
    others = [e for k, e in EXPECTATIONS.items() if k != "IMF"]
    assert all(e.missing_artefact_status == 404 for e in others)


def test_no_provider_serves_the_sdmx3_structure_path_yet():
    assert all(e.sdmx3_status != 200 for e in EXPECTATIONS.values())


def test_constraint_types_match_the_configured_strategies():
    assert EXPECTATIONS["ECB"].constraint_type == "Allowed"
    assert EXPECTATIONS["SPC"].constraint_type == "Actual"
    assert EXPECTATIONS["ESTAT"].constraint_type is None


def test_only_statsnz_requires_auth_for_listing():
    assert EXPECTATIONS["STATSNZ"].auth_required_for_listing is True
    others = [e for k, e in EXPECTATIONS.items() if k != "STATSNZ"]
    assert all(e.auth_required_for_listing is False for e in others)
