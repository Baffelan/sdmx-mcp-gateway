from sdmx_spec import (
    DETAIL_VALUES,
    DOCUMENTED_STATUS,
    REFERENCES_VALUES,
    classify_status,
    is_legal_reference,
)


def test_reference_enum_matches_the_standard():
    for value in ("none", "parents", "parentsandsiblings", "children",
                  "descendants", "all", "contentconstraint"):
        assert is_legal_reference(value), value
    assert not is_legal_reference("sideways")
    assert "codelist" in REFERENCES_VALUES  # specific resource types are legal too


def test_detail_enum_matches_the_standard():
    """sdmx-2.1-rest.yaml line 2204 lists exactly these six values."""
    assert DETAIL_VALUES == {
        "allstubs", "referencestubs", "referencepartial",
        "allcompletestubs", "referencecompletestubs", "full",
    }


def test_204_is_not_a_documented_status():
    """The 2.1 spec documents 200/304/4xx/5xx but never 204, which is why
    IMF answering 204 for a missing artefact leaves a client unable to tell
    'missing' from 'empty'."""
    assert 204 not in DOCUMENTED_STATUS
    assert 404 in DOCUMENTED_STATUS
    assert 200 in DOCUMENTED_STATUS


def test_classify_status_flags_undocumented_codes():
    verdict, note = classify_status(204, legal_request=True)
    assert verdict == "deviates"
    assert "204" in note


def test_classify_status_flags_rejection_of_a_legal_request():
    """ESTAT rejects references=parents, which the standard lists as valid."""
    verdict, note = classify_status(400, legal_request=True)
    assert verdict == "deviates"
    assert note and "rejected" in note


def test_classify_status_accepts_success_and_expected_rejections():
    assert classify_status(200, legal_request=True) == ("conforms", None)
    # rejecting a request the standard does not define is not a deviation
    assert classify_status(400, legal_request=False)[0] == "conforms"


def test_server_error_on_a_legal_request_deviates():
    verdict, note = classify_status(500, legal_request=True)
    assert verdict == "deviates"
    assert note and "500" in note


def test_not_modified_conforms_for_a_legal_request():
    """304 is documented and is not a rejection; it must not be read as one."""
    assert classify_status(304, legal_request=True) == ("conforms", None)
