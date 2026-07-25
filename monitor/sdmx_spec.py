"""What the SDMx REST standard says, so observations can be judged against it.

Hand-extracted from the OpenAPI specifications kept at the workspace root:
`sdmx-2.1-rest.yaml` (the `references` and `detail` enums near line 2189-2244,
and the response codes documented on all 46 operations) and
`sdmx-3.0-rest.yaml` (the `/structure/{structureType}/...` path shape near
line 144). Extracting by hand keeps 3,300 lines of YAML out of the runtime;
the constants below are small and change only when the standard does.

The point of this module is attribution. When a provider's behaviour and the
gateway's assumption disagree, the standard decides whose problem it is.
"""

# sdmx-2.1-rest.yaml: structure query `references` enum.
_STRUCTURE_TYPES = frozenset({
    "datastructure", "metadatastructure", "categoryscheme", "conceptscheme",
    "codelist", "hierarchicalcodelist", "organisationscheme", "agencyscheme",
    "dataproviderscheme", "dataconsumerscheme", "organisationunitscheme",
    "dataflow", "metadataflow", "reportingtaxonomy", "provisionagreement",
    "structureset", "process", "categorisation", "contentconstraint",
    "actualconstraint", "allowedconstraint", "attachmentconstraint",
    "transformationscheme", "rulesetscheme", "userdefinedoperatorscheme",
    "customtypescheme", "namepersonalisationscheme", "namealiasscheme",
})
_REFERENCE_KEYWORDS = frozenset({
    "none", "parents", "parentsandsiblings", "children", "descendants", "all",
})
REFERENCES_VALUES = _REFERENCE_KEYWORDS | _STRUCTURE_TYPES

# sdmx-2.1-rest.yaml line 2204: structure query `detail` enum.
DETAIL_VALUES = frozenset({
    "allstubs", "referencestubs", "referencepartial", "allcompletestubs",
    "referencecompletestubs", "full",
})

# Every response code documented in sdmx-2.1-rest.yaml. 204 is absent. 510 is
# also excluded: 12 operations $ref it (e.g. line 1034) but
# components.responses never defines a '510' entry, so it has no description
# anywhere in the spec and cannot be treated as genuinely documented.
DOCUMENTED_STATUS = frozenset({
    200, 304, 400, 401, 403, 404, 406, 413, 414, 500, 501, 503,
})

# sdmx-3.0-rest.yaml: the 3.0 structure path shape.
SDMX3_STRUCTURE_PATH = "/structure/{structureType}/{agencyID}/{resourceID}/{version}"


def is_legal_reference(value: str) -> bool:
    return value in REFERENCES_VALUES


def classify_status(observed: int, *, legal_request: bool) -> tuple[str, str | None]:
    """Judge one observed status code against the standard.

    `legal_request` says whether the request we made is one the standard
    defines. Rejecting an undefined request is fine; rejecting a defined one,
    or answering with a code the standard never documents, is a deviation.
    """
    if observed not in DOCUMENTED_STATUS:
        return "deviates", ("responded " + str(observed)
                            + ", which the SDMx REST standard does not document")
    if not legal_request:
        return "conforms", None
    if observed == 200:
        return "conforms", None
    if 400 <= observed < 500:
        return "deviates", ("rejected a request the standard defines (HTTP "
                            + str(observed) + ")")
    if observed >= 500:
        return "deviates", ("server error on a request the standard defines (HTTP "
                            + str(observed) + ")")
    return "conforms", None
