"""What the gateway assumes about each provider's API behaviour.

Every value here was live-verified on 2026-07-25 by probing the provider.
These are behaviours, not content: which query forms are accepted, what a
missing artefact returns, whether authentication is demanded. Content (which
dataflows exist, what dimensions they have) changes constantly and is
deliberately not tracked.

Where `config.py` in the gateway declares an assumption (references_support,
constraints, auth), this file mirrors it so a drift between the gateway's
belief and the provider's behaviour becomes visible.
"""

from dataclasses import dataclass

REFERENCE_PROBES: tuple[str, ...] = (
    "none", "children", "parents", "parentsandsiblings",
    "descendants", "all", "contentconstraint",
)

_ALL_REFERENCES_WORK = dict.fromkeys(REFERENCE_PROBES, True)


@dataclass(frozen=True)
class ContractExpectation:
    key: str
    flow_agency: str
    flow_id: str
    references: dict[str, bool]
    availableconstraint_status: int
    sdmx3_status: int
    missing_artefact_status: int
    constraint_type: str | None
    auth_required_for_listing: bool = False
    # Whether /availableconstraint/ carries the obs_count annotation, which
    # reports how many observations a selection holds without fetching them.
    # A .Stat Suite extension, not SDMx 2.1, so it varies by provider.
    # None means "record but do not judge", used where it is unverified.
    # Live-probed 2026-07-31.
    obs_count_annotation: bool | None = None


EXPECTATIONS: dict[str, ContractExpectation] = {
    "SPC": ContractExpectation(
        key="SPC", flow_agency="SPC", flow_id="DF_ADBKI",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=404,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=True,
    ),
    "FBOS": ContractExpectation(
        key="FBOS", flow_agency="FBOS", flow_id="DF_BOP_TABLE1",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=True,
    ),
    "SBS": ContractExpectation(
        key="SBS", flow_agency="SBS", flow_id="DF_CPI",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=404,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=True,
    ),
    "ECB": ContractExpectation(
        key="ECB", flow_agency="ECB", flow_id="EXR",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=404, sdmx3_status=404,
        missing_artefact_status=404, constraint_type="Allowed",
    ),
    "UNICEF": ContractExpectation(
        key="UNICEF", flow_agency="UNICEF", flow_id="GLOBAL_DATAFLOW",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=False,
    ),
    "IMF": ContractExpectation(
        key="IMF", flow_agency="IMF.STA", flow_id="CPI",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=404,
        missing_artefact_status=204, constraint_type="Actual",
        obs_count_annotation=False,
    ),
    "OECD": ContractExpectation(
        key="OECD", flow_agency="OECD.SDD.TPS", flow_id="DSD_PRICES@DF_PRICES_HICP",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=True,
    ),
    "ESTAT": ContractExpectation(
        key="ESTAT", flow_agency="ESTAT", flow_id="nama_10_gdp",
        references={
            "none": True,
            "children": True,
            "parents": False,
            "parentsandsiblings": False,
            "descendants": True,
            "all": False,
            "contentconstraint": False,
        },
        availableconstraint_status=405, sdmx3_status=405,
        missing_artefact_status=404, constraint_type=None,
    ),
    "ILO": ContractExpectation(
        key="ILO", flow_agency="ILO", flow_id="DF_GED_XLU1_SEX_HHT_CHL_RT",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=500, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
    ),
    "ABS": ContractExpectation(
        key="ABS", flow_agency="ABS", flow_id="CPI",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=True,
    ),
    "BIS": ContractExpectation(
        key="BIS", flow_agency="BIS", flow_id="WS_CBPOL",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
        obs_count_annotation=False,
    ),
    "STATSNZ": ContractExpectation(
        key="STATSNZ", flow_agency="STATSNZ", flow_id="AGR_AGR_001",
        references=dict(_ALL_REFERENCES_WORK),
        availableconstraint_status=200, sdmx3_status=400,
        missing_artefact_status=404, constraint_type="Actual",
        auth_required_for_listing=True,
    ),
}
