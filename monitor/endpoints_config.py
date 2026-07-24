"""Static endpoint configuration for the SDMx MCP gateway health monitor.

The monitor keeps its own endpoint list on purpose: pinned known-good queries
are a monitoring judgment, and importing gateway code would let a shared bug
pass on both sides of a comparison. Keys match the gateway's endpoint keys so
the drift guard can compare the two lists at runtime.

Every pinned path below was live-verified on 2026-07-24 (see the plan and
spec in docs/superpowers/ at the workspace root). Notes:
- UNICEF/IMF/OECD/ESTAT/ILO/ABS data paths are filtered slices because their
  unfiltered flows are huge, hang, or (ESTAT) go async.
- ESTAT/OECD/ILO/ABS metadata paths fetch a single dataflow because their
  full stub lists are 0.8-7 MB.
- STATSNZ requires a subscription key and has no pinned data query yet; its
  data checks are recorded as skipped.
"""

import os
from dataclasses import dataclass

SDMX_CSV = "application/vnd.sdmx.data+csv;version=1.0.0"


@dataclass(frozen=True)
class Endpoint:
    key: str
    name: str
    base_url: str
    agency_id: str
    metadata_path: str
    data_path: str | None
    data_accept: str = SDMX_CSV
    requires_env: str | None = None
    auth_header: str | None = None

    @property
    def metadata_url(self) -> str:
        return self.base_url + self.metadata_path

    @property
    def data_url(self) -> str | None:
        if self.data_path is None:
            return None
        return self.base_url + self.data_path

    @property
    def credentials_missing(self) -> bool:
        return self.requires_env is not None and not os.getenv(self.requires_env)

    def auth_headers(self) -> dict[str, str]:
        if self.requires_env is None or self.auth_header is None:
            return {}
        value = os.getenv(self.requires_env)
        if not value:
            return {}
        return {self.auth_header: value}


ENDPOINTS: list[Endpoint] = [
    Endpoint(
        key="SPC",
        name="Pacific Data Hub",
        base_url="https://stats-sdmx-disseminate.pacificdata.org/rest",
        agency_id="SPC",
        metadata_path="/dataflow/SPC/all/latest?detail=allstubs",
        data_path="/data/SPC,DF_ADBKI/all?firstNObservations=1",
    ),
    Endpoint(
        key="FBOS",
        name="Fiji Bureau of Statistics",
        base_url="https://data-sdmx-disseminate.statsfiji.gov.fj/rest",
        agency_id="FBOS",
        metadata_path="/dataflow/FBOS/all/latest?detail=allstubs",
        data_path="/data/FBOS,DF_BOP_TABLE1/all?firstNObservations=1",
    ),
    Endpoint(
        key="SBS",
        name="Samoa Bureau of Statistics",
        base_url="https://data-sdmx-disseminate.sbs.gov.ws/rest",
        agency_id="SBS",
        metadata_path="/dataflow/SBS/all/latest?detail=allstubs",
        data_path="/data/SBS,DF_CPI/all?firstNObservations=1",
    ),
    Endpoint(
        key="ECB",
        name="European Central Bank",
        base_url="https://data-api.ecb.europa.eu/service",
        agency_id="ECB",
        metadata_path="/dataflow/ECB/all/latest?detail=allstubs",
        data_path="/data/EXR/D.USD.EUR.SP00.A?lastNObservations=1",
        data_accept="text/csv",
    ),
    Endpoint(
        key="UNICEF",
        name="UNICEF",
        base_url="https://sdmx.data.unicef.org/ws/public/sdmxapi/rest",
        agency_id="UNICEF",
        metadata_path="/dataflow/UNICEF/all/latest?detail=allstubs",
        data_path="/data/UNICEF,GLOBAL_DATAFLOW/ALB.CME_MRY0T4._T?firstNObservations=1",
    ),
    Endpoint(
        key="IMF",
        name="International Monetary Fund",
        base_url="https://api.imf.org/external/sdmx/2.1",
        agency_id="IMF.STA",
        metadata_path="/dataflow/IMF.STA/all/latest?detail=allstubs",
        data_path="/data/IMF.STA,CPI/ABW.CPI.CP01.IX.A?firstNObservations=1",
    ),
    Endpoint(
        key="OECD",
        name="OECD",
        base_url="https://sdmx.oecd.org/public/rest",
        agency_id="OECD",
        metadata_path="/dataflow/OECD.SDD.TPS/DSD_PRICES@DF_PRICES_HICP",
        data_path="/data/OECD.SDD.TPS,DSD_PRICES@DF_PRICES_HICP/FRA.M......?firstNObservations=1",
    ),
    Endpoint(
        key="ESTAT",
        name="Eurostat",
        base_url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1",
        agency_id="ESTAT",
        metadata_path="/dataflow/ESTAT/nama_10_gdp",
        data_path="/data/nama_10_gdp/A.CP_MEUR.B1GQ.EL?firstNObservations=1",
    ),
    Endpoint(
        key="ILO",
        name="International Labour Organization",
        base_url="https://sdmx.ilo.org/rest",
        agency_id="ILO",
        metadata_path="/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest",
        data_path="/data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1",
    ),
    Endpoint(
        key="ABS",
        name="Australian Bureau of Statistics",
        base_url="https://data.api.abs.gov.au/rest",
        agency_id="ABS",
        metadata_path="/dataflow/ABS/CPI/latest",
        data_path="/data/ABS,CPI/1.10001.10.50.Q?firstNObservations=1",
    ),
    Endpoint(
        key="BIS",
        name="Bank for International Settlements",
        base_url="https://stats.bis.org/api/v1",
        agency_id="BIS",
        metadata_path="/dataflow/BIS/all/latest?detail=allstubs",
        data_path="/data/WS_CBPOL/all/all?firstNObservations=1",
    ),
    Endpoint(
        key="STATSNZ",
        name="Stats NZ (Aotearoa Data Explorer)",
        base_url="https://api.data.stats.govt.nz/rest",
        agency_id="STATSNZ",
        metadata_path="/dataflow/STATSNZ/all/latest?format=xml",
        data_path=None,
        requires_env="SDMX_STATSNZ_KEY",
        auth_header="Ocp-Apim-Subscription-Key",
    ),
]

ENDPOINT_KEYS: list[str] = [ep.key for ep in ENDPOINTS]
