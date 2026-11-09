from dataclasses import dataclass, field

CKAN = "https://opendata.nhsbsa.net/api/3/action"

DATASETS = {
    "legacy": "english-prescribing-data-epd",
    "snomed": "english-prescribing-dataset-epd-with-snomed-code",
}


@dataclass(frozen=True)
class Config:
    start_month: str                      # inclusive, YYYYMM
    end_month: str                        # inclusive, YYYYMM
    catalog: str = "workspace"
    schema: str = "epd"
    volume: str = "raw"
    # The original EPD stops at June 2025; later months are in the SNOMED version.
    snomed_from: str = "202507"
    datasets: dict = field(default_factory=lambda: dict(DATASETS))

    @property
    def vol(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def silver(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_epd"

    @property
    def bronze_list_size(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_list_size"

    @property
    def silver_list_size(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_list_size"

    @property
    def bronze_ods(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_ods"

    @property
    def dim_practice(self) -> str:
        return f"{self.catalog}.{self.schema}.dim_practice"

    @property
    def dim_practice_succession(self) -> str:
        return f"{self.catalog}.{self.schema}.dim_practice_succession"

    @property
    def bronze_onspd(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_onspd"

    @property
    def silver_onspd(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_onspd"

    @property
    def bronze_imd(self) -> str:
        return f"{self.catalog}.{self.schema}.bronze_imd"

    @property
    def silver_imd(self) -> str:
        return f"{self.catalog}.{self.schema}.silver_imd"

    @property
    def gold_practice_month(self) -> str:
        return f"{self.catalog}.{self.schema}.gold_practice_month"

    @property
    def gold_bnf_chapter_month(self) -> str:
        return f"{self.catalog}.{self.schema}.gold_bnf_chapter_month"

    def bronze(self, src: str) -> str:
        return f"{self.catalog}.{self.schema}.bronze_epd_{src}"

    def landing(self, src: str) -> str:
        return f"{self.vol}/landing/{src}"

    def months(self) -> list[str]:
        """Every YYYYMM from start_month to end_month inclusive."""
        out, y, m = [], int(self.start_month[:4]), int(self.start_month[4:])
        while f"{y}{m:02d}" <= self.end_month:
            out.append(f"{y}{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out

    def source_for(self, month: str) -> str:
        return "legacy" if month < self.snomed_from else "snomed"
