"""App settings from environment variables set in the bundle's app config."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    warehouse_id: str
    catalog: str
    governance_schema: str
    tag_key: str
    env: str
    governance_group: str
    scan_job_id: str

    @property
    def process_name(self) -> str:
        return f"{self.env}_phi_review_app"

    def table(self, name: str) -> str:
        return f"`{self.catalog}`.`{self.governance_schema}`.`{name}`"


def load() -> Settings:
    def need(name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            raise RuntimeError(f"Environment variable {name} is required")
        return value

    return Settings(
        warehouse_id=need("DATABRICKS_WAREHOUSE_ID"),
        catalog=need("PHI_CATALOG"),
        governance_schema=need("PHI_GOVERNANCE_SCHEMA"),
        tag_key=need("PHI_TAG_KEY"),
        env=need("PHI_ENV"),
        governance_group=need("PHI_GOVERNANCE_GROUP"),
        scan_job_id=os.getenv("PHI_SCAN_JOB_ID", "").strip(),
    )
