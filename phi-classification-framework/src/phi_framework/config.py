"""Job configuration parsed from job parameters."""
import argparse
from dataclasses import dataclass
from typing import Sequence, Tuple

from . import constants as c


@dataclass(frozen=True)
class FrameworkConfig:
    env: str
    catalog: str
    governance_schema: str
    tag_key: str
    process_name: str
    run_id: str
    allowed_catalogs: Tuple[str, ...]
    job_names: Tuple[str, ...]
    app_process_name: str

    def table(self, name: str) -> str:
        return f"`{self.catalog}`.`{self.governance_schema}`.`{name}`"

    @property
    def schema_fqn(self) -> str:
        return f"`{self.catalog}`.`{self.governance_schema}`"


def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--src_root", help="Bundle src directory, put on sys.path by the job entry point.")
    p.add_argument("--env", required=True, choices=c.ENVIRONMENTS)
    p.add_argument("--catalog", required=True, help="Catalog holding the governance schema.")
    p.add_argument("--governance_schema", required=True)
    p.add_argument("--tag_key", required=True, help="Governed tag key, e.g. phi_classification.")
    p.add_argument("--process_name", required=True, help="Logical job name written to audit columns.")
    p.add_argument("--run_id", required=True, help="Set to {{job.run_id}}.")
    p.add_argument(
        "--allowed_catalogs",
        required=True,
        help="Comma-separated catalogs this environment may scan or tag (environment guardrail).",
    )
    return p


def from_args(args: argparse.Namespace) -> FrameworkConfig:
    env = args.env
    allowed = tuple(x.strip() for x in args.allowed_catalogs.split(",") if x.strip())
    if not allowed:
        raise ValueError("--allowed_catalogs must list at least one catalog")
    return FrameworkConfig(
        env=env,
        catalog=args.catalog,
        governance_schema=args.governance_schema,
        tag_key=args.tag_key,
        process_name=args.process_name,
        run_id=str(args.run_id),
        allowed_catalogs=allowed,
        job_names=job_names(env),
        app_process_name=f"{env}_phi_review_app",
    )


def job_names(env: str) -> Tuple[str, ...]:
    return (
        f"{env}_phi_classification_scan",
        f"{env}_phi_decision_ingestion",
        f"{env}_phi_classification_application",
    )


def parse(description: str, argv: Sequence[str] = None, extra=None):
    """Parse the shared arguments plus any job-specific ones added by `extra(parser)`."""
    parser = base_parser(description)
    if extra:
        extra(parser)
    args = parser.parse_args(argv)
    cfg = from_args(args)
    if cfg.process_name not in cfg.job_names + (f"{cfg.env}_phi_framework_setup",):
        raise ValueError(f"process_name {cfg.process_name!r} is not a framework job for {cfg.env}")
    return cfg, args
