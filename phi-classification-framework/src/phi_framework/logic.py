"""Pure workflow rules: issue detection, scope precedence, decision validation, and tag DDL.

Nothing here touches Spark or Databricks APIs, so every rule is unit-tested locally.
"""
import calendar
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from . import constants as c


def add_months(ts: datetime, months: int) -> datetime:
    """Add calendar months, clamping the day to the end of shorter months."""
    month_index = ts.month - 1 + months
    year = ts.year + month_index // 12
    month = month_index % 12 + 1
    day = min(ts.day, calendar.monthrange(year, month)[1])
    return ts.replace(year=year, month=month, day=day)


# ---------------------------------------------------------------- scope precedence


@dataclass(frozen=True)
class Scope:
    scope_id: str
    catalog_name: str
    schema_name: str  # A schema name, or "*" for every schema in the catalog.
    risk_tier: str
    steward_group: str


def find_scope_conflicts(scopes: Sequence[Scope]) -> List[Tuple[str, List[str]]]:
    """Return (description, scope_ids) for every catalog/schema covered by more than one active row."""
    seen: Dict[Tuple[str, str], List[str]] = {}
    for s in scopes:
        seen.setdefault((s.catalog_name.lower(), s.schema_name.lower()), []).append(s.scope_id)
    return [
        (f"{cat}.{sch}", sorted(ids)) for (cat, sch), ids in sorted(seen.items()) if len(ids) > 1
    ]


def resolve_scope(scopes: Sequence[Scope], catalog: str, schema: str) -> Optional[Scope]:
    """An explicit schema row overrides the catalog's '*' row."""
    explicit = wildcard = None
    for s in scopes:
        if s.catalog_name.lower() != catalog.lower():
            continue
        if s.schema_name.lower() == schema.lower():
            explicit = s
        elif s.schema_name == "*":
            wildcard = s
    return explicit or wildcard


# ---------------------------------------------------------------- issue detection


@dataclass(frozen=True)
class VerifiedReview:
    """The column's most recent TAG_VERIFIED item."""

    column_fingerprint: str
    reviewed_at: datetime


@dataclass(frozen=True)
class ActiveAttestation:
    column_fingerprint: str
    expires_at: datetime


def classify_column(
    fingerprint: str,
    tag_value: Optional[str],
    allowed_values: Iterable[str],
    verified: Optional[VerifiedReview],
    attestation: Optional[ActiveAttestation],
    correction_requested: bool,
    recert_months: int,
    now: datetime,
) -> Optional[str]:
    """Return the single issue type for a column, or None when no review is needed.

    Precedence follows constants.ISSUE_TYPES.
    """
    if correction_requested:
        return c.CORRECTION_REQUESTED
    if tag_value is not None:
        if tag_value not in set(allowed_values):
            return c.INVALID_TAG_VALUE
        if verified is not None and verified.column_fingerprint != fingerprint:
            return c.STALE_PHI_REVIEW
        if verified is None:
            return c.ORPHANED_TAG
        if add_months(verified.reviewed_at, recert_months) <= now:
            return c.RECERTIFICATION_DUE
        return None
    if attestation is not None:
        if attestation.column_fingerprint != fingerprint:
            return c.STALE_NON_PHI_ATTESTATION
        if attestation.expires_at <= now:
            return c.RECERTIFICATION_DUE
        return None
    return c.MISSING_CLASSIFICATION


def suggest_value(
    issue_type: str,
    tag_value: Optional[str],
    allowed_values: Iterable[str],
    detections: Sequence[dict],
    mapping: Dict[str, str],
) -> Tuple[Optional[str], Optional[str]]:
    """Return (suggested_tag_value, suggestion_reason).

    Reconfirmation issues pre-fill the current valid tag. Otherwise the suggestion comes from
    high-confidence Data Classification detections through the approved mapping; no suggestion is
    made when there is no mapping, only low-confidence detections, or conflicting detections.
    """
    allowed = set(allowed_values)
    if (
        issue_type in (c.RECERTIFICATION_DUE, c.CORRECTION_REQUESTED, c.STALE_PHI_REVIEW)
        and tag_value in allowed
    ):
        return tag_value, "Current tag value"
    high = [d for d in detections if str(d.get("confidence", "")).upper() == "HIGH"]
    if high:
        latest = max(d.get("latest_detected_time") or datetime.min for d in high)
        high = [d for d in high if (d.get("latest_detected_time") or datetime.min) == latest]
    mapped = {mapping.get(d.get("class_tag")) for d in high} - {None}
    mapped &= allowed
    if len(mapped) != 1:
        return None, None
    value = mapped.pop()
    d = next(d for d in high if mapping.get(d.get("class_tag")) == value)
    reason = f"{d.get('class_tag')}; HIGH; {d.get('frequency')}"
    return value, reason


# ---------------------------------------------------------------- decision validation


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    error: Optional[str] = None


def validate_decision(
    decision: Optional[str],
    item_status: str,
    suggested_tag_value: Optional[str],
    corrected_tag_value: Optional[str],
    steward_comment: Optional[str],
    allowed_values: Iterable[str],
) -> ValidationResult:
    allowed = set(allowed_values)
    comment = (steward_comment or "").strip()
    if decision not in c.DECISIONS:
        return ValidationResult(False, f"Unknown decision {decision!r}.")
    if decision == c.APPROVE_SUGGESTION:
        if suggested_tag_value is None:
            return ValidationResult(False, "There is no suggested value to approve.")
        if suggested_tag_value not in allowed:
            return ValidationResult(False, f"Suggested value {suggested_tag_value!r} is not allowed.")
    elif decision == c.CORRECT_CLASSIFICATION:
        if not corrected_tag_value:
            return ValidationResult(False, "A corrected value is required.")
        if corrected_tag_value not in allowed:
            return ValidationResult(False, f"Corrected value {corrected_tag_value!r} is not allowed.")
    elif decision == c.CONFIRM_NOT_PHI:
        if not comment:
            return ValidationResult(False, "A rationale is required to confirm the column is not PHI.")
        if corrected_tag_value:
            return ValidationResult(False, "Leave the corrected value empty when confirming not PHI.")
    elif decision == c.REQUEST_PRIVACY_REVIEW:
        if item_status == c.NEEDS_PRIVACY_REVIEW:
            return ValidationResult(False, "The item is already in Privacy review.")
        if not comment:
            return ValidationResult(False, "Explain the question for Privacy review.")
    return ValidationResult(True)


def authorize_reviewer(
    submitter_role: str, submitted_by: str, assigned_steward: Optional[str], item_status: str
) -> ValidationResult:
    """Privacy outcomes are recorded by Data Governance; other decisions by the assigned steward
    or a member of the scope's steward group (the app verifies group membership)."""
    if submitter_role not in c.SUBMITTER_ROLES:
        return ValidationResult(False, f"Unknown submitter role {submitter_role!r}.")
    if item_status == c.NEEDS_PRIVACY_REVIEW and submitter_role != c.ROLE_DATA_GOVERNANCE:
        return ValidationResult(False, "Only Data Governance records Privacy review outcomes.")
    if submitter_role == c.ROLE_ASSIGNED_STEWARD and (
        assigned_steward is None or assigned_steward.lower() != submitted_by.lower()
    ):
        return ValidationResult(False, "Submitter is not the assigned steward.")
    return ValidationResult(True)


# ---------------------------------------------------------------- tag DDL


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def quote_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def render_tag_ddl(
    action_type: str,
    table_type: str,
    catalog: str,
    schema: str,
    table: str,
    column: str,
    tag_key: str,
    tag_value: Optional[str],
) -> str:
    if table_type not in c.TABLE_TYPE_ALTER:
        raise ValueError(f"Unsupported table_type {table_type!r}")
    target = ".".join(quote_ident(p) for p in (catalog, schema, table))
    prefix = f"ALTER {c.TABLE_TYPE_ALTER[table_type]} {target} ALTER COLUMN {quote_ident(column)}"
    if action_type == c.SET_PHI_TAG:
        if not tag_value:
            raise ValueError("SET_PHI_TAG requires a tag value")
        return f"{prefix} SET TAGS ({quote_literal(tag_key)} = {quote_literal(tag_value)})"
    if action_type == c.REMOVE_PHI_TAG:
        return f"{prefix} UNSET TAGS ({quote_literal(tag_key)})"
    raise ValueError(f"Unsupported action_type {action_type!r}")
