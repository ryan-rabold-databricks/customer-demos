"""Plain-language wording and action mapping for the review app.

Pure functions with no web or Databricks dependencies, so the wording rules are unit-tested.
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from phi_framework import constants as c

# Short labels for the HIPAA Safe Harbor identifiers; unknown values display as-is.
VALUE_LABELS = {
    "name": "Names",
    "address": "Geographic subdivisions smaller than a state (street, city, county, ZIP)",
    "date": "Dates (except year) tied to an individual; ages over 89",
    "phone_number": "Phone numbers",
    "fax_number": "Fax numbers",
    "email_address": "Email addresses",
    "ssn": "Social Security numbers",
    "mrn": "Medical record numbers",
    "health_plan_beneficiary_number": "Health plan beneficiary numbers",
    "account_number": "Account numbers",
    "cert_license_number": "Certificate or license numbers",
    "vehicle_identifier": "Vehicle identifiers and serial numbers, including license plates",
    "device_identifier": "Device identifiers and serial numbers",
    "web_url": "Web URLs",
    "ip_address": "IP addresses",
    "biometric_identifier": "Biometric identifiers, including finger and voice prints",
    "face_photo_image": "Full-face photographs and comparable images",
    "unique_identifier_code": "Any other unique identifying number, characteristic, or code",
}

STATUS_LABELS = {
    c.PENDING_STEWARD_REVIEW: ("Needs a decision", "todo"),
    c.VALIDATION_FAILED: ("Rejected: fix and resubmit", "bad"),
    c.NEEDS_PRIVACY_REVIEW: ("Waiting for Privacy", "wait"),
    c.PLANNED: ("Accepted: applying", "wait"),
    c.APPLICATION_IN_PROGRESS: ("Accepted: applying", "wait"),
    c.APPLICATION_FAILED: ("Accepted, but tagging failed: Data Platform is fixing it", "bad"),
    c.TAG_VERIFIED: ("Done: tagged", "done"),
    c.ATTESTED_NOT_PHI: ("Done: confirmed not PHI", "done"),
    c.CLOSED_NO_ACTION: ("Closed: column removed or out of scope", "done"),
}

EVENT_LABELS = {
    c.BATCH_CREATED: "Review batch created",
    c.ITEM_OPENED: "Opened for review",
    c.ITEM_CLOSED: "Closed: column removed or out of scope",
    c.SUBMISSION_INGESTED: "Submission processed",
    c.STEWARD_ASSIGNED: "Steward assigned",
    c.EVT_DECISION_RECEIVED: "Decision received",
    c.EVT_VALIDATION_FAILED: "Decision rejected",
    c.CHANGE_IGNORED_ALREADY_RESOLVED: "Later change ignored (already decided)",
    c.PRIVACY_REVIEW_REQUESTED: "Sent to Privacy",
    c.PLAN_CREATED: "Tag action approved",
    c.ATTESTATION_RECORDED: "Not-PHI attestation recorded",
    c.EVT_TAG_VERIFIED: "Tag applied and verified",
    c.TAG_REMOVED: "Tag removed and verified",
    c.EVT_APPLICATION_FAILED: "Tag application failed",
    c.BATCH_CLOSED_EVT: "Batch closed",
    c.BATCH_ERROR: "Batch error",
}

DECISION_LABELS = {
    c.APPROVE_SUGGESTION: "PHI (suggested value)",
    c.CORRECT_CLASSIFICATION: "PHI",
    c.CONFIRM_NOT_PHI: "Not PHI",
    c.REQUEST_PRIVACY_REVIEW: "Ask Privacy",
}

# The three choices the app offers, mapped to framework decisions.
ACTION_PHI = "phi"
ACTION_NOT_PHI = "not_phi"
ACTION_PRIVACY = "privacy"


def value_label(value: Optional[str]) -> str:
    if not value:
        return ""
    label = VALUE_LABELS.get(value)
    return f"{value}: {label}" if label else value


def status_label(status: str) -> Tuple[str, str]:
    """(text, tone) for a status; tone is one of todo, bad, wait, done."""
    return STATUS_LABELS.get(status, (status, "wait"))


def _date(ts: Optional[datetime]) -> str:
    return ts.strftime("%b %-d, %Y") if ts else "an earlier review"


def issue_sentence(item: dict, prior: Optional[dict] = None, correction_reason: Optional[str] = None) -> str:
    """Why this column is in the queue, in one sentence."""
    issue = item["issue_type"]
    prior = prior or {}
    when = _date(prior.get("applied_at") or prior.get("reviewed_at"))
    current = item.get("current_tag_value")
    if issue == c.MISSING_CLASSIFICATION:
        return "Not classified yet: the column has no PHI tag and no not-PHI confirmation."
    if issue == c.INVALID_TAG_VALUE:
        return f"Tagged '{current}', which is not an allowed value. Choose a valid value or confirm it is not PHI."
    if issue == c.STALE_NON_PHI_ATTESTATION:
        return f"Its type or description changed since it was confirmed not PHI on {when}."
    if issue == c.STALE_PHI_REVIEW:
        return f"Its type or description changed since it was tagged '{current}' on {when}."
    if issue == c.ORPHANED_TAG:
        return f"Tagged '{current}' outside the review process. Confirm the value, correct it, or confirm it is not PHI."
    if issue == c.RECERTIFICATION_DUE:
        months = c.RISK_TIER_MONTHS.get(item.get("risk_tier"), 0)
        what = f"tagged '{current}'" if current else "confirmed not PHI"
        return (f"Periodic recertification: last {what} on {when}. "
                f"{item.get('risk_tier', '').title()} scopes are recertified every {months} months.")
    if issue == c.CORRECTION_REQUESTED:
        reason = f": {correction_reason}" if correction_reason else "."
        return f"Data Governance reopened this column for a new decision{reason}"
    return issue


def decision_for(action: str, tag_value: str, suggested: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Map an app action to (decision, corrected_tag_value)."""
    if action == ACTION_PHI:
        if suggested and tag_value == suggested:
            return c.APPROVE_SUGGESTION, None
        return c.CORRECT_CLASSIFICATION, tag_value or None
    if action == ACTION_NOT_PHI:
        return c.CONFIRM_NOT_PHI, None
    if action == ACTION_PRIVACY:
        return c.REQUEST_PRIVACY_REVIEW, None
    return None, None


def submission_outcome(row: dict) -> Tuple[str, str]:
    """(text, tone) describing what happened to one of the user's submissions."""
    if row.get("outcome") is None:
        return "Waiting for validation", "wait"
    if row.get("applied") and row["submission_type"] == c.SUBMISSION_DECISION:
        # The item records this submission as its decision, whatever the ingestion event says.
        return status_label(row["item_status"])
    if row.get("ignored"):
        return "Ignored: the item had already been decided", "bad"
    if row.get("error"):
        return f"Rejected: {row['error']}", "bad"
    outcome = row["outcome"] or ""
    if outcome.startswith("rejected") or outcome.startswith("ignored"):
        return outcome.capitalize(), "bad"
    if row["submission_type"] == c.SUBMISSION_ASSIGNMENT:
        return "Assignment recorded", "done"
    # Accepted: report the item's current state, which moves on after tag application.
    text, tone = status_label(row["item_status"])
    if row["item_status"] == c.VALIDATION_FAILED:
        return "Accepted, but a later decision was rejected", "bad"
    return text, tone


def group_by_table(items: List[dict]) -> List[Tuple[str, List[dict]]]:
    """Group items by catalog.schema.table, keeping the order of first appearance."""
    groups: Dict[str, List[dict]] = {}
    for i in items:
        groups.setdefault(f"{i['catalog_name']}.{i['schema_name']}.{i['table_name']}", []).append(i)
    return list(groups.items())
