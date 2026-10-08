"""Unit tests for the review app's wording and action mapping."""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from phi_framework import constants as c  # noqa: E402
from review_app import ui  # noqa: E402


def item(**kw):
    base = {"issue_type": c.MISSING_CLASSIFICATION, "current_tag_value": None, "risk_tier": "HIGH",
            "catalog_name": "cat", "schema_name": "sch", "table_name": "t", "column_name": "x"}
    base.update(kw)
    return base


def test_issue_sentences_are_plain_and_use_prior_dates():
    prior = {"applied_at": datetime(2026, 10, 8, 14, 30)}
    assert "Not classified yet" in ui.issue_sentence(item())
    assert "confirmed not PHI on Oct 8, 2026" in ui.issue_sentence(item(issue_type=c.STALE_NON_PHI_ATTESTATION), prior)
    assert "tagged 'name' on Oct 8, 2026" in ui.issue_sentence(
        item(issue_type=c.STALE_PHI_REVIEW, current_tag_value="name"), prior)
    recert = ui.issue_sentence(item(issue_type=c.RECERTIFICATION_DUE, current_tag_value="mrn"), prior)
    assert "every 3 months" in recert and "'mrn'" in recert
    assert "earlier review" in ui.issue_sentence(item(issue_type=c.STALE_NON_PHI_ATTESTATION))
    assert ui.issue_sentence(item(issue_type=c.CORRECTION_REQUESTED), None, "Wrong value").endswith(": Wrong value")


def test_every_issue_and_status_has_wording():
    for issue in c.ISSUE_TYPES:
        assert ui.issue_sentence(item(issue_type=issue)) != issue
    for status in c.ITEM_STATUSES:
        text, tone = ui.status_label(status)
        assert text != status and tone in ("todo", "bad", "wait", "done")
    for event in c.EVENT_TYPES:
        assert event in ui.EVENT_LABELS


def test_decision_mapping():
    assert ui.decision_for("phi", "name", "name") == (c.APPROVE_SUGGESTION, None)
    assert ui.decision_for("phi", "mrn", "name") == (c.CORRECT_CLASSIFICATION, "mrn")
    assert ui.decision_for("phi", "mrn", None) == (c.CORRECT_CLASSIFICATION, "mrn")
    assert ui.decision_for("not_phi", "", None) == (c.CONFIRM_NOT_PHI, None)
    assert ui.decision_for("privacy", "", None) == (c.REQUEST_PRIVACY_REVIEW, None)
    assert ui.decision_for("other", "", None) == (None, None)


def test_submission_outcomes():
    base = {"submission_type": c.SUBMISSION_DECISION, "item_status": c.TAG_VERIFIED, "ignored": False, "error": None}
    assert ui.submission_outcome({**base, "outcome": None})[0] == "Waiting for validation"
    assert ui.submission_outcome({**base, "outcome": "PLANNED"})[0] == "Done: tagged"
    assert ui.submission_outcome({**base, "outcome": "validation failed", "error": "Bad value"}) == ("Rejected: Bad value", "bad")
    assert ui.submission_outcome({**base, "outcome": "ignored: already accepted", "ignored": True})[1] == "bad"
    assert ui.submission_outcome({**base, "outcome": "rejected: not authorized"})[0].startswith("Rejected")
    assert ui.submission_outcome({**base, "submission_type": c.SUBMISSION_ASSIGNMENT, "outcome": "processed"})[0] == "Assignment recorded"
    # An ingestion retry can log "ignored" for a submission it had already applied; the item is authoritative.
    assert ui.submission_outcome({**base, "outcome": "ignored: already accepted", "ignored": False,
                                  "applied": True})[0] == "Done: tagged"


def test_group_by_table_keeps_order():
    rows = [item(table_name="b", column_name="1"), item(table_name="a", column_name="2"), item(table_name="b", column_name="3")]
    groups = ui.group_by_table(rows)
    assert [g for g, _ in groups] == ["cat.sch.b", "cat.sch.a"] and len(groups[0][1]) == 2
