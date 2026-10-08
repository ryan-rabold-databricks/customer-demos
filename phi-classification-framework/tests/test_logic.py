"""Unit tests for the pure framework rules. Run with: pytest tests/"""
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from phi_framework import constants as c  # noqa: E402
from phi_framework import ddl, logic  # noqa: E402
from phi_framework.config import FrameworkConfig, job_names  # noqa: E402
from phi_framework.keys import column_fingerprint, make_key  # noqa: E402

NOW = datetime(2026, 10, 8, 12, 0, 0)
ALLOWED = ("name", "mrn", "ssn", "date")
FP = "fp-current"


def classify(**kw):
    args = dict(fingerprint=FP, tag_value=None, allowed_values=ALLOWED, verified=None, attestation=None,
                correction_requested=False, recert_months=3, now=NOW)
    args.update(kw)
    return logic.classify_column(**args)


# ---------------------------------------------------------------- keys


def test_make_key_is_deterministic_and_prefixed():
    k = make_key("ITEM", ["cat", "sch", "tbl", "col", "BATCH-1"])
    assert k == make_key("ITEM", ["cat", "sch", "tbl", "col", "BATCH-1"])
    assert k.startswith("ITEM-") and len(k) == len("ITEM-") + 16 and k[5:] == k[5:].upper()


def test_make_key_distinguishes_null_from_empty_and_boundaries():
    assert make_key("E", [None]) != make_key("E", [""])
    assert make_key("E", ["ab", "c"]) != make_key("E", ["a", "bc"])


def test_fingerprint_normalizes_case_and_whitespace_but_tracks_type_and_comment():
    a = column_fingerprint("Cat", "Sch", "T", "Col", "string", "Patient name")
    assert a == column_fingerprint("cat ", "sch", "t", "col", "STRING", " patient NAME ")
    assert a != column_fingerprint("cat", "sch", "t", "col", "int", "Patient name")
    assert a != column_fingerprint("cat", "sch", "t", "col", "string", "Other")
    assert column_fingerprint("c", "s", "t", "x", "string", None) == column_fingerprint("c", "s", "t", "x", "string", "")


# ---------------------------------------------------------------- issue detection


def test_untagged_unattested_column_is_missing():
    assert classify() == c.MISSING_CLASSIFICATION


def test_correction_takes_precedence_over_everything():
    assert classify(correction_requested=True, tag_value="bogus") == c.CORRECTION_REQUESTED


def test_invalid_tag_value():
    assert classify(tag_value="bogus") == c.INVALID_TAG_VALUE


def test_tag_without_review_evidence_is_orphaned():
    assert classify(tag_value="name") == c.ORPHANED_TAG


def test_tag_on_changed_column_is_stale():
    v = logic.VerifiedReview("fp-old", NOW)
    assert classify(tag_value="name", verified=v) == c.STALE_PHI_REVIEW


def test_verified_tag_is_quiet_until_recertification():
    assert classify(tag_value="name", verified=logic.VerifiedReview(FP, datetime(2026, 9, 1))) is None
    assert classify(tag_value="name", verified=logic.VerifiedReview(FP, datetime(2026, 7, 8, 12))) == c.RECERTIFICATION_DUE


def test_attestation_rules():
    active = logic.ActiveAttestation(FP, datetime(2027, 1, 1))
    assert classify(attestation=active) is None
    assert classify(attestation=logic.ActiveAttestation("fp-old", datetime(2027, 1, 1))) == c.STALE_NON_PHI_ATTESTATION
    assert classify(attestation=logic.ActiveAttestation(FP, datetime(2026, 10, 1))) == c.RECERTIFICATION_DUE


def test_add_months_clamps_day():
    assert logic.add_months(datetime(2026, 11, 30), 3) == datetime(2027, 2, 28)
    assert logic.add_months(datetime(2026, 10, 15, 9, 12, 5), 3) == datetime(2027, 1, 15, 9, 12, 5)


# ---------------------------------------------------------------- scope precedence


def test_explicit_schema_overrides_wildcard_and_conflicts_are_reported():
    star = logic.Scope("S-STAR", "cat", "*", "STANDARD", "g1")
    clin = logic.Scope("S-CLIN", "cat", "clinical", "HIGH", "g2")
    assert logic.resolve_scope([star, clin], "cat", "clinical") is clin
    assert logic.resolve_scope([star, clin], "cat", "billing") is star
    assert logic.resolve_scope([clin], "cat", "billing") is None
    assert logic.find_scope_conflicts([star, clin]) == []
    dup = logic.Scope("S-STAR2", "cat", "*", "HIGH", "g3")
    assert logic.find_scope_conflicts([star, dup, clin]) == [("cat.*", ["S-STAR", "S-STAR2"])]


# ---------------------------------------------------------------- suggestions


MAPPING = {"class.name": "name", "class.us_ssn": "ssn", "class.email_address": "email"}


def test_suggestion_from_single_high_confidence_mapping():
    d = [{"class_tag": "class.name", "confidence": "HIGH", "frequency": 0.97, "latest_detected_time": NOW}]
    assert logic.suggest_value(c.MISSING_CLASSIFICATION, None, ALLOWED, d, MAPPING) == ("name", "class.name; HIGH; 0.97")


def test_no_suggestion_for_low_confidence_conflicts_or_unmapped():
    low = [{"class_tag": "class.name", "confidence": "LOW", "frequency": 0.5, "latest_detected_time": NOW}]
    conflict = [
        {"class_tag": "class.name", "confidence": "HIGH", "frequency": 0.9, "latest_detected_time": NOW},
        {"class_tag": "class.us_ssn", "confidence": "HIGH", "frequency": 0.9, "latest_detected_time": NOW},
    ]
    not_allowed = [{"class_tag": "class.email_address", "confidence": "HIGH", "frequency": 1, "latest_detected_time": NOW}]
    for d in (low, conflict, not_allowed, []):
        assert logic.suggest_value(c.MISSING_CLASSIFICATION, None, ALLOWED, d, MAPPING) == (None, None)


def test_reconfirmation_prefills_current_tag():
    assert logic.suggest_value(c.RECERTIFICATION_DUE, "mrn", ALLOWED, [], MAPPING) == ("mrn", "Current tag value")
    assert logic.suggest_value(c.CORRECTION_REQUESTED, "mrn", ALLOWED, [], MAPPING)[0] == "mrn"


# ---------------------------------------------------------------- validation and authorization


@pytest.mark.parametrize(
    "decision,suggested,corrected,comment,status,ok",
    [
        (c.APPROVE_SUGGESTION, "name", None, "", c.PENDING_STEWARD_REVIEW, True),
        (c.APPROVE_SUGGESTION, None, None, "", c.PENDING_STEWARD_REVIEW, False),
        (c.APPROVE_SUGGESTION, "bogus", None, "", c.PENDING_STEWARD_REVIEW, False),
        (c.CORRECT_CLASSIFICATION, None, "mrn", "", c.PENDING_STEWARD_REVIEW, True),
        (c.CORRECT_CLASSIFICATION, None, "bogus", "", c.PENDING_STEWARD_REVIEW, False),
        (c.CORRECT_CLASSIFICATION, None, None, "", c.PENDING_STEWARD_REVIEW, False),
        (c.CONFIRM_NOT_PHI, None, None, "Technical code", c.PENDING_STEWARD_REVIEW, True),
        (c.CONFIRM_NOT_PHI, None, None, "  ", c.PENDING_STEWARD_REVIEW, False),
        (c.CONFIRM_NOT_PHI, None, "name", "x", c.PENDING_STEWARD_REVIEW, False),
        (c.REQUEST_PRIVACY_REVIEW, None, None, "Is this PHI?", c.PENDING_STEWARD_REVIEW, True),
        (c.REQUEST_PRIVACY_REVIEW, None, None, "Again?", c.NEEDS_PRIVACY_REVIEW, False),
        ("DEFER", None, None, "", c.PENDING_STEWARD_REVIEW, False),
    ],
)
def test_validate_decision(decision, suggested, corrected, comment, status, ok):
    assert logic.validate_decision(decision, status, suggested, corrected, comment, ALLOWED).ok is ok


def test_authorize_reviewer():
    a = logic.authorize_reviewer
    assert a(c.ROLE_ASSIGNED_STEWARD, "s@x.org", "S@x.org", c.PENDING_STEWARD_REVIEW).ok
    assert not a(c.ROLE_ASSIGNED_STEWARD, "other@x.org", "s@x.org", c.PENDING_STEWARD_REVIEW).ok
    assert a(c.ROLE_STEWARD_GROUP_MEMBER, "m@x.org", None, c.PENDING_STEWARD_REVIEW).ok
    assert not a(c.ROLE_STEWARD_GROUP_MEMBER, "m@x.org", None, c.NEEDS_PRIVACY_REVIEW).ok
    assert a(c.ROLE_DATA_GOVERNANCE, "g@x.org", "s@x.org", c.NEEDS_PRIVACY_REVIEW).ok


# ---------------------------------------------------------------- DDL


def test_render_tag_ddl_per_object_type_and_escaping():
    set_mv = logic.render_tag_ddl(c.SET_PHI_TAG, "MATERIALIZED_VIEW", "cat", "sch", "t", "col", "phi", "name")
    assert set_mv == "ALTER MATERIALIZED VIEW `cat`.`sch`.`t` ALTER COLUMN `col` SET TAGS ('phi' = 'name')"
    unset_st = logic.render_tag_ddl(c.REMOVE_PHI_TAG, "STREAMING_TABLE", "cat", "sch", "t", "c`ol", "phi", None)
    assert unset_st == "ALTER STREAMING TABLE `cat`.`sch`.`t` ALTER COLUMN `c``ol` UNSET TAGS ('phi')"
    assert "it\\'s" in logic.render_tag_ddl(c.SET_PHI_TAG, "MANAGED", "c", "s", "t", "x", "phi", "it's")
    with pytest.raises(ValueError):
        logic.render_tag_ddl(c.SET_PHI_TAG, "VIEW", "c", "s", "t", "x", "phi", "name")
    with pytest.raises(ValueError):
        logic.render_tag_ddl(c.SET_PHI_TAG, "MANAGED", "c", "s", "t", "x", "phi", None)


def _cfg():
    return FrameworkConfig("dev", "cat", "data_governance", "phi", "dev_phi_classification_scan", "1",
                           ("cat",), job_names("dev"), "dev_phi_review_app")


def test_process_constraints_limit_manual_and_app_writes():
    checks = {(t, n): e for t, n, e in ddl.check_constraints(_cfg())}
    registry = checks[("classification_scope_registry", "valid_audit_created_process")]
    item = checks[("classification_review_item", "valid_audit_created_process")]
    inbox = checks[("classification_decision_submission", "valid_audit_created_process")]
    assert "'MANUAL'" in registry and "'MANUAL'" not in item
    assert inbox == "audit_created_process IN ('dev_phi_review_app')"
    assert ("classification_review_event", "valid_audit_updated_process") not in checks


def test_every_table_defines_audit_columns_and_key():
    for t in ddl.TABLES:
        names = [col.name for col in t.columns]
        assert t.pk == names[0]
        assert "audit_created_process" in names
        assert ("audit_updated_process" in names) is (not t.append_only)
    stmts = ddl.create_statements(_cfg())
    assert any("'delta.appendOnly' = 'true'" in s and "classification_review_event" in s for s in stmts)
