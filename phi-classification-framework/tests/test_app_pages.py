"""Render every review-app page against an in-memory backend. Needs the app's requirements installed."""
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("databricks.sql")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
os.environ.update(
    DATABRICKS_WAREHOUSE_ID="wh", PHI_CATALOG="cat", PHI_GOVERNANCE_SCHEMA="gov", PHI_TAG_KEY="phi",
    PHI_ENV="dev", PHI_GOVERNANCE_GROUP="governance", DEV_USER_EMAIL="steward@example.org",
    DEV_USER_GROUPS="stewards,governance", )

from fastapi.testclient import TestClient  # noqa: E402

from phi_framework import constants as c  # noqa: E402
from review_app import main  # noqa: E402

DUE = datetime(2026, 10, 22, 17, 0)


def _item(item_id, column, status=c.PENDING_STEWARD_REVIEW, issue=c.MISSING_CLASSIFICATION, **kw):
    row = {
        "review_item_id": item_id, "review_batch_id": "BATCH-1", "catalog_name": "cat", "schema_name": "clinical",
        "table_name": "patient", "table_type": "MANAGED", "column_name": column, "full_data_type": "string",
        "column_comment": f"{column} comment", "column_fingerprint": "fp", "issue_type": issue,
        "current_tag_value": None, "suggested_tag_value": None, "suggestion_reason": None,
        "assigned_steward": None, "decision": None, "corrected_tag_value": None, "steward_comment": None,
        "reviewed_by": None, "reviewed_at": None, "status": status, "applied_at": None, "application_error": None,
        "scope_id": "SCOPE-1", "due_at": DUE, "batch_status": "OPEN", "steward_group": "stewards", "risk_tier": "HIGH",
    }
    row.update(kw)
    return row


class FakeSession:
    def __init__(self, store):
        self.store = store

    def open_items(self):
        return [i for i in self.store["items"].values() if i["status"] not in c.TERMINAL_ITEM_STATUSES]

    def item(self, item_id):
        return self.store["items"].get(item_id)

    def items(self, ids):
        return {i: self.store["items"][i] for i in ids if i in self.store["items"]}

    def prior_resolutions(self, items):
        return {}

    def rejection_reasons(self, ids):
        return {i: "Corrected value 'x' is not allowed." for i in ids}

    def correction_reason(self, item_id):
        return "Wrong value"

    def siblings(self, item):
        return [{"column_name": i["column_name"], "review_item_id": i["review_item_id"], "status": i["status"],
                 "column_comment": i["column_comment"], "tag_value": None} for i in self.store["items"].values()]

    def item_events(self, item_id):
        return [{"event_type": c.ITEM_OPENED, "event_timestamp": DUE, "business_actor": None,
                 "details": '{"issue_type":"MISSING_CLASSIFICATION"}'}]

    def pending_submissions(self):
        return self.store["submissions"]

    def history(self, email):
        return [{"submission_id": s["submission_id"], "submission_type": s["submission_type"],
                 "decision": s.get("decision"), "corrected_tag_value": s.get("corrected_tag_value"),
                 "steward_comment": s.get("steward_comment"), "assigned_steward": None, "submitted_at": DUE,
                 "review_item_id": s["review_item_id"], "catalog_name": "cat", "schema_name": "clinical",
                 "table_name": "patient", "column_name": "x", "item_status": c.PENDING_STEWARD_REVIEW,
                 "application_error": None, "outcome": None, "error": None, "ignored": False}
                for s in self.store["submissions"]]

    def batches(self):
        return []

    def submit_many(self, rows):
        for n, r in enumerate(rows):
            self.store["submissions"].append({
                "submission_id": f"SUB-{len(self.store['submissions'])}", "submission_type": r["submission_type"],
                "review_item_id": r["item"]["review_item_id"], "submitted_by": r["submitted_by"],
                "decision": r.get("decision"), "corrected_tag_value": r.get("corrected_tag_value"),
                "steward_comment": r.get("steward_comment"),
            })
        return []


@pytest.fixture
def client(monkeypatch):
    store = {"items": {}, "submissions": []}
    for n, col in enumerate(["patient_name", "mrn", "source_code"]):
        store["items"][f"ITEM-{n}"] = _item(f"ITEM-{n}", col, suggested_tag_value="name" if n == 0 else None)
    store["items"]["ITEM-3"] = _item("ITEM-3", "dx", status=c.VALIDATION_FAILED, issue=c.CORRECTION_REQUESTED)

    @contextmanager
    def session():
        yield FakeSession(store)

    monkeypatch.setattr(main.BACKEND, "session", session)
    monkeypatch.setattr(main.BACKEND, "_cfg", type("Cfg", (), {"host": "https://example.cloud.databricks.com"})())
    monkeypatch.setattr(main.BACKEND, "_tag_policy", lambda: (("name", "mrn", "date"), "HIPAA identifiers"))
    return TestClient(main.app), store


def test_queue_groups_by_table_and_uses_plain_language(client):
    http, _ = client
    page = http.get("/").text
    assert "clinical.patient" in page and "Not classified yet" in page and "Needs a decision" in page
    assert "Rejected: Corrected value" in page and "Mark not PHI" in page


def test_item_page_offers_three_actions_with_context(client):
    http, _ = client
    page = http.get("/items/ITEM-0").text
    for text in ("This is PHI", "Not PHI", "Ask Privacy", "Submit and next", "(suggested)",
                 "Other columns in this table", "Catalog Explorer", "What each PHI type means", "Names"):
        assert text in page, text
    assert "Your last decision was rejected" in http.get("/items/ITEM-3").text
    assert "reopened this column for a new decision: Wrong value" in http.get("/items/ITEM-3").text


def test_submit_and_next_moves_to_the_next_item_and_hides_submitted(client):
    http, store = client
    r = http.post("/items/ITEM-0/decide", data={"action": "phi", "tag_value": "name", "after": "next"},
                  follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/items/ITEM-")
    assert "ITEM-0" not in r.headers["location"]
    assert store["submissions"][0]["decision"] == c.APPROVE_SUGGESTION
    page = http.get("/").text
    assert "patient_name" not in page and "1 decision you submitted is awaiting validation" in page


def test_invalid_choice_is_rejected_before_saving(client):
    http, store = client
    r = http.post("/items/ITEM-1/decide", data={"action": "not_phi", "comment": " "}, follow_redirects=False)
    assert "err=" in r.headers["location"] and not store["submissions"]


def test_bulk_not_phi_and_approve_suggestions(client):
    http, store = client
    r = http.post("/bulk", data={"action": "not_phi", "comment": "", "item_ids": ["ITEM-1"]}, follow_redirects=False)
    assert "err=" in r.headers["location"] and not store["submissions"]
    r = http.post("/bulk", data={"action": "not_phi", "comment": "Codes only", "item_ids": ["ITEM-1", "ITEM-2"]},
                  follow_redirects=False)
    assert len(store["submissions"]) == 2 and {s["decision"] for s in store["submissions"]} == {c.CONFIRM_NOT_PHI}
    r = http.post("/bulk", data={"action": "phi", "item_ids": ["ITEM-0", "ITEM-3"]}, follow_redirects=False)
    assert "Skipped" in r.headers["location"] or "err=" in r.headers["location"]
    assert store["submissions"][-1]["decision"] == c.APPROVE_SUGGESTION


def test_history_and_governance_pages_render(client):
    http, _ = client
    http.post("/items/ITEM-1/decide", data={"action": "privacy", "comment": "Is this PHI?"})
    page = http.get("/history").text
    assert "Ask Privacy" in page and "Waiting for validation" in page
    assert "Data Governance" in http.get("/governance").text
