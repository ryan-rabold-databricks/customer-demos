"""End-to-end test of a dev deployment, driven through the review app's HTTP endpoints.

Prerequisites: the bundle is deployed, setup has run with seed_demo=true, one scan has run, and the
caller belongs to the governance group and both demo steward groups. The test changes tags on the
synthetic demo tables only.

Usage:
  python scripts/e2e_test.py --profile <profile> --warehouse_id <id> --app_url <url> \
      --catalog <catalog> --scan_job_id <id> [--governance_schema data_governance] [--tag_key phi_data_classification]
"""
import argparse
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

POLL_SECONDS = 20
TIMEOUT_SECONDS = 900


class Env:
    def __init__(self, a):
        self.a = a
        self.w = WorkspaceClient(profile=a.profile)
        self.gov = f"`{a.catalog}`.`{a.governance_schema}`"
        self.me = self.w.current_user.me().user_name.lower()
        self.failures = []

    # ---------------------------------------------------------------- helpers

    def sql(self, statement):
        r = self.w.statement_execution.execute_statement(
            statement=statement, warehouse_id=self.a.warehouse_id, wait_timeout="50s")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            r = self.w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"{statement[:120]}... -> {r.status.error}")
        cols = [c.name for c in r.manifest.schema.columns] if r.manifest else []
        return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []

    def post(self, path, fields):
        token = self.w.config.authenticate()["Authorization"]
        req = urllib.request.Request(
            self.a.app_url.rstrip("/") + path, data=urllib.parse.urlencode(fields).encode(),
            headers={"Authorization": token, "Content-Type": "application/x-www-form-urlencoded"}, method="POST")

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None

        try:
            urllib.request.build_opener(NoRedirect).open(req, timeout=60)
            raise RuntimeError("expected a redirect")
        except urllib.error.HTTPError as e:
            if e.code != 303:
                raise RuntimeError(f"POST {path} -> {e.code}: {e.read()[:300]}")
            return urllib.parse.unquote(e.headers["Location"])

    def check(self, label, condition, detail=""):
        print(("PASS " if condition else "FAIL ") + label + (f" ({detail})" if detail and not condition else ""))
        if not condition:
            self.failures.append(label)

    def items(self):
        return {
            (r["table_name"], r["column_name"]): r
            for r in self.sql(f"SELECT * FROM {self.gov}.classification_review_item ORDER BY audit_created_at")
        }

    def open_item(self, table, column):
        rows = self.sql(
            f"SELECT * FROM {self.gov}.classification_review_item WHERE table_name = '{table}' "
            f"AND column_name = '{column}' AND status NOT IN ('TAG_VERIFIED','ATTESTED_NOT_PHI','CLOSED_NO_ACTION')")
        return rows[0] if rows else None

    def tags(self):
        return {
            (r["table_name"], r["column_name"]): r["tag_value"]
            for r in self.sql(
                f"SELECT table_name, column_name, tag_value FROM `{self.a.catalog}`.information_schema.column_tags "
                f"WHERE tag_name = '{self.a.tag_key}' AND schema_name LIKE 'phi_demo_%'")
        }

    def wait_until(self, label, predicate):
        deadline = time.time() + TIMEOUT_SECONDS
        while time.time() < deadline:
            if predicate():
                print(f"... {label} after {int(TIMEOUT_SECONDS - (deadline - time.time()))}s")
                return True
            time.sleep(POLL_SECONDS)
        self.check(label, False, "timed out")
        return False

    def no_pending_submissions(self):
        return not self.sql(
            f"SELECT 1 FROM {self.gov}.classification_decision_submission s "
            f"LEFT ANTI JOIN {self.gov}.classification_review_event e "
            f"ON e.event_type = 'SUBMISSION_INGESTED' AND e.discriminator = s.submission_id LIMIT 1")

    def no_planned(self):
        return not self.sql(
            f"SELECT 1 FROM {self.gov}.classification_review_item "
            f"WHERE status IN ('PLANNED','APPLICATION_IN_PROGRESS') LIMIT 1")

    def decide(self, item, decision, value="", comment=""):
        """Submit through the app's three choices: This is PHI, Not PHI, or Ask Privacy."""
        action = {"APPROVE_SUGGESTION": "phi", "CORRECT_CLASSIFICATION": "phi", "CONFIRM_NOT_PHI": "not_phi",
                  "REQUEST_PRIVACY_REVIEW": "privacy"}[decision]
        if decision == "APPROVE_SUGGESTION":
            value = item["suggested_tag_value"] or ""
        return self.post(f"/items/{item['review_item_id']}/decide",
                         {"action": action, "tag_value": value, "comment": comment, "after": "stay"})

    def run_scan(self, **params):
        self.w.jobs.run_now_and_wait(int(self.a.scan_job_id), job_parameters=params or None)


PHI = {
    ("patient_encounter", "patient_name"): "name",
    ("patient_encounter", "mrn"): "mrn",
    ("patient_encounter", "birth_date"): "date",
    ("patient_encounter", "admit_ts"): "date",
    ("patient_encounter", "patient_email"): "email_address",
    ("encounter_daily_summary", "admit_date"): "date",
    ("claim", "member_name"): "name",
    ("claim", "member_ssn"): "ssn",
    ("claim", "health_plan_member_id"): "health_plan_beneficiary_number",
}
NOT_PHI = {
    ("patient_encounter", "encounter_id"), ("patient_encounter", "source_system_code"),
    ("encounter_daily_summary", "source_system_code"), ("encounter_daily_summary", "encounters"),
    ("claim", "claim_id"), ("claim", "claim_amount"), ("claim", "payer_code"),
}
PRIVACY = ("patient_encounter", "diagnosis_code")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for name in ("--profile", "--warehouse_id", "--app_url", "--catalog", "--scan_job_id"):
        p.add_argument(name, required=True)
    p.add_argument("--governance_schema", default="data_governance")
    p.add_argument("--tag_key", default="phi_data_classification")
    env = Env(p.parse_args())
    items = env.items()
    env.check("scan opened 17 items", len(items) == 17, str(len(items)))

    # 1. Server-side validation rejects a value outside the governed tag policy, before any write.
    loc = env.decide(items[("claim", "claim_amount")], "CORRECT_CLASSIFICATION", value="not_a_value")
    env.check("app rejects a value outside the tag policy", "err=" in loc, loc)

    # 2. Governance assigns a steward, then every decision type is submitted through the app.
    loc = env.post(f"/items/{items[('patient_encounter', 'patient_name')]['review_item_id']}/assign",
                   {"assigned_steward": env.me})
    env.check("assignment accepted", "msg=" in loc, loc)
    for key, value in PHI.items():
        env.check(f"submit {key}", "msg=" in env.decide(items[key], "CORRECT_CLASSIFICATION", value=value))
    for key in NOT_PHI:
        env.check(f"submit {key}", "msg=" in env.decide(items[key], "CONFIRM_NOT_PHI", comment="Technical or aggregate value; no individual identifier"))
    env.check("submit privacy request",
              "msg=" in env.decide(items[PRIVACY], "REQUEST_PRIVACY_REVIEW", comment="Is a diagnosis code alone PHI here?"))

    # 3. A submission that bypasses the app's checks is caught by ingestion.
    bogus = items[("claim", "claim_amount")]
    env.sql(
        f"INSERT INTO {env.gov}.classification_decision_submission VALUES ("
        f"'SUB-E2E-BOGUS', 'DECISION', '{bogus['review_batch_id']}', '{bogus['review_item_id']}', "
        f"'CORRECT_CLASSIFICATION', 'not_a_value', NULL, NULL, 'intruder@example.org', 'ASSIGNED_STEWARD', "
        f"current_timestamp() - INTERVAL 1 MINUTE, current_timestamp(), current_user(), 'dev_phi_review_app', NULL)")

    env.wait_until("ingestion processed all submissions", env.no_pending_submissions)
    after = env.items()
    env.check("assigned_steward set", after[("patient_encounter", "patient_name")]["assigned_steward"] == env.me)
    env.check("reviewed_by is the authenticated user",
              after[("patient_encounter", "patient_name")]["reviewed_by"] == env.me)
    env.check("privacy item waits", after[PRIVACY]["status"] == "NEEDS_PRIVACY_REVIEW", after[PRIVACY]["status"])
    rejected = env.sql(f"SELECT 1 FROM {env.gov}.classification_review_event WHERE discriminator = 'SUB-E2E-BOGUS' "
                       f"AND event_type = 'VALIDATION_FAILED'")
    env.check("ingestion rejects unauthorized/invalid submission", bool(rejected))

    # 4. The application job is triggered by the new plan rows.
    env.wait_until("application finished all plans", env.no_planned)
    after, tags = env.items(), env.tags()
    for key, value in PHI.items():
        env.check(f"{key} TAG_VERIFIED with {value}",
                  after[key]["status"] == "TAG_VERIFIED" and tags.get(key) == value,
                  f"{after[key]['status']} / {tags.get(key)}")
    for key in NOT_PHI:
        env.check(f"{key} ATTESTED_NOT_PHI", after[key]["status"] == "ATTESTED_NOT_PHI" and key not in tags,
                  after[key]["status"])

    batches = {r["scope_id"]: r["status"] for r in env.sql(
        f"SELECT scope_id, status FROM {env.gov}.classification_review_batch WHERE last_error IS NULL")}
    env.check("billing batch closed", batches.get("SCOPE-BILLING-DEV") == "CLOSED", str(batches))
    env.check("clinical batch held open by Privacy review", batches.get("SCOPE-CLINICAL-DEV") == "OPEN", str(batches))

    # 5. Data Governance records Privacy's outcome; the clinical batch then closes.
    env.check("privacy outcome submitted",
              "msg=" in env.decide(after[PRIVACY], "CONFIRM_NOT_PHI", comment="Privacy: code alone is not identifying"))
    env.wait_until("clinical batch closed", lambda: env.sql(
        f"SELECT 1 FROM {env.gov}.classification_review_batch WHERE scope_id = 'SCOPE-CLINICAL-DEV' "
        f"AND status = 'CLOSED' AND last_error IS NULL"))

    # 6. A changed decision on an accepted item is recorded and ignored.
    env.sql(
        f"INSERT INTO {env.gov}.classification_decision_submission VALUES ("
        f"'SUB-E2E-LATE', 'DECISION', '{after[('claim', 'member_ssn')]['review_batch_id']}', "
        f"'{after[('claim', 'member_ssn')]['review_item_id']}', 'CONFIRM_NOT_PHI', NULL, 'changed my mind', NULL, "
        f"'{env.me}', 'DATA_GOVERNANCE', current_timestamp(), current_timestamp(), current_user(), 'dev_phi_review_app', NULL)")
    env.wait_until("late change ingested", env.no_pending_submissions)
    ignored = env.sql(f"SELECT 1 FROM {env.gov}.classification_review_event WHERE discriminator = 'SUB-E2E-LATE' "
                      f"AND event_type = 'CHANGE_IGNORED_ALREADY_RESOLVED'")
    env.check("CHANGE_IGNORED_ALREADY_RESOLVED recorded", bool(ignored))
    env.check("accepted tag unchanged", env.tags().get(("claim", "member_ssn")) == "ssn")

    # 7. A rescan of a fully classified scope opens nothing.
    before = len(env.items())
    env.run_scan()
    env.check("rescan opens no items", len(env.items()) == before)

    # 8. Correction through the app: reopen patient_email, confirm not PHI, tag is removed.
    column = f"{env.a.catalog}.phi_demo_clinical.patient_encounter.patient_email"
    loc = env.post("/corrections", {"column_fqn": column, "reason": "E2E: verify tag removal path"})
    env.check("correction scan started", "msg=" in loc, loc)
    env.wait_until("correction item opened", lambda: env.open_item("patient_encounter", "patient_email"))
    corr = env.open_item("patient_encounter", "patient_email")
    env.check("issue is CORRECTION_REQUESTED", corr and corr["issue_type"] == "CORRECTION_REQUESTED")
    env.check("suggestion pre-filled with current tag", corr and corr["suggested_tag_value"] == "email_address")
    env.decide(corr, "CONFIRM_NOT_PHI", comment="E2E: synthetic addresses only")
    env.wait_until("tag removed and item attested", lambda: env.sql(
        f"SELECT 1 FROM {env.gov}.classification_review_item WHERE review_item_id = '{corr['review_item_id']}' "
        f"AND status = 'ATTESTED_NOT_PHI'"))
    env.check("patient_email tag absent", ("patient_encounter", "patient_email") not in env.tags())
    removed = env.sql(f"SELECT 1 FROM {env.gov}.classification_review_event WHERE event_type = 'TAG_REMOVED' "
                      f"AND review_item_id = '{corr['review_item_id']}'")
    env.check("TAG_REMOVED event recorded", bool(removed))

    # 9. Tags or metadata changed outside the framework are detected by the next scan.
    env.sql(f"ALTER TABLE `{env.a.catalog}`.phi_demo_billing.claim ALTER COLUMN payer_code "
            f"SET TAGS ('{env.a.tag_key}' = 'account_number')")
    env.sql(f"ALTER TABLE `{env.a.catalog}`.phi_demo_billing.claim ALTER COLUMN member_name "
            f"COMMENT 'Health plan member full legal name'")
    env.run_scan()
    orphan = env.open_item("claim", "payer_code")
    stale = env.open_item("claim", "member_name")
    env.check("manual tag detected as ORPHANED_TAG", orphan and orphan["issue_type"] == "ORPHANED_TAG",
              orphan and orphan["issue_type"])
    env.check("comment change detected as STALE_PHI_REVIEW", stale and stale["issue_type"] == "STALE_PHI_REVIEW",
              stale and stale["issue_type"])

    print(f"\n{'ALL CHECKS PASSED' if not env.failures else f'{len(env.failures)} FAILED: {env.failures}'}")
    sys.exit(1 if env.failures else 0)


if __name__ == "__main__":
    main()
