"""Data access for the review app, using the app's service principal on a SQL warehouse.

The app reads the governance tables and appends to classification_decision_submission only; every
other governance write is made by the framework jobs.
"""
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from phi_framework import constants as c
from phi_framework.keys import make_key

from .settings import Settings

_ALLOWED_TTL_SECONDS = 300

_ITEM_SELECT = """
SELECT i.*, b.scope_id, b.due_at, b.status AS batch_status, r.steward_group, r.risk_tier
FROM {item} i
JOIN {batch} b ON b.review_batch_id = i.review_batch_id
JOIN {scope} r ON r.scope_id = b.scope_id
"""


class Backend:
    def __init__(self, settings: Settings):
        self.s = settings
        self.cfg = Config()
        self._allowed: Optional[tuple] = None
        self._allowed_at = 0.0

    @contextmanager
    def _cursor(self):
        conn = sql.connect(
            server_hostname=self.cfg.host,
            http_path=f"/sql/1.0/warehouses/{self.s.warehouse_id}",
            credentials_provider=lambda: self.cfg.authenticate,
        )
        try:
            with conn.cursor() as cur:
                yield cur
        finally:
            conn.close()

    def _query(self, statement: str, params: Optional[dict] = None) -> List[dict]:
        with self._cursor() as cur:
            cur.execute(statement, params or {})
            cols = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cols, row)) for row in cur.fetchall()] if cols else []

    def _items_sql(self) -> str:
        return _ITEM_SELECT.format(
            item=self.s.table("classification_review_item"),
            batch=self.s.table("classification_review_batch"),
            scope=self.s.table("classification_scope_registry"),
        )

    # ------------------------------------------------------------ reads

    def open_items(self) -> List[dict]:
        terminal = ", ".join(f"'{x}'" for x in c.TERMINAL_ITEM_STATUSES)
        return self._query(
            self._items_sql() + f" WHERE i.status NOT IN ({terminal}) ORDER BY b.due_at, i.table_name, i.column_name"
        )

    def item(self, review_item_id: str) -> Optional[dict]:
        rows = self._query(self._items_sql() + " WHERE i.review_item_id = :id", {"id": review_item_id})
        return rows[0] if rows else None

    def item_events(self, review_item_id: str) -> List[dict]:
        return self._query(
            f"""SELECT event_type, event_timestamp, business_actor, CAST(event_details AS STRING) AS details
                FROM {self.s.table('classification_review_event')}
                WHERE review_item_id = :id ORDER BY event_timestamp DESC, event_type LIMIT 50""",
            {"id": review_item_id},
        )

    def pending_submissions(self, review_item_id: Optional[str] = None) -> List[dict]:
        where = "WHERE s.review_item_id = :id" if review_item_id else ""
        return self._query(
            f"""SELECT s.* FROM {self.s.table('classification_decision_submission')} s
                LEFT ANTI JOIN {self.s.table('classification_review_event')} e
                  ON e.event_type = '{c.SUBMISSION_INGESTED}' AND e.discriminator = s.submission_id
                {where} ORDER BY s.submitted_at DESC""",
            {"id": review_item_id} if review_item_id else None,
        )

    def batches(self) -> List[dict]:
        return self._query(
            f"""SELECT b.review_batch_id, b.scope_id, b.status, b.created_at, b.due_at, b.closed_at, b.last_error,
                       count(i.review_item_id) AS items,
                       count_if(i.status IN ({', '.join(f"'{x}'" for x in c.TERMINAL_ITEM_STATUSES)})) AS resolved
                FROM {self.s.table('classification_review_batch')} b
                LEFT JOIN {self.s.table('classification_review_item')} i ON i.review_batch_id = b.review_batch_id
                GROUP BY ALL ORDER BY b.created_at DESC LIMIT 50"""
        )

    def allowed_values(self) -> tuple:
        """Allowed values from the governed tag policy, cached briefly so policy edits take effect.

        An empty policy is never cached, so values defined after startup are picked up on the next call.
        """
        now = time.monotonic()
        if self._allowed is not None and now - self._allowed_at < _ALLOWED_TTL_SECONDS:
            return self._allowed
        resp = WorkspaceClient().api_client.do("GET", f"/api/2.1/tag-policies/{self.s.tag_key}")
        values = tuple(v["name"] for v in resp.get("values", []))
        if values:
            self._allowed, self._allowed_at = values, now
        return values

    # ------------------------------------------------------------ writes

    def submit(
        self,
        submission_type: str,
        item: dict,
        submitted_by: str,
        submitter_role: str,
        decision: Optional[str] = None,
        corrected_tag_value: Optional[str] = None,
        steward_comment: Optional[str] = None,
        assigned_steward: Optional[str] = None,
    ) -> str:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        submission_id = make_key(
            "SUB", [item["review_item_id"], submitted_by, submission_type, now.isoformat(), str(time.monotonic_ns())]
        )
        self._query(
            f"""INSERT INTO {self.s.table('classification_decision_submission')} (
                  submission_id, submission_type, review_batch_id, review_item_id, decision,
                  corrected_tag_value, steward_comment, assigned_steward, submitted_by, submitter_role,
                  submitted_at, audit_created_at, audit_created_by, audit_created_process, audit_created_run_id)
                VALUES (:submission_id, :submission_type, :batch, :item, :decision, :corrected, :comment,
                        :assigned, :submitted_by, :role, :submitted_at, current_timestamp(), current_user(),
                        :process, NULL)""",
            {
                "submission_id": submission_id,
                "submission_type": submission_type,
                "batch": item["review_batch_id"],
                "item": item["review_item_id"],
                "decision": decision,
                "corrected": corrected_tag_value or None,
                "comment": steward_comment or None,
                "assigned": assigned_steward or None,
                "submitted_by": submitted_by,
                "role": submitter_role,
                "submitted_at": now,
                "process": self.s.process_name,
            },
        )
        return submission_id

    def request_correction(self, column_fqn: str, reason: str, requested_by: str) -> int:
        if not self.s.scan_job_id:
            raise RuntimeError("PHI_SCAN_JOB_ID is not configured")
        run = WorkspaceClient().jobs.run_now(
            job_id=int(self.s.scan_job_id),
            job_parameters={
                "correction_column": column_fqn,
                "correction_reason": reason,
                "correction_requested_by": requested_by,
            },
        )
        return getattr(run, "response", run).run_id


def summarize(items: List[dict]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for i in items:
        out[i["status"]] = out.get(i["status"], 0) + 1
    return out
