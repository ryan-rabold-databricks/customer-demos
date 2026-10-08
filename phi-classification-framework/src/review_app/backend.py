"""Data access for the review app, using the app's service principal on a SQL warehouse.

The app reads the governance tables and appends to classification_decision_submission only; every
other governance write is made by the framework jobs. Each request opens one Session so all of its
queries share a single warehouse connection.
"""
import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from phi_framework import constants as c
from phi_framework.keys import make_key
from phi_framework.logic import quote_literal

from .settings import Settings

_POLICY_TTL_SECONDS = 300
_TERMINAL = ", ".join(f"'{x}'" for x in c.TERMINAL_ITEM_STATUSES)

_ITEM_SELECT = """
SELECT i.*, b.scope_id, b.due_at, b.status AS batch_status, r.steward_group, r.risk_tier
FROM {item} i
JOIN {batch} b ON b.review_batch_id = i.review_batch_id
JOIN {scope} r ON r.scope_id = b.scope_id
"""


def ident(row: dict) -> Tuple[str, str, str, str]:
    return tuple(row[k].lower() for k in ("catalog_name", "schema_name", "table_name", "column_name"))


def table_key(row: dict) -> Tuple[str, str, str]:
    return tuple(row[k] for k in ("catalog_name", "schema_name", "table_name"))


def _in_list(values: Iterable[str]) -> str:
    return ", ".join(quote_literal(v) for v in values) or "NULL"


class Session:
    """Queries for one request, sharing one cursor."""

    def __init__(self, settings: Settings, cursor):
        self.s = settings
        self._cur = cursor

    def query(self, statement: str, params: Optional[dict] = None) -> List[dict]:
        self._cur.execute(statement, params or {})
        cols = [d[0] for d in self._cur.description] if self._cur.description else []
        return [dict(zip(cols, row)) for row in self._cur.fetchall()] if cols else []

    def _t(self, name: str) -> str:
        return self.s.table(name)

    def _items_sql(self) -> str:
        return _ITEM_SELECT.format(item=self._t("classification_review_item"),
                                   batch=self._t("classification_review_batch"),
                                   scope=self._t("classification_scope_registry"))

    # ------------------------------------------------------------ items

    def open_items(self) -> List[dict]:
        return self.query(self._items_sql() + f" WHERE i.status NOT IN ({_TERMINAL})"
                          " ORDER BY b.due_at, i.catalog_name, i.schema_name, i.table_name, i.column_name")

    def item(self, review_item_id: str) -> Optional[dict]:
        rows = self.query(self._items_sql() + " WHERE i.review_item_id = :id", {"id": review_item_id})
        return rows[0] if rows else None

    def items(self, review_item_ids: Iterable[str]) -> Dict[str, dict]:
        ids = list(review_item_ids)
        if not ids:
            return {}
        rows = self.query(self._items_sql() + f" WHERE i.review_item_id IN ({_in_list(ids)})")
        return {r["review_item_id"]: r for r in rows}

    def prior_resolutions(self, items: List[dict]) -> Dict[tuple, dict]:
        """The most recent resolved item for each listed column, keyed by column identity."""
        tables = {".".join(table_key(i)) for i in items}
        if not tables:
            return {}
        rows = self.query(f"""
            SELECT catalog_name, schema_name, table_name, column_name,
                   to_json(max_by(named_struct('status', status, 'reviewed_at', reviewed_at,
                                               'applied_at', applied_at,
                                               'tag_value', coalesce(corrected_tag_value, suggested_tag_value)),
                                  coalesce(applied_at, reviewed_at, audit_updated_at))) AS last
            FROM {self._t('classification_review_item')}
            WHERE status IN ('{c.TAG_VERIFIED}', '{c.ATTESTED_NOT_PHI}')
              AND concat_ws('.', catalog_name, schema_name, table_name) IN ({_in_list(tables)})
            GROUP BY ALL""")
        return {ident(r): _as_dict(r["last"]) for r in rows}

    def rejection_reasons(self, review_item_ids: Iterable[str]) -> Dict[str, str]:
        """Latest validation error for items that are currently VALIDATION_FAILED."""
        ids = list(review_item_ids)
        if not ids:
            return {}
        rows = self.query(f"""
            SELECT review_item_id,
                   max_by(event_details:error::string, event_timestamp) AS error
            FROM {self._t('classification_review_event')}
            WHERE event_type = '{c.EVT_VALIDATION_FAILED}' AND review_item_id IN ({_in_list(ids)})
            GROUP BY review_item_id""")
        return {r["review_item_id"]: r["error"] for r in rows}

    def correction_reason(self, review_item_id: str) -> Optional[str]:
        rows = self.query(f"""
            SELECT event_details:reason::string AS reason
            FROM {self._t('classification_review_event')}
            WHERE event_type = '{c.ITEM_OPENED}' AND review_item_id = :id LIMIT 1""", {"id": review_item_id})
        return rows[0]["reason"] if rows else None

    def siblings(self, item: dict) -> List[dict]:
        """Every column of the item's table the framework has seen, with its latest record."""
        rows = self.query(f"""
            SELECT column_name,
                   to_json(max_by(named_struct('review_item_id', review_item_id, 'status', status,
                                               'full_data_type', full_data_type, 'column_comment', column_comment,
                                               'tag_value', coalesce(corrected_tag_value, suggested_tag_value),
                                               'current_tag_value', current_tag_value),
                                  audit_updated_at)) AS latest
            FROM {self._t('classification_review_item')}
            WHERE catalog_name = :cat AND schema_name = :sch AND table_name = :tbl
            GROUP BY column_name ORDER BY column_name""",
            {"cat": item["catalog_name"], "sch": item["schema_name"], "tbl": item["table_name"]})
        return [{"column_name": r["column_name"], **_as_dict(r["latest"])} for r in rows]

    def item_events(self, review_item_id: str) -> List[dict]:
        return self.query(f"""
            SELECT event_type, event_timestamp, business_actor, CAST(event_details AS STRING) AS details
            FROM {self._t('classification_review_event')}
            WHERE review_item_id = :id ORDER BY event_timestamp DESC, event_type LIMIT 50""",
            {"id": review_item_id})

    # ------------------------------------------------------------ submissions

    def pending_submissions(self) -> List[dict]:
        return self.query(f"""
            SELECT s.* FROM {self._t('classification_decision_submission')} s
            LEFT ANTI JOIN {self._t('classification_review_event')} e
              ON e.event_type = '{c.SUBMISSION_INGESTED}' AND e.discriminator = s.submission_id
            ORDER BY s.submitted_at DESC""")

    def history(self, email: str, limit: int = 200) -> List[dict]:
        ev = self._t("classification_review_event")
        return self.query(f"""
            SELECT s.submission_id, s.submission_type, s.decision, s.corrected_tag_value, s.steward_comment,
                   s.assigned_steward, s.submitted_at, i.review_item_id, i.catalog_name, i.schema_name,
                   i.table_name, i.column_name, i.status AS item_status, i.application_error,
                   (lower(i.reviewed_by) = lower(s.submitted_by) AND i.reviewed_at = s.submitted_at) AS applied,
                   ing.event_details:outcome::string AS outcome,
                   vf.event_details:error::string AS error,
                   ign.event_id IS NOT NULL AS ignored
            FROM {self._t('classification_decision_submission')} s
            JOIN {self._t('classification_review_item')} i ON i.review_item_id = s.review_item_id
            LEFT JOIN {ev} ing ON ing.discriminator = s.submission_id AND ing.event_type = '{c.SUBMISSION_INGESTED}'
            LEFT JOIN {ev} vf ON vf.discriminator = s.submission_id AND vf.event_type = '{c.EVT_VALIDATION_FAILED}'
            LEFT JOIN {ev} ign ON ign.discriminator = s.submission_id
                              AND ign.event_type = '{c.CHANGE_IGNORED_ALREADY_RESOLVED}'
            WHERE lower(s.submitted_by) = :me
            ORDER BY s.submitted_at DESC LIMIT {int(limit)}""", {"me": email.lower()})

    def batches(self) -> List[dict]:
        return self.query(f"""
            SELECT b.review_batch_id, b.scope_id, b.status, b.created_at, b.due_at, b.closed_at, b.last_error,
                   count(i.review_item_id) AS items, count_if(i.status IN ({_TERMINAL})) AS resolved
            FROM {self._t('classification_review_batch')} b
            LEFT JOIN {self._t('classification_review_item')} i ON i.review_batch_id = b.review_batch_id
            GROUP BY ALL ORDER BY b.created_at DESC LIMIT 50""")

    def submit_many(self, rows: List[dict]) -> List[str]:
        """Append submissions in one statement. Each row: submission_type, item, submitted_by,
        submitter_role, and optional decision, corrected_tag_value, steward_comment, assigned_steward."""
        if not rows:
            return []
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        params, values, ids = {"process": self.s.process_name, "submitted_at": now}, [], []
        for n, r in enumerate(rows):
            item = r["item"]
            sid = make_key("SUB", [item["review_item_id"], r["submitted_by"], r["submission_type"],
                                   now.isoformat(), str(time.monotonic_ns()), str(n)])
            ids.append(sid)
            params.update({
                f"id{n}": sid, f"type{n}": r["submission_type"], f"batch{n}": item["review_batch_id"],
                f"item{n}": item["review_item_id"], f"decision{n}": r.get("decision"),
                f"corrected{n}": r.get("corrected_tag_value") or None, f"comment{n}": r.get("steward_comment") or None,
                f"assigned{n}": r.get("assigned_steward") or None, f"by{n}": r["submitted_by"],
                f"role{n}": r["submitter_role"],
            })
            values.append(f"(:id{n}, :type{n}, :batch{n}, :item{n}, :decision{n}, :corrected{n}, :comment{n}, "
                          f":assigned{n}, :by{n}, :role{n}, :submitted_at, current_timestamp(), current_user(), "
                          f":process, NULL)")
        self.query(f"""
            INSERT INTO {self._t('classification_decision_submission')} (
              submission_id, submission_type, review_batch_id, review_item_id, decision, corrected_tag_value,
              steward_comment, assigned_steward, submitted_by, submitter_role, submitted_at,
              audit_created_at, audit_created_by, audit_created_process, audit_created_run_id)
            VALUES {', '.join(values)}""", params)
        return ids


def _as_dict(value) -> dict:
    """Parse a to_json(struct) column; timestamps become naive UTC datetimes."""
    if not value:
        return {}
    out = json.loads(value) if isinstance(value, str) else dict(value)
    for k, v in out.items():
        if isinstance(v, str) and k.endswith("_at"):
            out[k] = _parse_ts(v)
    return out


def _parse_ts(text: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        return None


class Backend:
    def __init__(self, settings: Settings):
        self.s = settings
        self._cfg: Optional[Config] = None
        self._policy: Optional[Tuple[tuple, str]] = None
        self._policy_at = 0.0

    @property
    def cfg(self) -> Config:
        """Created on first use so importing the app never makes network calls."""
        if self._cfg is None:
            self._cfg = Config()
        return self._cfg

    @contextmanager
    def session(self):
        conn = sql.connect(
            server_hostname=self.cfg.host,
            http_path=f"/sql/1.0/warehouses/{self.s.warehouse_id}",
            credentials_provider=lambda: self.cfg.authenticate,
        )
        try:
            with conn.cursor() as cur:
                yield Session(self.s, cur)
        finally:
            conn.close()

    def _tag_policy(self) -> Tuple[tuple, str]:
        """(allowed values, policy description), cached briefly so policy edits take effect.

        An empty value list is never cached, so values defined after startup are picked up.
        """
        now = time.monotonic()
        if self._policy is not None and now - self._policy_at < _POLICY_TTL_SECONDS:
            return self._policy
        resp = WorkspaceClient().api_client.do("GET", f"/api/2.1/tag-policies/{self.s.tag_key}")
        policy = (tuple(v["name"] for v in resp.get("values", [])), resp.get("description") or "")
        if policy[0]:
            self._policy, self._policy_at = policy, now
        return policy

    def allowed_values(self) -> tuple:
        return self._tag_policy()[0]

    def policy_description(self) -> str:
        return self._tag_policy()[1]

    def explorer_url(self, item: dict) -> str:
        host = self.cfg.host.rstrip("/")
        path = "/".join(quote(item[k], safe="") for k in ("catalog_name", "schema_name", "table_name"))
        return f"{host}/explore/data/{path}"

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
