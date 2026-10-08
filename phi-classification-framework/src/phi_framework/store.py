"""Spark data access shared by the framework jobs.

Every governance-table write goes through this module so audit columns are populated the same way
everywhere: on insert the audit_updated_* columns equal audit_created_*, and updates touch only
audit_updated_*. Inserts use insert-only MERGE on deterministic keys, so reruns never duplicate rows.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from . import constants as c
from . import ddl
from .config import FrameworkConfig
from .keys import make_key

log = logging.getLogger(__name__)

_SPARK_TYPES = {
    "STRING": "string",
    "TIMESTAMP": "timestamp",
    "DATE": "date",
    "BOOLEAN": "boolean",
    "VARIANT": "string",  # Carried as JSON text and parsed with parse_json on write.
}


def utcnow() -> datetime:
    """Naive UTC timestamp; the Spark session time zone is pinned to UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


class Store:
    def __init__(self, spark, cfg: FrameworkConfig):
        self.spark = spark
        self.cfg = cfg
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        self.user = spark.sql("SELECT current_user()").first()[0]
        self._view_seq = 0

    # ------------------------------------------------------------ reads

    def sql(self, query: str):
        return self.spark.sql(query)

    def rows(self, query: str) -> List[dict]:
        return [r.asDict(recursive=True) for r in self.spark.sql(query).collect()]

    # ------------------------------------------------------------ writes

    def _audit(self, tdef: ddl.TableDef, now: datetime) -> Dict[str, object]:
        audit = {
            "audit_created_at": now,
            "audit_created_by": self.user,
            "audit_created_process": self.cfg.process_name,
            "audit_created_run_id": self.cfg.run_id,
        }
        if not tdef.append_only:
            audit.update(
                audit_updated_at=now,
                audit_updated_by=self.user,
                audit_updated_process=self.cfg.process_name,
                audit_updated_run_id=self.cfg.run_id,
            )
        return audit

    def _temp_view(self, tdef: ddl.TableDef, rows: Sequence[dict], columns: Sequence[str]) -> str:
        types = {col.name: col.type for col in tdef.columns}
        schema = ", ".join(f"`{name}` {_SPARK_TYPES[types[name]]}" for name in columns)
        data = [tuple(r.get(name) for name in columns) for r in rows]
        self._view_seq += 1
        view = f"_phi_src_{tdef.name}_{self._view_seq}"
        self.spark.createDataFrame(data, schema).createOrReplaceTempView(view)
        return view

    @staticmethod
    def _value_expr(tdef: ddl.TableDef, name: str) -> str:
        col_type = next(col.type for col in tdef.columns if col.name == name)
        return f"parse_json(s.`{name}`)" if col_type == "VARIANT" else f"s.`{name}`"

    def insert_new(self, tdef: ddl.TableDef, rows: Sequence[dict], now: Optional[datetime] = None) -> int:
        """Insert rows whose primary key does not exist yet. Returns the number of source rows."""
        if not rows:
            return 0
        now = now or utcnow()
        full = [{**r, **self._audit(tdef, now)} for r in rows]
        columns = [col.name for col in tdef.columns]
        view = self._temp_view(tdef, full, columns)
        values = ", ".join(self._value_expr(tdef, n) for n in columns)
        target = self.cfg.table(tdef.name)
        if tdef.append_only:
            # Blind append: a MERGE reads the whole target and conflicts with concurrent appends from
            # other jobs. Keys already present are filtered out first; keys include the run ID and each
            # job runs one at a time, so no concurrent writer can add the same key.
            existing = {r[0] for r in self.spark.sql(
                f"SELECT s.{tdef.pk} FROM {view} s LEFT SEMI JOIN {target} t ON t.{tdef.pk} = s.{tdef.pk}").collect()}
            full = [r for r in full if r[tdef.pk] not in existing]
            if not full:
                return 0
            view = self._temp_view(tdef, full, columns)
            self.spark.sql(f"INSERT INTO {target} ({', '.join(columns)}) SELECT {values} FROM {view} s")
            return len(full)
        self.spark.sql(
            f"MERGE INTO {target} t USING {view} s ON t.{tdef.pk} = s.{tdef.pk} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({values})"
        )
        return len(rows)

    def update(
        self,
        tdef: ddl.TableDef,
        rows: Sequence[dict],
        update_columns: Sequence[str],
        guard: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Update `update_columns` on existing rows matched by primary key.

        `guard` is an optional SQL predicate on the target row (alias t), e.g. a status check, so a
        concurrent change is never overwritten by a stale decision.
        """
        if tdef.append_only:
            raise ValueError(f"{tdef.name} is append-only")
        if not rows:
            return 0
        now = now or utcnow()
        columns = [tdef.pk, *update_columns]
        view = self._temp_view(tdef, rows, columns)
        sets = [f"t.`{n}` = {self._value_expr(tdef, n)}" for n in update_columns]
        sets += [
            f"t.audit_updated_at = timestamp'{now.isoformat(sep=' ')}'",
            f"t.audit_updated_by = '{self.user}'",
            f"t.audit_updated_process = '{self.cfg.process_name}'",
            f"t.audit_updated_run_id = '{self.cfg.run_id}'",
        ]
        cond = f" AND ({guard})" if guard else ""
        self.spark.sql(
            f"MERGE INTO {self.cfg.table(tdef.name)} t USING {view} s ON t.{tdef.pk} = s.{tdef.pk} "
            f"WHEN MATCHED{cond} THEN UPDATE SET {', '.join(sets)}"
        )
        return len(rows)

    # ------------------------------------------------------------ events

    def event(
        self,
        event_type: str,
        review_batch_id: str,
        review_item_id: Optional[str] = None,
        business_actor: Optional[str] = None,
        details: Optional[dict] = None,
        discriminator: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict:
        return {
            "event_id": make_key(
                "EVT", [event_type, review_batch_id, review_item_id, self.cfg.run_id, discriminator]
            ),
            "review_batch_id": review_batch_id,
            "review_item_id": review_item_id,
            "event_type": event_type,
            "event_timestamp": now or utcnow(),
            "business_actor": business_actor,
            "event_details": json.dumps(details, default=str) if details is not None else None,
            "discriminator": discriminator,
        }

    def write_events(self, events: Iterable[dict]) -> int:
        events = list(events)
        return self.insert_new(ddl.REVIEW_EVENT, events)

    # ------------------------------------------------------------ shared workflow steps

    def close_completed_batches(self) -> int:
        """Close OPEN batches whose items are all terminal and record BATCH_CLOSED."""
        terminal = ", ".join(f"'{s}'" for s in c.TERMINAL_ITEM_STATUSES)
        ready = self.rows(
            f"""
            SELECT b.review_batch_id
            FROM {self.cfg.table('classification_review_batch')} b
            WHERE b.status = '{c.BATCH_OPEN}'
              AND EXISTS (SELECT 1 FROM {self.cfg.table('classification_review_item')} i
                          WHERE i.review_batch_id = b.review_batch_id)
              AND NOT EXISTS (SELECT 1 FROM {self.cfg.table('classification_review_item')} i
                              WHERE i.review_batch_id = b.review_batch_id
                                AND i.status NOT IN ({terminal}))
            """
        )
        if not ready:
            return 0
        now = utcnow()
        self.update(
            ddl.REVIEW_BATCH,
            [{"review_batch_id": r["review_batch_id"], "status": c.BATCH_CLOSED, "closed_at": now} for r in ready],
            ["status", "closed_at"],
            guard=f"t.status = '{c.BATCH_OPEN}'",
            now=now,
        )
        self.write_events(self.event(c.BATCH_CLOSED_EVT, r["review_batch_id"], now=now) for r in ready)
        log.info("Closed %d batches", len(ready))
        return len(ready)

    def record_batch_error(self, scope_id: str, message: str) -> None:
        """Record a batch-level failure on a CLOSED error batch for this run, plus BATCH_ERROR."""
        now = utcnow()
        batch_id = make_key("BATCH", [self.cfg.run_id, scope_id])
        self.insert_new(
            ddl.REVIEW_BATCH,
            [
                {
                    "review_batch_id": batch_id,
                    "scope_id": scope_id,
                    "scan_run_id": self.cfg.run_id,
                    "status": c.BATCH_CLOSED,
                    "created_at": now,
                    "due_at": now,
                    "closed_at": now,
                    "last_error": message,
                }
            ],
            now=now,
        )
        self.update(ddl.REVIEW_BATCH, [{"review_batch_id": batch_id, "last_error": message}], ["last_error"], now=now)
        self.write_events([self.event(c.BATCH_ERROR, batch_id, details={"error": message}, now=now)])


def catalog_tags(store: Store, catalogs: Iterable[str], tag_key: str) -> Dict[tuple, str]:
    """Current direct tag values keyed by (catalog, schema, table, column), lower-cased identity."""
    out: Dict[tuple, str] = {}
    for cat in sorted(set(catalogs)):
        for r in store.rows(
            f"SELECT catalog_name, schema_name, table_name, column_name, tag_value "
            f"FROM `{cat}`.information_schema.column_tags WHERE tag_name = '{tag_key}'"
        ):
            key = tuple(r[k].lower() for k in ("catalog_name", "schema_name", "table_name", "column_name"))
            out[key] = r["tag_value"]
    return out


def allowed_tag_values(tag_key: str) -> List[str]:
    """Allowed values from the governed tag policy (the single source of truth)."""
    from databricks.sdk import WorkspaceClient

    resp = WorkspaceClient().api_client.do("GET", f"/api/2.1/tag-policies/{tag_key}")
    values = [v["name"] for v in resp.get("values", [])]
    if not values:
        raise RuntimeError(
            f"Governed tag {tag_key!r} has no allowed values; define them before running the framework."
        )
    return values
