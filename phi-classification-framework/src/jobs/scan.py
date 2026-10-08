"""<env>_phi_classification_scan: find columns that need PHI classification review.

Reads metadata, tags, attestations, and (when readable) Data Classification results. It never reads
the data in scanned tables. With --correction_column it opens a CORRECTION_REQUESTED item for one
column instead of running the full scan.
"""
import json
import logging
import sys
from datetime import timedelta

# Make the bundle's src directory importable; the job passes --src_root ${workspace.file_path}/src.
if "--src_root" in sys.argv:
    sys.path.insert(0, sys.argv[sys.argv.index("--src_root") + 1])

from phi_framework import constants as c  # noqa: E402
from phi_framework import config, ddl, logic  # noqa: E402
from phi_framework.keys import column_fingerprint, make_key  # noqa: E402
from phi_framework.runtime import configure_logging, get_spark  # noqa: E402
from phi_framework.store import Store, allowed_tag_values, catalog_tags, utcnow  # noqa: E402

log = logging.getLogger("phi.scan")


def _extra(p):
    p.add_argument("--class_tag_mapping", default="{}",
                   help="JSON map of Data Classification class_tag to an allowed tag value.")
    p.add_argument("--correction_column", default="",
                   help="catalog.schema.table.column to reopen for a correction.")
    p.add_argument("--correction_reason", default="")
    p.add_argument("--correction_requested_by", default="")


def _ident(r):
    return tuple(r[k].lower() for k in ("catalog_name", "schema_name", "table_name", "column_name"))


def load_scopes(store: Store, env: str):
    rows = store.rows(
        f"""
        SELECT scope_id, catalog_name, schema_name, risk_tier, steward_group
        FROM {store.cfg.table('classification_scope_registry')}
        WHERE environment = '{env}' AND scan_enabled
          AND current_date() BETWEEN effective_date AND coalesce(expiration_date, date'9999-12-31')
        """
    )
    return [logic.Scope(**r) for r in rows]


def load_columns(store: Store, scopes):
    """In-scope columns from each scoped catalog's own information_schema."""
    cfg = store.cfg
    types = ", ".join(f"'{t}'" for t in c.SCANNED_TABLE_TYPES)
    out = []
    for cat in sorted({s.catalog_name for s in scopes}):
        schemas = {s.schema_name for s in scopes if s.catalog_name == cat}
        schema_filter = ""
        if "*" not in schemas:
            schema_filter = "AND c.table_schema IN (" + ", ".join(f"'{s}'" for s in sorted(schemas)) + ")"
        rows = store.rows(
            f"""
            SELECT c.table_catalog AS catalog_name, c.table_schema AS schema_name, c.table_name,
                   t.table_type, c.column_name, c.full_data_type, c.comment AS column_comment
            FROM `{cat}`.information_schema.columns c
            JOIN `{cat}`.information_schema.tables t
              ON t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema
             AND t.table_name = c.table_name
            WHERE c.table_schema <> 'information_schema' AND t.table_type IN ({types}) {schema_filter}
            """
        )
        for r in rows:
            if r["catalog_name"] == cfg.catalog and r["schema_name"] == cfg.governance_schema:
                continue
            if r["table_name"].startswith(c.EXCLUDED_TABLE_PREFIXES):
                continue
            scope = logic.resolve_scope(scopes, r["catalog_name"], r["schema_name"])
            if scope is None:
                continue
            r["scope"] = scope
            r["column_fingerprint"] = column_fingerprint(
                r["catalog_name"], r["schema_name"], r["table_name"], r["column_name"],
                r["full_data_type"], r["column_comment"],
            )
            out.append(r)
    return out


def load_detections(store: Store, catalogs):
    """High-signal Data Classification detections, or {} when results are not readable."""
    cats = ", ".join(f"'{x}'" for x in sorted(set(catalogs)))
    try:
        rows = store.rows(
            f"""
            SELECT catalog_name, schema_name, table_name, column_name, class_tag, confidence,
                   frequency, latest_detected_time
            FROM system.data_classification.results WHERE catalog_name IN ({cats})
            """
        )
    except Exception as e:  # noqa: BLE001 - access is optional; suggestions degrade to none
        log.warning("Data Classification results unavailable; no suggestions this run: %s", str(e)[:300])
        return {}
    out = {}
    for r in rows:
        out.setdefault(_ident(r), []).append(r)
    return out


def main(argv=None):
    configure_logging()
    cfg, args = config.parse(__doc__, argv, _extra)
    spark = get_spark()
    store = Store(spark, cfg)
    now = utcnow()
    mapping = json.loads(args.class_tag_mapping or "{}")

    scopes = load_scopes(store, cfg.env)
    if not scopes:
        log.info("No active scopes for %s; nothing to scan.", cfg.env)
        return
    outside = sorted({s.scope_id for s in scopes if s.catalog_name not in cfg.allowed_catalogs})
    if outside:
        for sid in outside:
            store.record_batch_error(sid, f"Environment guardrail: catalog of {sid} is not allowed in {cfg.env}.")
        raise RuntimeError(f"Scopes outside the environment's allowed catalogs: {outside}")
    conflicts = logic.find_scope_conflicts(scopes)
    if conflicts:
        for target, ids in conflicts:
            for sid in ids:
                store.record_batch_error(sid, f"Conflicting active scope rows for {target}: {ids}")
        raise RuntimeError(f"Scope conflicts, scan stopped without opening items: {conflicts}")

    allowed = allowed_tag_values(cfg.tag_key)
    columns = load_columns(store, scopes)
    catalogs = {s.catalog_name for s in scopes}
    tags = catalog_tags(store, catalogs, cfg.tag_key)
    detections = load_detections(store, catalogs) if mapping else {}

    items = store.rows(f"SELECT * FROM {cfg.table('classification_review_item')}")
    open_items = {_ident(i): i for i in items if i["status"] not in c.TERMINAL_ITEM_STATUSES}
    verified = {}
    for i in sorted((i for i in items if i["status"] == c.TAG_VERIFIED), key=lambda i: i["applied_at"]):
        verified[_ident(i)] = logic.VerifiedReview(i["column_fingerprint"], i["reviewed_at"])
    attestations = {
        _ident(a): logic.ActiveAttestation(a["column_fingerprint"], a["expires_at"])
        for a in store.rows(
            f"SELECT * FROM {cfg.table('classification_attestation')} WHERE superseded_at IS NULL"
        )
    }

    correction = None
    if args.correction_column:
        parts = tuple(p.strip().lower() for p in args.correction_column.split("."))
        if len(parts) != 4:
            raise ValueError("--correction_column must be catalog.schema.table.column")
        matches = [col for col in columns if _ident(col) == parts]
        if not matches:
            raise ValueError(f"{args.correction_column} is not an in-scope column")
        if parts in open_items:
            log.info("Correction not opened: %s already has open item %s; decide on that item instead.",
                     args.correction_column, open_items[parts]["review_item_id"])
            return
        correction = parts
        columns = matches

    new_items, refreshed = [], []
    for col in columns:
        ident = _ident(col)
        tag_value = tags.get(ident)
        scope = col["scope"]
        issue = logic.classify_column(
            col["column_fingerprint"], tag_value, allowed, verified.get(ident), attestations.get(ident),
            correction == ident, c.RISK_TIER_MONTHS[scope.risk_tier], now,
        )
        if issue is None:
            continue
        suggested, reason = logic.suggest_value(issue, tag_value, allowed, detections.get(ident, []), mapping)
        details = {
            "table_type": col["table_type"],
            "full_data_type": col["full_data_type"],
            "column_comment": col["column_comment"],
            "column_fingerprint": col["column_fingerprint"],
            "issue_type": issue,
            "current_tag_value": tag_value,
            "suggested_tag_value": suggested,
            "suggestion_reason": reason,
        }
        existing = open_items.get(ident)
        if existing is None:
            new_items.append((col, details))
        elif any(existing[k] != v for k, v in details.items()):
            refreshed.append({"review_item_id": existing["review_item_id"], **details})

    # Open new items, one batch per scope for this run.
    batches, item_rows, events = {}, [], []
    for col, details in new_items:
        scope = col["scope"]
        batch_id = make_key("BATCH", [cfg.run_id, scope.scope_id])
        batches.setdefault(scope.scope_id, {
            "review_batch_id": batch_id, "scope_id": scope.scope_id, "scan_run_id": cfg.run_id,
            "status": c.BATCH_OPEN, "created_at": now, "due_at": now + timedelta(days=c.REVIEW_DUE_DAYS),
            "closed_at": None, "last_error": None,
        })
        item_id = make_key("ITEM", [col["catalog_name"], col["schema_name"], col["table_name"],
                                    col["column_name"], batch_id])
        item_rows.append({
            "review_item_id": item_id, "review_batch_id": batch_id,
            "catalog_name": col["catalog_name"], "schema_name": col["schema_name"],
            "table_name": col["table_name"], "column_name": col["column_name"], **details,
            "assigned_steward": None, "decision": None, "corrected_tag_value": None,
            "steward_comment": None, "reviewed_by": None, "reviewed_at": None,
            "status": c.PENDING_STEWARD_REVIEW, "applied_at": None, "application_error": None,
        })
        evt_details = {"issue_type": details["issue_type"]}
        actor = None
        if correction:
            evt_details["reason"] = args.correction_reason
            actor = args.correction_requested_by or None
        events.append(store.event(c.ITEM_OPENED, batch_id, item_id, actor, evt_details, now=now))
    for b in batches.values():
        events.append(store.event(c.BATCH_CREATED, b["review_batch_id"],
                                  details={"scope_id": b["scope_id"]}, now=now))

    store.insert_new(ddl.REVIEW_BATCH, list(batches.values()), now=now)
    store.insert_new(ddl.REVIEW_ITEM, item_rows, now=now)
    terminal = ", ".join(f"'{s}'" for s in c.TERMINAL_ITEM_STATUSES)
    store.update(ddl.REVIEW_ITEM, refreshed, list(refreshed[0].keys())[1:] if refreshed else [],
                 guard=f"t.status NOT IN ({terminal})", now=now)

    # Close open items whose column was removed or left scope (full scans only).
    closed = []
    if correction is None:
        present = {_ident(col) for col in columns}
        for ident, item in open_items.items():
            if ident not in present:
                closed.append(item)
        store.update(ddl.REVIEW_ITEM,
                     [{"review_item_id": i["review_item_id"], "status": c.CLOSED_NO_ACTION} for i in closed],
                     ["status"], guard=f"t.status NOT IN ({terminal})", now=now)
        events += [store.event(c.ITEM_CLOSED, i["review_batch_id"], i["review_item_id"],
                               details={"reason": "Column removed or left scope"}, now=now) for i in closed]
    store.write_events(events)
    store.close_completed_batches()
    log.info("Scan complete: %d new items in %d batches, %d refreshed, %d closed.",
             len(item_rows), len(batches), len(refreshed), len(closed))


if __name__ == "__main__":
    main()
