"""<env>_phi_classification_application: apply and verify validated tag actions.

Triggered by new rows in classification_application_plan. Plan rows are never changed; progress is
recorded on the review item and in events. Before executing, the job revalidates the column's
fingerprint and object type, so a column that changed since approval is never tagged.
"""
import logging
import sys
import time

# Make the bundle's src directory importable; the job passes --src_root ${workspace.file_path}/src.
if "--src_root" in sys.argv:
    sys.path.insert(0, sys.argv[sys.argv.index("--src_root") + 1])

from phi_framework import constants as c  # noqa: E402
from phi_framework import config, ddl  # noqa: E402
from phi_framework.keys import column_fingerprint  # noqa: E402
from phi_framework.logic import quote_literal  # noqa: E402
from phi_framework.runtime import configure_logging, get_spark  # noqa: E402
from phi_framework.store import Store, utcnow  # noqa: E402

log = logging.getLogger("phi.apply")

VERIFY_ATTEMPTS = 6
VERIFY_DELAY_SECONDS = 5


def _current_column(store: Store, plan: dict):
    cat = plan["catalog_name"]
    rows = store.rows(
        f"""
        SELECT c.full_data_type, c.comment, t.table_type
        FROM `{cat}`.information_schema.columns c
        JOIN `{cat}`.information_schema.tables t
          ON t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema
         AND t.table_name = c.table_name
        WHERE c.table_schema = {quote_literal(plan['schema_name'])}
          AND c.table_name = {quote_literal(plan['table_name'])}
          AND c.column_name = {quote_literal(plan['column_name'])}
        """
    )
    return rows[0] if rows else None


def _tag_value(store: Store, plan: dict, tag_key: str):
    rows = store.rows(
        f"""
        SELECT tag_value FROM `{plan['catalog_name']}`.information_schema.column_tags
        WHERE schema_name = {quote_literal(plan['schema_name'])}
          AND table_name = {quote_literal(plan['table_name'])}
          AND column_name = {quote_literal(plan['column_name'])}
          AND tag_name = {quote_literal(tag_key)}
        """
    )
    return rows[0]["tag_value"] if rows else None


def _verify(store: Store, plan: dict, tag_key: str) -> bool:
    expected = plan["approved_tag_value"] if plan["action_type"] == c.SET_PHI_TAG else None
    for attempt in range(VERIFY_ATTEMPTS):
        if _tag_value(store, plan, tag_key) == expected:
            return True
        time.sleep(VERIFY_DELAY_SECONDS * (attempt + 1))
    return False


def apply_plan(store: Store, plan: dict) -> tuple:
    """Return (item_update, event). Never raises for a single plan failure."""
    cfg = store.cfg
    item_id, batch_id = plan["review_item_id"], plan["review_batch_id"]
    if plan["catalog_name"] not in cfg.allowed_catalogs:
        error = f"Environment guardrail: {plan['catalog_name']} is not allowed in {cfg.env}."
    else:
        current = _current_column(store, plan)
        if current is None:
            error = "Column no longer exists."
        elif current["table_type"] != plan["table_type"]:
            error = f"Object type changed from {plan['table_type']} to {current['table_type']}."
        elif column_fingerprint(plan["catalog_name"], plan["schema_name"], plan["table_name"],
                                plan["column_name"], current["full_data_type"],
                                current["comment"]) != plan["column_fingerprint"]:
            error = "Column metadata changed since approval; review again."
        else:
            error = None
            try:
                store.sql(plan["rendered_ddl"])
            except Exception as e:  # noqa: BLE001 - recorded on the item for remediation
                error = f"Tag statement failed: {str(e)[:500]}"
            if error is None and not _verify(store, plan, cfg.tag_key):
                error = "Verification failed: Unity Catalog tag state does not match the plan."
    now = utcnow()
    if error:
        return (
            {"review_item_id": item_id, "status": c.APPLICATION_FAILED, "application_error": error,
             "applied_at": None},
            store.event(c.EVT_APPLICATION_FAILED, batch_id, item_id, plan["reviewed_by"],
                        {"application_plan_id": plan["application_plan_id"], "error": error}, now=now),
        )
    if plan["action_type"] == c.SET_PHI_TAG:
        return (
            {"review_item_id": item_id, "status": c.TAG_VERIFIED, "application_error": None, "applied_at": now},
            store.event(c.EVT_TAG_VERIFIED, batch_id, item_id, plan["reviewed_by"],
                        {"verified_value": plan["approved_tag_value"]}, now=now),
        )
    return (
        {"review_item_id": item_id, "status": c.ATTESTED_NOT_PHI, "application_error": None, "applied_at": now},
        store.event(c.TAG_REMOVED, batch_id, item_id, plan["reviewed_by"],
                    {"application_plan_id": plan["application_plan_id"]}, now=now),
    )


def main(argv=None):
    configure_logging()
    cfg, _ = config.parse(__doc__, argv)
    store = Store(get_spark(), cfg)
    plans = store.rows(
        f"""
        SELECT p.* FROM {cfg.table('classification_application_plan')} p
        JOIN {cfg.table('classification_review_item')} i ON i.review_item_id = p.review_item_id
        WHERE i.status IN ('{c.PLANNED}', '{c.APPLICATION_IN_PROGRESS}', '{c.APPLICATION_FAILED}')
        ORDER BY p.audit_created_at
        """
    )
    if not plans:
        log.info("No plans to apply.")
        store.close_completed_batches()
        return
    in_progress = [{"review_item_id": p["review_item_id"], "status": c.APPLICATION_IN_PROGRESS} for p in plans]
    store.update(ddl.REVIEW_ITEM, in_progress, ["status"])

    updates, events = [], []
    for plan in plans:
        update, event = apply_plan(store, plan)
        updates.append(update)
        events.append(event)
    store.update(ddl.REVIEW_ITEM, updates, ["status", "application_error", "applied_at"])
    store.write_events(events)
    store.close_completed_batches()
    failed = sum(1 for u in updates if u["status"] == c.APPLICATION_FAILED)
    log.info("Applied %d plans; %d failed.", len(plans), failed)


if __name__ == "__main__":
    main()
