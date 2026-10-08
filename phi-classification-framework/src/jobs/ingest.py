"""<env>_phi_decision_ingestion: validate decisions submitted through the review app.

Triggered by new rows in classification_decision_submission. Each submission is processed once
(tracked by a SUBMISSION_INGESTED event whose discriminator is the submission_id). Valid PHI
decisions become application plans, non-PHI decisions become attestations, and decisions on items
that were already accepted are recorded as CHANGE_IGNORED_ALREADY_RESOLVED and change nothing.
"""
import logging
import sys

# Make the bundle's src directory importable; the job passes --src_root ${workspace.file_path}/src.
if "--src_root" in sys.argv:
    sys.path.insert(0, sys.argv[sys.argv.index("--src_root") + 1])

from phi_framework import constants as c  # noqa: E402
from phi_framework import config, ddl, logic  # noqa: E402
from phi_framework.keys import make_key  # noqa: E402
from phi_framework.runtime import configure_logging, get_spark  # noqa: E402
from phi_framework.store import Store, allowed_tag_values, catalog_tags, utcnow  # noqa: E402

log = logging.getLogger("phi.ingest")

ITEM_DECISION_COLUMNS = [
    "assigned_steward", "decision", "corrected_tag_value", "steward_comment",
    "reviewed_by", "reviewed_at", "status",
]


def _ident(r):
    return tuple(r[k].lower() for k in ("catalog_name", "schema_name", "table_name", "column_name"))


def main(argv=None):
    configure_logging()
    cfg, _ = config.parse(__doc__, argv)
    store = Store(get_spark(), cfg)
    now = utcnow()

    pending = store.rows(
        f"""
        SELECT s.* FROM {cfg.table('classification_decision_submission')} s
        LEFT ANTI JOIN {cfg.table('classification_review_event')} e
          ON e.event_type = '{c.SUBMISSION_INGESTED}' AND e.discriminator = s.submission_id
        ORDER BY s.submitted_at, s.submission_id
        """
    )
    if not pending:
        log.info("No new submissions.")
        store.close_completed_batches()
        return

    item_ids = ", ".join(f"'{s['review_item_id']}'" for s in pending)
    items = {
        i["review_item_id"]: i
        for i in store.rows(
            f"""
            SELECT i.*, r.risk_tier
            FROM {cfg.table('classification_review_item')} i
            JOIN {cfg.table('classification_review_batch')} b ON b.review_batch_id = i.review_batch_id
            JOIN {cfg.table('classification_scope_registry')} r ON r.scope_id = b.scope_id
            WHERE i.review_item_id IN ({item_ids})
            """
        )
    }
    allowed = allowed_tag_values(cfg.tag_key)
    live_tags = catalog_tags(store, {i["catalog_name"] for i in items.values()}, cfg.tag_key)
    active_atts = {
        _ident(a): a
        for a in store.rows(
            f"SELECT * FROM {cfg.table('classification_attestation')} WHERE superseded_at IS NULL"
        )
    }

    events, plans, attestations, superseded, changed = [], [], [], [], {}
    for sub in pending:
        sid = sub["submission_id"]
        item = items.get(sub["review_item_id"])
        outcome = "processed"

        def evt(event_type, details=None, actor=None):
            events.append(store.event(event_type, sub["review_batch_id"], sub["review_item_id"],
                                      actor, details, discriminator=sid, now=now))

        if item is None:
            evt(c.SUBMISSION_INGESTED, {"outcome": "rejected", "error": "Unknown review item"})
            continue

        if sub["submission_type"] == c.SUBMISSION_ASSIGNMENT:
            if sub["submitter_role"] != c.ROLE_DATA_GOVERNANCE:
                outcome = "rejected: only Data Governance assigns stewards"
            elif item["status"] in c.TERMINAL_ITEM_STATUSES:
                outcome = "rejected: item is resolved"
            else:
                item["assigned_steward"] = sub["assigned_steward"]
                changed[item["review_item_id"]] = item
                evt(c.STEWARD_ASSIGNED, {"assigned_steward": sub["assigned_steward"]}, sub["submitted_by"])
            evt(c.SUBMISSION_INGESTED, {"type": sub["submission_type"], "outcome": outcome})
            continue

        submitted = {k: sub[k] for k in ("decision", "corrected_tag_value", "steward_comment")}
        if item["status"] not in c.DECIDABLE_ITEM_STATUSES:
            recorded = {k: item[k] for k in submitted}
            if submitted != recorded:
                evt(c.CHANGE_IGNORED_ALREADY_RESOLVED,
                    {"status": item["status"], "submitted": submitted, "recorded": recorded},
                    sub["submitted_by"])
            evt(c.SUBMISSION_INGESTED, {"type": sub["submission_type"], "outcome": "ignored: already accepted"})
            continue

        evt(c.EVT_DECISION_RECEIVED, submitted, sub["submitted_by"])
        auth = logic.authorize_reviewer(sub["submitter_role"], sub["submitted_by"],
                                        item["assigned_steward"], item["status"])
        result = auth if not auth.ok else logic.validate_decision(
            sub["decision"], item["status"], item["suggested_tag_value"],
            sub["corrected_tag_value"], sub["steward_comment"], allowed)
        item.update(submitted, reviewed_by=sub["submitted_by"], reviewed_at=sub["submitted_at"])
        changed[item["review_item_id"]] = item

        if not result.ok:
            item["status"] = c.VALIDATION_FAILED
            evt(c.EVT_VALIDATION_FAILED, {"error": result.error}, sub["submitted_by"])
            evt(c.SUBMISSION_INGESTED, {"type": sub["submission_type"], "outcome": "validation failed"})
            continue

        decision = sub["decision"]
        plan_action = plan_value = None
        if decision == c.REQUEST_PRIVACY_REVIEW:
            item["status"] = c.NEEDS_PRIVACY_REVIEW
            evt(c.PRIVACY_REVIEW_REQUESTED, {"question": sub["steward_comment"]}, sub["submitted_by"])
        elif decision in (c.APPROVE_SUGGESTION, c.CORRECT_CLASSIFICATION):
            plan_action = c.SET_PHI_TAG
            plan_value = item["suggested_tag_value"] if decision == c.APPROVE_SUGGESTION else sub["corrected_tag_value"]
        elif decision == c.CONFIRM_NOT_PHI:
            ident = _ident(item)
            prior = active_atts.get(ident)
            if prior is not None:
                superseded.append({"attestation_id": prior["attestation_id"], "superseded_at": now})
            att = {
                "attestation_id": make_key("ATT", [item["review_item_id"]]),
                "review_batch_id": item["review_batch_id"], "review_item_id": item["review_item_id"],
                "catalog_name": item["catalog_name"], "schema_name": item["schema_name"],
                "table_name": item["table_name"], "column_name": item["column_name"],
                "column_fingerprint": item["column_fingerprint"],
                "attestation_type": c.CONFIRMED_NOT_PHI, "rationale": sub["steward_comment"],
                "attested_by": sub["submitted_by"], "attested_at": now,
                "expires_at": logic.add_months(now, c.RISK_TIER_MONTHS[item["risk_tier"]]),
                "superseded_at": None,
            }
            attestations.append(att)
            active_atts[ident] = att
            evt(c.ATTESTATION_RECORDED, {"attestation_id": att["attestation_id"]}, sub["submitted_by"])
            if live_tags.get(ident) is not None:
                plan_action = c.REMOVE_PHI_TAG
            else:
                item["status"] = c.ATTESTED_NOT_PHI

        if plan_action:
            plan = {
                "application_plan_id": make_key("PLAN", [item["review_item_id"]]),
                "review_batch_id": item["review_batch_id"], "review_item_id": item["review_item_id"],
                "action_type": plan_action, "catalog_name": item["catalog_name"],
                "schema_name": item["schema_name"], "table_name": item["table_name"],
                "table_type": item["table_type"], "column_name": item["column_name"],
                "column_fingerprint": item["column_fingerprint"], "approved_tag_value": plan_value,
                "reviewed_by": sub["submitted_by"], "reviewed_at": sub["submitted_at"],
                "rendered_ddl": logic.render_tag_ddl(
                    plan_action, item["table_type"], item["catalog_name"], item["schema_name"],
                    item["table_name"], item["column_name"], cfg.tag_key, plan_value),
            }
            plans.append(plan)
            item["status"] = c.PLANNED
            evt(c.PLAN_CREATED, {"action_type": plan_action, "approved_tag_value": plan_value},
                sub["submitted_by"])
        evt(c.SUBMISSION_INGESTED, {"type": sub["submission_type"], "outcome": item["status"]})

    # Attestations and plans first, then item status, then the evidence trail.
    store.update(ddl.ATTESTATION, superseded, ["superseded_at"], guard="t.superseded_at IS NULL", now=now)
    store.insert_new(ddl.ATTESTATION, attestations, now=now)
    store.insert_new(ddl.APPLICATION_PLAN, plans, now=now)
    store.update(ddl.REVIEW_ITEM, list(changed.values()), ITEM_DECISION_COLUMNS, now=now)
    store.write_events(events)
    store.close_completed_batches()
    log.info("Ingested %d submissions: %d plans, %d attestations.", len(pending), len(plans), len(attestations))


if __name__ == "__main__":
    main()
