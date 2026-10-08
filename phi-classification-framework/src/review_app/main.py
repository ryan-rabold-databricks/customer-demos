"""FastAPI entry point for the steward review app."""
import logging
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from phi_framework import constants as c
from phi_framework.logic import validate_decision

from . import settings as settings_mod
from . import ui
from .backend import Backend, Session, ident, summarize
from .identity import User, current_user, submitter_role

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("phi.app")

HERE = Path(__file__).resolve().parent
SETTINGS = settings_mod.load()
BACKEND = Backend(SETTINGS)

app = FastAPI(title="PHI Classification Review", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
templates = Jinja2Templates(directory=HERE / "templates")
templates.env.globals.update(status_label=ui.status_label, value_label=ui.value_label,
                             event_label=lambda t: ui.EVENT_LABELS.get(t, t),
                             decision_label=lambda d: ui.DECISION_LABELS.get(d, d or ""))

_COLUMN_FQN = re.compile(r"^[^.\s`]+\.[^.\s`]+\.[^.\s`]+\.[^.\s`]+$")
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")


def _render(request: Request, template: str, user: User, **ctx) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        template,
        {
            "user": user,
            "is_governance": user.is_governance(SETTINGS.governance_group),
            "env": SETTINGS.env,
            "message": request.query_params.get("msg"),
            "error": request.query_params.get("err"),
            **ctx,
        },
    )


def _redirect(path: str, msg: Optional[str] = None, err: Optional[str] = None) -> RedirectResponse:
    query = "&".join(f"{k}={quote(v)}" for k, v in (("msg", msg), ("err", err)) if v)
    return RedirectResponse(f"{path}?{query}" if query else path, status_code=303)


def _require_governance(user: User) -> None:
    if not user.is_governance(SETTINGS.governance_group):
        raise HTTPException(status_code=403, detail="Data Governance only")


def _queue(db: Session, user: User):
    """The user's queue: (assigned to me, my steward groups, my decisions awaiting validation).

    Items with a decision awaiting ingestion leave the queue; a rejected decision brings the item
    back as VALIDATION_FAILED.
    """
    items = db.open_items()
    pending = [s for s in db.pending_submissions() if s["submission_type"] == c.SUBMISSION_DECISION]
    awaiting = {s["review_item_id"] for s in pending}
    mine_awaiting = len({s["review_item_id"] for s in pending if s["submitted_by"].lower() == user.email})
    decidable = [i for i in items if i["status"] in c.DECIDABLE_ITEM_STATUSES and i["review_item_id"] not in awaiting]
    decidable = [i for i in decidable if submitter_role(user, i, SETTINGS.governance_group) is not None]
    mine = [i for i in decidable if (i["assigned_steward"] or "").lower() == user.email]
    group = [i for i in decidable if i not in mine and i["steward_group"] in user.groups]
    return mine, group, mine_awaiting


def _annotate(db: Session, items: List[dict]) -> List[dict]:
    """Add the plain-language reason, status, and any rejection reason to each item."""
    priors = db.prior_resolutions(items)
    rejected = db.rejection_reasons(i["review_item_id"] for i in items if i["status"] == c.VALIDATION_FAILED)
    out = []
    for i in items:
        text, tone = ui.status_label(i["status"])
        out.append({**i, "reason": ui.issue_sentence(i, priors.get(ident(i))), "status_text": text,
                    "tone": tone, "rejection": rejected.get(i["review_item_id"])})
    return out


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def queue(request: Request, user: User = Depends(current_user)):
    with BACKEND.session() as db:
        mine, group, awaiting = _queue(db, user)
        rows = _annotate(db, mine + group)
    by_id = {r["review_item_id"]: r for r in rows}
    return _render(
        request, "queue.html", user, awaiting=awaiting,
        mine=ui.group_by_table([by_id[i["review_item_id"]] for i in mine]),
        group=ui.group_by_table([by_id[i["review_item_id"]] for i in group]),
    )


@app.get("/items/{review_item_id}", response_class=HTMLResponse)
def item_detail(review_item_id: str, request: Request, user: User = Depends(current_user)):
    with BACKEND.session() as db:
        item = db.item(review_item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Review item not found")
        [annotated] = _annotate(db, [item])
        if item["issue_type"] == c.CORRECTION_REQUESTED:
            annotated["reason"] = ui.issue_sentence(item, None, db.correction_reason(review_item_id))
        pending = any(s["review_item_id"] == review_item_id and s["submission_type"] == c.SUBMISSION_DECISION
                      for s in db.pending_submissions())
        siblings = db.siblings(item)
        events = db.item_events(review_item_id)
    role = submitter_role(user, item, SETTINGS.governance_group)
    return _render(
        request, "item.html", user, item=annotated, role=role, pending=pending,
        can_decide=role is not None and item["status"] in c.DECIDABLE_ITEM_STATUSES and not pending,
        allowed=BACKEND.allowed_values(), policy_description=BACKEND.policy_description(),
        siblings=siblings, events=events, explorer_url=BACKEND.explorer_url(item),
    )


@app.post("/items/{review_item_id}/decide")
def decide(
    review_item_id: str,
    action: str = Form(...),
    tag_value: str = Form(""),
    comment: str = Form(""),
    after: str = Form("stay"),
    user: User = Depends(current_user),
):
    path = f"/items/{review_item_id}"
    with BACKEND.session() as db:
        item = db.item(review_item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Review item not found")
        role = submitter_role(user, item, SETTINGS.governance_group)
        if role is None or item["status"] not in c.DECIDABLE_ITEM_STATUSES:
            return _redirect(path, err="You cannot submit a decision on this item.")
        decision, corrected = ui.decision_for(action, tag_value.strip(), item["suggested_tag_value"])
        # The same rule ingestion applies, checked here for immediate feedback.
        result = validate_decision(decision, item["status"], item["suggested_tag_value"], corrected,
                                   comment, BACKEND.allowed_values())
        if not result.ok:
            return _redirect(path, err=result.error)
        db.submit_many([{"submission_type": c.SUBMISSION_DECISION, "item": item, "submitted_by": user.email,
                         "submitter_role": role, "decision": decision, "corrected_tag_value": corrected,
                         "steward_comment": comment.strip()}])
        log.info("Decision %s on %s submitted by %s as %s", decision, review_item_id, user.email, role)
        if after == "next":
            mine, group, _ = _queue(db, user)
            nxt = next((i for i in mine + group if i["review_item_id"] != review_item_id), None)
            if nxt is None:
                return _redirect("/", msg="Decision submitted. Your queue is empty.")
            return _redirect(f"/items/{nxt['review_item_id']}", msg="Decision submitted. Here is your next item.")
    return _redirect(path, msg="Decision submitted. It is validated within a few minutes.")


@app.post("/bulk")
def bulk(
    action: str = Form(...),
    comment: str = Form(""),
    item_ids: List[str] = Form([]),
    user: User = Depends(current_user),
):
    if not item_ids:
        return _redirect("/", err="Select at least one column.")
    if action == ui.ACTION_NOT_PHI and not comment.strip():
        return _redirect("/", err="Enter a reason for marking the selected columns not PHI.")
    allowed = BACKEND.allowed_values()
    with BACKEND.session() as db:
        items = db.items(item_ids)
        awaiting = {s["review_item_id"] for s in db.pending_submissions()
                    if s["submission_type"] == c.SUBMISSION_DECISION}
        rows, skipped = [], []
        for item_id in item_ids:
            item = items.get(item_id)
            role = submitter_role(user, item, SETTINGS.governance_group) if item else None
            if item is None or role is None or item["status"] not in c.DECIDABLE_ITEM_STATUSES \
                    or item_id in awaiting:
                skipped.append(item["column_name"] if item else item_id)
                continue
            tag_value = (item["suggested_tag_value"] or "") if action == ui.ACTION_PHI else ""
            decision, corrected = ui.decision_for(action, tag_value, item["suggested_tag_value"])
            if not validate_decision(decision, item["status"], item["suggested_tag_value"], corrected,
                                     comment, allowed).ok:
                skipped.append(item["column_name"])
                continue
            rows.append({"submission_type": c.SUBMISSION_DECISION, "item": item, "submitted_by": user.email,
                         "submitter_role": role, "decision": decision, "corrected_tag_value": corrected,
                         "steward_comment": comment.strip()})
        db.submit_many(rows)
    log.info("Bulk %s: %d submitted, %d skipped by %s", action, len(rows), len(skipped), user.email)
    msg = f"{len(rows)} decision{'s' if len(rows) != 1 else ''} submitted."
    if skipped:
        why = "no suggested value" if action == ui.ACTION_PHI else "not decidable by you right now"
        return _redirect("/", msg=msg, err=f"Skipped ({why}): {', '.join(sorted(skipped))}")
    return _redirect("/", msg=msg)


@app.get("/history", response_class=HTMLResponse)
def history(request: Request, user: User = Depends(current_user)):
    with BACKEND.session() as db:
        rows = db.history(user.email)
    for r in rows:
        r["outcome_text"], r["tone"] = ui.submission_outcome(r)
    return _render(request, "history.html", user, rows=rows)


@app.get("/governance", response_class=HTMLResponse)
def governance(request: Request, user: User = Depends(current_user)):
    _require_governance(user)
    with BACKEND.session() as db:
        items = _annotate(db, db.open_items())
        batches = db.batches()
        pending = {s["review_item_id"] for s in db.pending_submissions()}
    return _render(request, "governance.html", user, items=items, batches=batches,
                   counts=summarize(items), pending=pending)


@app.post("/items/{review_item_id}/assign")
def assign(review_item_id: str, assigned_steward: str = Form(...), user: User = Depends(current_user)):
    _require_governance(user)
    assigned_steward = assigned_steward.strip().lower()
    if not _EMAIL.match(assigned_steward):
        return _redirect("/governance", err="Enter the steward's email address.")
    with BACKEND.session() as db:
        item = db.item(review_item_id)
        if item is None or item["status"] in c.TERMINAL_ITEM_STATUSES:
            return _redirect("/governance", err="That item cannot be assigned.")
        db.submit_many([{"submission_type": c.SUBMISSION_ASSIGNMENT, "item": item, "submitted_by": user.email,
                         "submitter_role": c.ROLE_DATA_GOVERNANCE, "assigned_steward": assigned_steward}])
    return _redirect("/governance", msg=f"Assignment to {assigned_steward} submitted.")


@app.post("/corrections")
def request_correction(column_fqn: str = Form(...), reason: str = Form(...), user: User = Depends(current_user)):
    _require_governance(user)
    column_fqn, reason = column_fqn.strip(), reason.strip()
    if not _COLUMN_FQN.match(column_fqn) or not reason:
        return _redirect("/governance", err="Enter catalog.schema.table.column and a reason.")
    run_id = BACKEND.request_correction(column_fqn, reason, user.email)
    return _redirect("/governance", msg=f"Correction scan started (run {run_id}).")
