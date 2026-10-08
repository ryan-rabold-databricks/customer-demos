"""FastAPI entry point for the steward review app."""
import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from phi_framework import constants as c
from phi_framework.logic import validate_decision

from . import settings as settings_mod
from .backend import Backend, summarize
from .identity import User, current_user, submitter_role

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("phi.app")

HERE = Path(__file__).resolve().parent
SETTINGS = settings_mod.load()
BACKEND = Backend(SETTINGS)

app = FastAPI(title="PHI Classification Review", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
templates = Jinja2Templates(directory=HERE / "templates")

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


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def queue(request: Request, user: User = Depends(current_user)):
    items = BACKEND.open_items()
    # Items with a decision awaiting ingestion leave the queue; a rejected decision brings the item
    # back as VALIDATION_FAILED.
    pending_decisions = [s for s in BACKEND.pending_submissions() if s["submission_type"] == c.SUBMISSION_DECISION]
    awaiting = {s["review_item_id"] for s in pending_decisions}
    mine_awaiting = len({s["review_item_id"] for s in pending_decisions if s["submitted_by"].lower() == user.email})
    decidable = [
        i for i in items if i["status"] in c.DECIDABLE_ITEM_STATUSES and i["review_item_id"] not in awaiting
    ]
    mine = [i for i in decidable if (i["assigned_steward"] or "").lower() == user.email]
    group = [i for i in decidable if i not in mine and i["steward_group"] in user.groups]
    return _render(request, "queue.html", user, mine=mine, group=group, pending=set(),
                   awaiting=mine_awaiting, counts=summarize(items))


@app.get("/items/{review_item_id}", response_class=HTMLResponse)
def item_detail(review_item_id: str, request: Request, user: User = Depends(current_user)):
    item = BACKEND.item(review_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    role = submitter_role(user, item, SETTINGS.governance_group)
    return _render(
        request, "item.html", user, item=item, role=role,
        can_decide=role is not None and item["status"] in c.DECIDABLE_ITEM_STATUSES,
        decisions=c.DECISIONS, allowed=BACKEND.allowed_values(),
        events=BACKEND.item_events(review_item_id),
        pending=BACKEND.pending_submissions(review_item_id),
    )


@app.post("/items/{review_item_id}/decision")
def submit_decision(
    review_item_id: str,
    decision: str = Form(...),
    corrected_tag_value: str = Form(""),
    steward_comment: str = Form(""),
    user: User = Depends(current_user),
):
    path = f"/items/{review_item_id}"
    item = BACKEND.item(review_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    role = submitter_role(user, item, SETTINGS.governance_group)
    if role is None or item["status"] not in c.DECIDABLE_ITEM_STATUSES:
        return _redirect(path, err="You cannot submit a decision on this item.")
    if decision != c.CORRECT_CLASSIFICATION:
        corrected_tag_value = ""
    # Same rule ingestion applies, checked here for immediate feedback.
    result = validate_decision(decision, item["status"], item["suggested_tag_value"],
                               corrected_tag_value or None, steward_comment, BACKEND.allowed_values())
    if not result.ok:
        return _redirect(path, err=result.error)
    BACKEND.submit(c.SUBMISSION_DECISION, item, user.email, role, decision=decision,
                   corrected_tag_value=corrected_tag_value, steward_comment=steward_comment)
    log.info("Decision %s on %s submitted by %s as %s", decision, review_item_id, user.email, role)
    return _redirect(path, msg="Decision submitted. Ingestion validates it within a few minutes.")


@app.get("/governance", response_class=HTMLResponse)
def governance(request: Request, user: User = Depends(current_user)):
    _require_governance(user)
    items = BACKEND.open_items()
    return _render(request, "governance.html", user, items=items, batches=BACKEND.batches(),
                   counts=summarize(items), pending={s["review_item_id"] for s in BACKEND.pending_submissions()})


@app.post("/items/{review_item_id}/assign")
def assign(review_item_id: str, assigned_steward: str = Form(...), user: User = Depends(current_user)):
    _require_governance(user)
    assigned_steward = assigned_steward.strip().lower()
    if not _EMAIL.match(assigned_steward):
        return _redirect("/governance", err="Enter the steward's email address.")
    item = BACKEND.item(review_item_id)
    if item is None or item["status"] in c.TERMINAL_ITEM_STATUSES:
        return _redirect("/governance", err="That item cannot be assigned.")
    BACKEND.submit(c.SUBMISSION_ASSIGNMENT, item, user.email, c.ROLE_DATA_GOVERNANCE,
                   assigned_steward=assigned_steward)
    return _redirect("/governance", msg=f"Assignment to {assigned_steward} submitted.")


@app.post("/corrections")
def request_correction(column_fqn: str = Form(...), reason: str = Form(...), user: User = Depends(current_user)):
    _require_governance(user)
    column_fqn, reason = column_fqn.strip(), reason.strip()
    if not _COLUMN_FQN.match(column_fqn) or not reason:
        return _redirect("/governance", err="Enter catalog.schema.table.column and a reason.")
    run_id = BACKEND.request_correction(column_fqn, reason, user.email)
    return _redirect("/governance", msg=f"Correction scan started (run {run_id}).")
