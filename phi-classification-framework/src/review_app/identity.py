"""Who is using the app, and what they may do.

The Databricks Apps proxy authenticates every request and forwards the user's email and an
on-behalf-of token. The token is used only to read the user's own group memberships; all data
access uses the app's service principal.
"""
import hashlib
import os
import time
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from fastapi import HTTPException, Request

from phi_framework import constants as c

_CACHE_SECONDS = 300
_group_cache: Dict[str, Tuple[float, FrozenSet[str]]] = {}


@dataclass(frozen=True)
class User:
    email: str
    groups: FrozenSet[str] = field(default_factory=frozenset)

    def is_governance(self, governance_group: str) -> bool:
        return governance_group in self.groups


def _groups_for(token: str) -> FrozenSet[str]:
    key = hashlib.sha256(token.encode()).hexdigest()
    hit = _group_cache.get(key)
    if hit and time.time() - hit[0] < _CACHE_SECONDS:
        return hit[1]
    w = WorkspaceClient(host=Config().host, token=token, auth_type="pat")
    groups = frozenset(g.display for g in (w.current_user.me().groups or []) if g.display)
    _group_cache[key] = (time.time(), groups)
    return groups


def current_user(request: Request) -> User:
    email = request.headers.get("x-forwarded-email")
    token = request.headers.get("x-forwarded-access-token")
    if email and token:
        return User(email=email.lower(), groups=_groups_for(token))
    # Local development only: no proxy headers are present.
    dev_email = os.getenv("DEV_USER_EMAIL")
    if dev_email:
        groups = frozenset(g.strip() for g in os.getenv("DEV_USER_GROUPS", "").split(",") if g.strip())
        return User(email=dev_email.lower(), groups=groups)
    raise HTTPException(status_code=401, detail="No authenticated user")


def submitter_role(user: User, item: dict, governance_group: str) -> Optional[str]:
    """The role under which `user` may decide `item`, or None when not authorized."""
    if item["status"] == c.NEEDS_PRIVACY_REVIEW:
        return c.ROLE_DATA_GOVERNANCE if user.is_governance(governance_group) else None
    if item.get("assigned_steward") and item["assigned_steward"].lower() == user.email:
        return c.ROLE_ASSIGNED_STEWARD
    if user.is_governance(governance_group):
        return c.ROLE_DATA_GOVERNANCE
    if item.get("steward_group") in user.groups:
        return c.ROLE_STEWARD_GROUP_MEMBER
    return None
