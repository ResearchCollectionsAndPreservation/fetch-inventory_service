from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from typing import Iterable, Set

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Set, Iterable, Optional

from sqlmodel import select
from sqlalchemy.orm import joinedload

from app.database.session import session_manager
from app.models.groups import Group


@dataclass
class PermissionsCache:
    """
    Keeps a map of group_name -> set(permission_name) in memory.
    Refreshes at most every `ttl_seconds`.
    """
    ttl_seconds: int = 15 * 60

    _group_to_perms: Dict[str, Set[str]] = field(default_factory=dict, init=False)
    _last_refresh: float = field(default=0.0, init=False)

    # simple in-process lock (good enough for single-process dev / typical uvicorn single worker)
    # if you run multiple workers, each worker will maintain its own cache (usually fine).
    _refreshing: bool = field(default=False, init=False)

    def _load_from_db(self) -> Dict[str, Set[str]]:
        with session_manager() as session:
            groups = (
                session.exec(
                    select(Group).options(joinedload(Group.permissions))
                )
                .unique()
                .all()
            )

            mapping: Dict[str, Set[str]] = {}
            for g in groups:
                group_name = getattr(g, "name", None)
                if not group_name:
                    continue

                perms = set()
                for p in (getattr(g, "permissions", None) or []):
                    perm_name = getattr(p, "name", None)
                    if perm_name:
                        perms.add(perm_name)

                mapping[group_name] = perms

        return mapping

    def refresh_if_needed(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_refresh) < self.ttl_seconds:
            return
        if self._refreshing:
            # Another request is already refreshing. Use stale-but-recent data.
            return

        self._refreshing = True
        try:
            self._group_to_perms = self._load_from_db()
            self._last_refresh = time.time()
        finally:
            self._refreshing = False

    def permissions_for_groups(self, group_names: Iterable[str]) -> Set[str]:
        # Ensure cache is warm
        self.refresh_if_needed()

        perms: Set[str] = set()
        for name in group_names:
            perms |= self._group_to_perms.get(name, set())
        return perms

    def snapshot(self) -> Dict[str, Set[str]]:
        """For debugging/inspection (don’t expose publicly)."""
        self.refresh_if_needed()
        return {k: set(v) for k, v in self._group_to_perms.items()}


permissions_cache = PermissionsCache()


def require_permissions(*required: str, any_of: bool = False):
    required_set = set(required)

    def checker(request: Request):
        role_string = getattr(request.state, "role", None) or ""
        user_roles = role_string.split(":") if role_string else []

        user_perms = permissions_cache.permissions_for_groups(user_roles)

        ok = (bool(required_set & user_perms) if any_of else required_set.issubset(user_perms))
        if not ok:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

        return True

    return checker
