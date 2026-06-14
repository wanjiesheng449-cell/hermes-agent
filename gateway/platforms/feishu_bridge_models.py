from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FeishuInboundKind(str, Enum):
    NEW_TASK = "new_task"
    STATUS_QUERY = "status_query"
    USER_REPLY = "user_reply"
    CANCEL = "cancel"


class FeishuRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_USER = "waiting_user"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_active(self) -> bool:
        return self in {
            FeishuRunStatus.QUEUED,
            FeishuRunStatus.RUNNING,
            FeishuRunStatus.WAITING_USER,
            FeishuRunStatus.RETRYING,
        }


@dataclass(frozen=True)
class FeishuRouteDecision:
    kind: FeishuInboundKind
    normalized_text: str


_STATUS_QUERIES = {
    "干完了吗",
    "到哪一步了",
    "现在到哪一步了",
    "什么情况",
    "进度",
}

_CANCEL_QUERIES = {
    "取消",
    "停止",
    "别跑了",
    "算了",
}


def classify_feishu_text(text: str) -> FeishuRouteDecision:
    normalized = (text or "").strip()
    if normalized in _STATUS_QUERIES:
        return FeishuRouteDecision(kind=FeishuInboundKind.STATUS_QUERY, normalized_text=normalized)
    if normalized in _CANCEL_QUERIES:
        return FeishuRouteDecision(kind=FeishuInboundKind.CANCEL, normalized_text=normalized)
    return FeishuRouteDecision(kind=FeishuInboundKind.NEW_TASK, normalized_text=normalized)


def make_reply_key(run_id: str, reply_type: str, *, checkpoint_version: int | None = None) -> str:
    if checkpoint_version is not None:
        return f"run:{run_id}:{reply_type}:{checkpoint_version}"
    return f"run:{run_id}:{reply_type}"
