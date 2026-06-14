from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Lock

from gateway.platforms.feishu_bridge_models import FeishuRunStatus
from utils import atomic_json_write


@dataclass(frozen=True)
class InboundRecordResult:
    accepted: bool
    dedupe_key: str


@dataclass
class RunRecord:
    run_id: str
    conversation_id: str
    chat_id: str
    thread_id: str | None
    trigger_message_id: str
    status: str
    current_step: str
    progress_summary: str = ""
    waiting_reason: str = ""
    checkpoint_version: int | None = None


class FeishuBridgeStore:
    def __init__(self, state_path: Path) -> None:
        self._state_path = Path(state_path)
        self._lock = Lock()
        self._state = {
            "inbound": {},
            "runs": {},
            "replies": {},
        }
        if self._state_path.exists():
            self._state = json.loads(self._state_path.read_text(encoding="utf-8"))

    @property
    def state_path(self) -> Path:
        return self._state_path

    def _flush(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json_write(self._state_path, self._state, indent=2)

    def record_inbound(
        self,
        *,
        platform: str,
        tenant_id: str,
        chat_id: str,
        thread_id: str | None,
        message_id: str,
        event_id: str,
        normalized_text: str,
    ) -> InboundRecordResult:
        dedupe_key = f"{platform}:{tenant_id}:{message_id or event_id}"
        with self._lock:
            if dedupe_key in self._state["inbound"]:
                return InboundRecordResult(accepted=False, dedupe_key=dedupe_key)
            self._state["inbound"][dedupe_key] = {
                "chat_id": chat_id,
                "thread_id": thread_id,
                "normalized_text": normalized_text,
            }
            self._flush()
            return InboundRecordResult(accepted=True, dedupe_key=dedupe_key)

    def create_run(
        self,
        conversation_id: str,
        chat_id: str,
        thread_id: str | None,
        trigger_message_id: str,
    ) -> RunRecord:
        with self._lock:
            active = self.get_active_run(conversation_id)
            if active is not None:
                raise RuntimeError(f"active run already exists for {conversation_id}")
            run = RunRecord(
                run_id=str(uuid.uuid4()),
                conversation_id=conversation_id,
                chat_id=chat_id,
                thread_id=thread_id,
                trigger_message_id=trigger_message_id,
                status=FeishuRunStatus.QUEUED.value,
                current_step="queued",
                progress_summary="",
                waiting_reason="",
                checkpoint_version=None,
            )
            self._state["runs"][run.run_id] = asdict(run)
            self._flush()
            return run

    def get_active_run(self, conversation_id: str) -> RunRecord | None:
        for payload in self._state["runs"].values():
            if payload["conversation_id"] != conversation_id:
                continue
            status = FeishuRunStatus(payload["status"])
            if status.is_active:
                return RunRecord(**payload)
        return None

    def get_run(self, run_id: str) -> RunRecord | None:
        payload = self._state["runs"].get(run_id)
        if payload is None:
            return None
        return RunRecord(**payload)

    def update_run(
        self,
        run_id: str,
        *,
        status: FeishuRunStatus,
        current_step: str,
        progress_summary: str = "",
        waiting_reason: str = "",
        checkpoint_version: int | None = None,
    ) -> None:
        with self._lock:
            payload = self._state["runs"][run_id]
            payload["status"] = status.value
            payload["current_step"] = current_step
            payload["progress_summary"] = progress_summary
            payload["waiting_reason"] = waiting_reason
            if checkpoint_version is not None:
                payload["checkpoint_version"] = checkpoint_version
            self._flush()

    def mark_reply_sent(self, reply_key: str, content: str) -> bool:
        with self._lock:
            if reply_key in self._state["replies"]:
                return False
            self._state["replies"][reply_key] = {"content": content}
            self._flush()
            return True
