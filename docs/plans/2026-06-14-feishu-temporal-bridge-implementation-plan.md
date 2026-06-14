# Feishu Temporal Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable Feishu/Lark long-task control plane on top of Hermes so duplicate inbound events do not cause duplicate replies and long-running tasks survive worker restarts via Temporal.

**Architecture:** Keep `gateway/platforms/feishu.py` as the existing transport adapter, but stop letting it directly decide whether every inbound message should run the agent. Add a thin Feishu bridge layer that classifies messages, persists run state, and starts or signals a Temporal workflow; the Temporal worker then invokes Hermes in resumable segments and publishes deduplicated progress/final replies back through the adapter.

**Tech Stack:** Python, `gateway/platforms/feishu.py`, `gateway/session.py`, `gateway/run.py`, `temporalio`, pytest, unittest mocks, Hermes gateway test helpers

---

## File Structure

### New files

- `gateway/platforms/feishu_bridge_models.py`
  Purpose: dataclasses and enums for inbound message records, run status, status-query snapshots, reply keys, and workflow payloads.
- `gateway/platforms/feishu_bridge_store.py`
  Purpose: persistence wrapper for inbound dedupe, active-run lookup, outbound reply idempotency, and run event recording.
- `gateway/platforms/feishu_bridge_router.py`
  Purpose: classify Feishu messages into `new_task`, `status_query`, `user_reply`, or `cancel`, then route to store + Temporal APIs.
- `gateway/platforms/feishu_temporal.py`
  Purpose: Temporal workflow and activity entrypoints for `FeishuAgentRunWorkflow`.
- `gateway/platforms/feishu_temporal_client.py`
  Purpose: thin client wrapper used by the router to start workflows, signal waiting runs, cancel runs, and query snapshots.
- `tests/gateway/test_feishu_bridge_models.py`
  Purpose: unit tests for run state transitions, reply-key generation, and route classification helpers.
- `tests/gateway/test_feishu_bridge_store.py`
  Purpose: unit tests for inbound idempotency, active-run locking, and outbound dedupe.
- `tests/gateway/test_feishu_bridge_router.py`
  Purpose: unit tests for status query routing, waiting-user resume, and cancel behavior.
- `tests/gateway/test_feishu_temporal_workflow.py`
  Purpose: unit tests for workflow loop behavior, resume signals, retry outcomes, and finalization.

### Modified files

- `gateway/platforms/feishu.py`
  Purpose: replace direct inline long-task dispatch in inbound event handling with bridge router calls while preserving current webhook/websocket parsing, mention gating, dedupe cache, and reaction UX.
- `gateway/config.py`
  Purpose: add Temporal/bridge configuration loading for Feishu bridge mode without introducing ad hoc env-var sprawl beyond required credentials/connection settings.
- `pyproject.toml`
  Purpose: add the `temporalio` dependency and any optional extras grouping used for gateway platform dependencies.
- `tests/gateway/test_feishu.py`
  Purpose: extend current Feishu adapter coverage to assert bridge handoff behavior at the adapter boundary.
- `cli-config.yaml.example`
  Purpose: document the bridge/Temporal config block in the canonical configuration example.
- `README.md` or `README.zh-CN.md`
  Purpose: add a short operator-facing section for running the Feishu bridge worker and Temporal worker.

## Task 1: Introduce the bridge domain model and classifier

**Files:**
- Create: `gateway/platforms/feishu_bridge_models.py`
- Test: `tests/gateway/test_feishu_bridge_models.py`

- [ ] **Step 1: Write the failing tests for run states, reply keys, and route classification**

```python
from gateway.platforms.feishu_bridge_models import (
    FeishuInboundKind,
    FeishuRunStatus,
    FeishuRouteDecision,
    classify_feishu_text,
    make_reply_key,
)


def test_status_queries_map_to_status_kind():
    decision = classify_feishu_text("干完了吗")
    assert decision.kind is FeishuInboundKind.STATUS_QUERY
    assert decision.normalized_text == "干完了吗"


def test_cancel_queries_map_to_cancel_kind():
    decision = classify_feishu_text("取消")
    assert decision.kind is FeishuInboundKind.CANCEL


def test_plain_task_defaults_to_new_task():
    decision = classify_feishu_text("帮我排查这个报错")
    assert decision.kind is FeishuInboundKind.NEW_TASK


def test_reply_key_uses_run_and_checkpoint():
    assert make_reply_key("run-1", "progress", checkpoint_version=3) == "run:run-1:progress:3"


def test_active_statuses_exclude_terminal_states():
    assert FeishuRunStatus.RUNNING.is_active is True
    assert FeishuRunStatus.COMPLETED.is_active is False
```

- [ ] **Step 2: Run the tests to verify the symbols do not exist yet**

Run: `pytest tests/gateway/test_feishu_bridge_models.py -q`

Expected: `ImportError` or `AttributeError` for `FeishuInboundKind`, `classify_feishu_text`, or `make_reply_key`

- [ ] **Step 3: Write the minimal bridge models and classifier**

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


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


_STATUS_QUERIES = {"干完了吗", "到哪一步了", "现在到哪一步了", "什么情况", "进度"}
_CANCEL_QUERIES = {"取消", "停止", "别跑了", "算了"}


def classify_feishu_text(text: str) -> FeishuRouteDecision:
    normalized = (text or "").strip()
    if normalized in _STATUS_QUERIES:
        return FeishuRouteDecision(FeishuInboundKind.STATUS_QUERY, normalized)
    if normalized in _CANCEL_QUERIES:
        return FeishuRouteDecision(FeishuInboundKind.CANCEL, normalized)
    return FeishuRouteDecision(FeishuInboundKind.NEW_TASK, normalized)


def make_reply_key(run_id: str, reply_type: str, *, checkpoint_version: Optional[int] = None) -> str:
    if checkpoint_version is not None:
        return f"run:{run_id}:{reply_type}:{checkpoint_version}"
    return f"run:{run_id}:{reply_type}"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/gateway/test_feishu_bridge_models.py -q`

Expected: `5 passed`

- [ ] **Step 5: Commit the isolated domain-model slice**

```bash
git add gateway/platforms/feishu_bridge_models.py tests/gateway/test_feishu_bridge_models.py
git commit -m "feat: add feishu bridge domain models"
```

## Task 2: Add run-store persistence and dedupe semantics

**Files:**
- Create: `gateway/platforms/feishu_bridge_store.py`
- Test: `tests/gateway/test_feishu_bridge_store.py`

- [ ] **Step 1: Write the failing tests for inbound dedupe, single active run, and outbound idempotency**

```python
from gateway.platforms.feishu_bridge_models import FeishuRunStatus
from gateway.platforms.feishu_bridge_store import FeishuBridgeStore


def test_duplicate_message_id_is_rejected(tmp_path):
    store = FeishuBridgeStore(tmp_path / "feishu_bridge_state.json")
    first = store.record_inbound(
        platform="feishu",
        tenant_id="tenant-1",
        chat_id="oc_123",
        thread_id=None,
        message_id="om_1",
        event_id="evt_1",
        normalized_text="干完了吗",
    )
    second = store.record_inbound(
        platform="feishu",
        tenant_id="tenant-1",
        chat_id="oc_123",
        thread_id=None,
        message_id="om_1",
        event_id="evt_1",
        normalized_text="干完了吗",
    )
    assert first.accepted is True
    assert second.accepted is False


def test_only_one_active_run_per_conversation(tmp_path):
    store = FeishuBridgeStore(tmp_path / "feishu_bridge_state.json")
    run = store.create_run("conv-1", "chat-1", None, "om_1")
    store.update_run(run.run_id, status=FeishuRunStatus.RUNNING, current_step="boot")
    assert store.get_active_run("conv-1").run_id == run.run_id
    try:
        store.create_run("conv-1", "chat-1", None, "om_2")
    except RuntimeError as exc:
        assert "active run" in str(exc)
    else:
        raise AssertionError("expected active-run guard")


def test_reply_key_is_sent_once(tmp_path):
    store = FeishuBridgeStore(tmp_path / "feishu_bridge_state.json")
    assert store.mark_reply_sent("run:1:start", "hello") is True
    assert store.mark_reply_sent("run:1:start", "hello") is False
```

- [ ] **Step 2: Run the tests to verify the store is missing**

Run: `pytest tests/gateway/test_feishu_bridge_store.py -q`

Expected: `ImportError: cannot import name 'FeishuBridgeStore'`

- [ ] **Step 3: Implement a minimal JSON-backed bridge store with exact active-run checks**

```python
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Lock
from typing import Optional

from gateway.platforms.feishu_bridge_models import FeishuRunStatus
from utils import atomic_json_write


@dataclass
class InboundRecordResult:
    accepted: bool
    dedupe_key: str


@dataclass
class RunRecord:
    run_id: str
    conversation_id: str
    chat_id: str
    thread_id: Optional[str]
    trigger_message_id: str
    status: str
    current_step: str


class FeishuBridgeStore:
    def __init__(self, state_path: Path) -> None:
        self._state_path = Path(state_path)
        self._lock = Lock()
        self._state = {"inbound": {}, "runs": {}, "replies": {}}
        if self._state_path.exists():
            self._state = json.loads(self._state_path.read_text(encoding="utf-8"))

    def _flush(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json_write(self._state_path, self._state, indent=2)

    def record_inbound(self, *, platform: str, tenant_id: str, chat_id: str, thread_id: Optional[str], message_id: str, event_id: str, normalized_text: str) -> InboundRecordResult:
        dedupe_key = f"{platform}:{tenant_id}:{message_id or event_id}"
        with self._lock:
            if dedupe_key in self._state["inbound"]:
                return InboundRecordResult(False, dedupe_key)
            self._state["inbound"][dedupe_key] = {
                "chat_id": chat_id,
                "thread_id": thread_id,
                "normalized_text": normalized_text,
            }
            self._flush()
            return InboundRecordResult(True, dedupe_key)

    def create_run(self, conversation_id: str, chat_id: str, thread_id: Optional[str], trigger_message_id: str) -> RunRecord:
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
            )
            self._state["runs"][run.run_id] = asdict(run)
            self._flush()
            return run

    def get_active_run(self, conversation_id: str) -> Optional[RunRecord]:
        for payload in self._state["runs"].values():
            if payload["conversation_id"] != conversation_id:
                continue
            status = FeishuRunStatus(payload["status"])
            if status.is_active:
                return RunRecord(**payload)
        return None

    def update_run(self, run_id: str, *, status: FeishuRunStatus, current_step: str) -> None:
        with self._lock:
            payload = self._state["runs"][run_id]
            payload["status"] = status.value
            payload["current_step"] = current_step
            self._flush()

    def mark_reply_sent(self, reply_key: str, content: str) -> bool:
        with self._lock:
            if reply_key in self._state["replies"]:
                return False
            self._state["replies"][reply_key] = {"content": content}
            self._flush()
            return True
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: `pytest tests/gateway/test_feishu_bridge_store.py -q`

Expected: `3 passed`

- [ ] **Step 5: Commit the persistence slice**

```bash
git add gateway/platforms/feishu_bridge_store.py tests/gateway/test_feishu_bridge_store.py
git commit -m "feat: add feishu bridge run store"
```

## Task 3: Add the bridge router and make status queries read-only

**Files:**
- Create: `gateway/platforms/feishu_bridge_router.py`
- Modify: `gateway/platforms/feishu.py`
- Test: `tests/gateway/test_feishu_bridge_router.py`
- Test: `tests/gateway/test_feishu.py`

- [ ] **Step 1: Write the failing router tests for status queries, waiting-user replies, and cancel signals**

```python
from types import SimpleNamespace
from unittest.mock import MagicMock

from gateway.platforms.feishu_bridge_models import FeishuRunStatus
from gateway.platforms.feishu_bridge_router import FeishuBridgeRouter
from gateway.platforms.feishu_bridge_store import FeishuBridgeStore


def test_status_query_reads_active_run_without_starting_new_workflow(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")
    store.update_run(run.run_id, status=FeishuRunStatus.RUNNING, current_step="installing")
    temporal = MagicMock()
    router = FeishuBridgeRouter(store=store, temporal_client=temporal)

    result = router.route_message(
        conversation_id="conv-1",
        chat_id="oc_1",
        thread_id=None,
        message_id="om_status",
        event_id="evt_status",
        text="干完了吗",
    )

    assert result.action == "status_reply"
    temporal.start_run.assert_not_called()


def test_waiting_user_reply_signals_existing_run(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")
    store.update_run(run.run_id, status=FeishuRunStatus.WAITING_USER, current_step="need path")
    temporal = MagicMock()
    router = FeishuBridgeRouter(store=store, temporal_client=temporal)

    result = router.route_message(
        conversation_id="conv-1",
        chat_id="oc_1",
        thread_id=None,
        message_id="om_reply",
        event_id="evt_reply",
        text="/Users/xiaofu/Documents/comfy",
    )

    assert result.action == "signal_waiting_run"
    temporal.signal_user_reply.assert_called_once()
```

- [ ] **Step 2: Run the router tests and confirm the module is missing**

Run: `pytest tests/gateway/test_feishu_bridge_router.py -q`

Expected: `ImportError` for `FeishuBridgeRouter`

- [ ] **Step 3: Implement the router with explicit read-only status routing**

```python
from __future__ import annotations

from dataclasses import dataclass

from gateway.platforms.feishu_bridge_models import FeishuInboundKind, classify_feishu_text


@dataclass(frozen=True)
class RouteResult:
    action: str
    run_id: str | None = None


class FeishuBridgeRouter:
    def __init__(self, *, store, temporal_client) -> None:
        self.store = store
        self.temporal_client = temporal_client

    def route_message(self, *, conversation_id: str, chat_id: str, thread_id: str | None, message_id: str, event_id: str, text: str) -> RouteResult:
        self.store.record_inbound(
            platform="feishu",
            tenant_id="default",
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=message_id,
            event_id=event_id,
            normalized_text=text.strip(),
        )
        decision = classify_feishu_text(text)
        active = self.store.get_active_run(conversation_id)

        if decision.kind is FeishuInboundKind.STATUS_QUERY and active is not None:
            return RouteResult(action="status_reply", run_id=active.run_id)

        if decision.kind is FeishuInboundKind.CANCEL and active is not None:
            self.temporal_client.cancel_run(active.run_id)
            return RouteResult(action="cancel_run", run_id=active.run_id)

        if active is not None and active.status == "waiting_user":
            self.temporal_client.signal_user_reply(active.run_id, text.strip())
            return RouteResult(action="signal_waiting_run", run_id=active.run_id)

        if active is None:
            run = self.store.create_run(conversation_id, chat_id, thread_id, message_id)
            self.temporal_client.start_run(run.run_id, conversation_id=conversation_id, text=text.strip())
            return RouteResult(action="start_run", run_id=run.run_id)

        return RouteResult(action="busy_status", run_id=active.run_id)
```

- [ ] **Step 4: Patch the Feishu adapter boundary to call the router instead of always dispatching the agent**

```python
# inside gateway/platforms/feishu.py

from gateway.platforms.feishu_bridge_router import FeishuBridgeRouter
from gateway.platforms.feishu_temporal_client import FeishuTemporalClient
from gateway.platforms.feishu_bridge_store import FeishuBridgeStore


def _build_bridge_router(self) -> FeishuBridgeRouter:
    if getattr(self, "_bridge_router", None) is None:
        state_path = get_hermes_home() / "feishu_bridge_state.json"
        self._bridge_router = FeishuBridgeRouter(
            store=FeishuBridgeStore(state_path),
            temporal_client=FeishuTemporalClient.from_config(self.config),
        )
    return self._bridge_router


async def _process_inbound_message(...):
    router = self._build_bridge_router()
    route = router.route_message(
        conversation_id=build_session_key(event.source),
        chat_id=event.source.chat_id,
        thread_id=event.source.thread_id,
        message_id=event.message_id or "",
        event_id=event.metadata.get("event_id", "") if getattr(event, "metadata", None) else "",
        text=event.text or "",
    )
    if route.action == "status_reply":
        await self.send(event.source.chat_id, self._format_bridge_status(route.run_id), thread_id=event.source.thread_id)
        return
    if route.action in {"cancel_run", "signal_waiting_run", "start_run", "busy_status"}:
        return
```

- [ ] **Step 5: Run router and Feishu adapter tests**

Run: `pytest tests/gateway/test_feishu_bridge_router.py tests/gateway/test_feishu.py -q`

Expected: targeted router tests pass, and existing Feishu tests still pass after adapter handoff wiring

- [ ] **Step 6: Commit the routing slice**

```bash
git add gateway/platforms/feishu_bridge_router.py gateway/platforms/feishu.py tests/gateway/test_feishu_bridge_router.py tests/gateway/test_feishu.py
git commit -m "feat: route feishu inbound messages through bridge"
```

## Task 4: Add Temporal client, workflow, and resumable Hermes activity contracts

**Files:**
- Create: `gateway/platforms/feishu_temporal.py`
- Create: `gateway/platforms/feishu_temporal_client.py`
- Modify: `pyproject.toml`
- Test: `tests/gateway/test_feishu_temporal_workflow.py`

- [ ] **Step 1: Write the failing workflow tests for start, waiting-user resume, and finalization**

```python
from unittest.mock import MagicMock

from gateway.platforms.feishu_temporal import (
    HermesStepOutcome,
    HermesStepResult,
    reduce_step_result,
)


def test_checkpoint_outcome_keeps_run_active():
    result = HermesStepResult(
        outcome_type=HermesStepOutcome.CHECKPOINT,
        assistant_text="working",
        progress_summary="installing deps",
        checkpoint_payload={"step": "deps"},
        waiting_reason="",
        next_action_hint="continue",
    )
    reduced = reduce_step_result(result)
    assert reduced["terminal"] is False
    assert reduced["status"] == "running"


def test_waiting_user_outcome_transitions_to_waiting():
    result = HermesStepResult(
        outcome_type=HermesStepOutcome.WAITING_USER,
        assistant_text="need path",
        progress_summary="blocked",
        checkpoint_payload={"step": "need_path"},
        waiting_reason="path required",
        next_action_hint="await signal",
    )
    reduced = reduce_step_result(result)
    assert reduced["status"] == "waiting_user"
    assert reduced["terminal"] is False
```

- [ ] **Step 2: Run the workflow test file and verify the Temporal module is absent**

Run: `pytest tests/gateway/test_feishu_temporal_workflow.py -q`

Expected: `ImportError` for `gateway.platforms.feishu_temporal`

- [ ] **Step 3: Add the Temporal dependency and minimal workflow/result types**

```toml
# pyproject.toml
[project.optional-dependencies]
feishu = [
  "lark-oapi>=1.4.0",
  "aiohttp>=3.9.0",
  "websockets>=12.0",
  "temporalio>=1.7.0",
]
```

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class HermesStepOutcome(str, Enum):
    COMPLETED = "completed"
    WAITING_USER = "waiting_user"
    CHECKPOINT = "checkpoint"
    RETRYABLE_ERROR = "retryable_error"
    FATAL_ERROR = "fatal_error"


@dataclass(frozen=True)
class HermesStepResult:
    outcome_type: HermesStepOutcome
    assistant_text: str
    progress_summary: str
    checkpoint_payload: dict
    waiting_reason: str
    next_action_hint: str


def reduce_step_result(result: HermesStepResult) -> dict:
    if result.outcome_type is HermesStepOutcome.COMPLETED:
        return {"status": "completed", "terminal": True}
    if result.outcome_type is HermesStepOutcome.WAITING_USER:
        return {"status": "waiting_user", "terminal": False}
    if result.outcome_type is HermesStepOutcome.CHECKPOINT:
        return {"status": "running", "terminal": False}
    if result.outcome_type is HermesStepOutcome.RETRYABLE_ERROR:
        return {"status": "retrying", "terminal": False}
    return {"status": "failed", "terminal": True}
```

- [ ] **Step 4: Add the Temporal client wrapper and workflow skeleton**

```python
# gateway/platforms/feishu_temporal_client.py

class FeishuTemporalClient:
    def __init__(self, temporal_client) -> None:
        self._client = temporal_client

    @classmethod
    def from_config(cls, gateway_config):
        return cls(temporal_client=None)

    def start_run(self, run_id: str, *, conversation_id: str, text: str) -> None:
        if self._client is None:
            return

    def signal_user_reply(self, run_id: str, text: str) -> None:
        if self._client is None:
            return

    def cancel_run(self, run_id: str) -> None:
        if self._client is None:
            return
```

```python
# gateway/platforms/feishu_temporal.py

from temporalio import workflow


@workflow.defn
class FeishuAgentRunWorkflow:
    def __init__(self) -> None:
        self._user_reply = None
        self._cancel_requested = False
        self._snapshot = {
            "status": "queued",
            "current_step": "queued",
            "progress_summary": "",
            "waiting_reason": "",
        }

    @workflow.signal
    def user_reply(self, text: str) -> None:
        self._user_reply = text

    @workflow.signal
    def cancel_run(self) -> None:
        self._cancel_requested = True

    @workflow.query
    def snapshot(self) -> dict:
        return self._snapshot
```

- [ ] **Step 5: Run the workflow tests**

Run: `pytest tests/gateway/test_feishu_temporal_workflow.py -q`

Expected: the reducer and workflow skeleton tests pass before any full worker integration

- [ ] **Step 6: Commit the Temporal foundation**

```bash
git add pyproject.toml gateway/platforms/feishu_temporal.py gateway/platforms/feishu_temporal_client.py tests/gateway/test_feishu_temporal_workflow.py
git commit -m "feat: add temporal foundation for feishu bridge"
```

## Task 5: Connect the workflow to run-store updates and outbound reply idempotency

**Files:**
- Modify: `gateway/platforms/feishu_bridge_store.py`
- Modify: `gateway/platforms/feishu_temporal.py`
- Modify: `gateway/platforms/feishu.py`
- Test: `tests/gateway/test_feishu_temporal_workflow.py`
- Test: `tests/gateway/test_feishu_bridge_store.py`

- [ ] **Step 1: Write the failing tests for progress dedupe and final reply idempotency**

```python
from gateway.platforms.feishu_bridge_store import FeishuBridgeStore


def test_progress_reply_key_is_unique_per_checkpoint(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    assert store.mark_reply_sent("run:abc:progress:1", "step 1") is True
    assert store.mark_reply_sent("run:abc:progress:1", "step 1") is False
    assert store.mark_reply_sent("run:abc:progress:2", "step 2") is True
```

- [ ] **Step 2: Run the store and workflow tests**

Run: `pytest tests/gateway/test_feishu_bridge_store.py tests/gateway/test_feishu_temporal_workflow.py -q`

Expected: new assertions fail because checkpoint-aware progress persistence is not wired

- [ ] **Step 3: Extend the store to persist checkpoint version, waiting reason, and progress summary**

```python
def update_run(self, run_id: str, *, status: FeishuRunStatus, current_step: str, progress_summary: str = "", waiting_reason: str = "", checkpoint_version: int | None = None) -> None:
    with self._lock:
        payload = self._state["runs"][run_id]
        payload["status"] = status.value
        payload["current_step"] = current_step
        payload["progress_summary"] = progress_summary
        payload["waiting_reason"] = waiting_reason
        if checkpoint_version is not None:
            payload["checkpoint_version"] = checkpoint_version
        self._flush()
```

- [ ] **Step 4: Add publish/finalize hooks in the workflow implementation**

```python
def publish_progress(store, run_id: str, *, status, current_step: str, progress_summary: str, waiting_reason: str, checkpoint_version: int) -> str | None:
    store.update_run(
        run_id,
        status=status,
        current_step=current_step,
        progress_summary=progress_summary,
        waiting_reason=waiting_reason,
        checkpoint_version=checkpoint_version,
    )
    reply_key = f"run:{run_id}:progress:{checkpoint_version}"
    if store.mark_reply_sent(reply_key, progress_summary):
        return reply_key
    return None


def finalize_run(store, run_id: str, *, final_status, final_text: str) -> str | None:
    store.update_run(run_id, status=final_status, current_step=final_status.value, progress_summary=final_text)
    reply_key = f"run:{run_id}:final"
    if store.mark_reply_sent(reply_key, final_text):
        return reply_key
    return None
```

- [ ] **Step 5: Preserve the adapter's current send path and only add reply-key guards around bridge-originated replies**

```python
async def _send_bridge_reply(self, chat_id: str, text: str, *, thread_id: str | None, reply_key: str) -> None:
    store = self._build_bridge_router().store
    if not store.mark_reply_sent(reply_key, text):
        return
    await self.send(chat_id, text, thread_id=thread_id)
```

- [ ] **Step 6: Run the targeted tests**

Run: `pytest tests/gateway/test_feishu_bridge_store.py tests/gateway/test_feishu_temporal_workflow.py tests/gateway/test_feishu.py -q`

Expected: progress and final replies are deduplicated while normal Feishu adapter behavior remains intact

- [ ] **Step 7: Commit the progress/final reply integration**

```bash
git add gateway/platforms/feishu_bridge_store.py gateway/platforms/feishu_temporal.py gateway/platforms/feishu.py tests/gateway/test_feishu_bridge_store.py tests/gateway/test_feishu_temporal_workflow.py tests/gateway/test_feishu.py
git commit -m "feat: add bridge progress and final reply idempotency"
```

## Task 6: Add operator config, worker startup docs, and end-to-end regression coverage

**Files:**
- Modify: `gateway/config.py`
- Modify: `cli-config.yaml.example`
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Test: `tests/gateway/test_feishu.py`
- Test: `tests/gateway/test_project_metadata.py`

- [ ] **Step 1: Write the failing config-loading tests for bridge mode**

```python
from gateway.config import load_gateway_config, Platform


def test_feishu_bridge_mode_loads_temporal_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("FEISHU_APP_ID", "cli_app")
    monkeypatch.setenv("FEISHU_APP_SECRET", "cli_secret")
    monkeypatch.setenv("FEISHU_CONNECTION_MODE", "webhook")
    monkeypatch.setenv("FEISHU_BRIDGE_MODE", "temporal")
    monkeypatch.setenv("TEMPORAL_TARGET_HOST", "127.0.0.1:7233")

    config = load_gateway_config()
    assert config.platforms[Platform.FEISHU].extra["bridge_mode"] == "temporal"
    assert config.platforms[Platform.FEISHU].extra["temporal_target_host"] == "127.0.0.1:7233"
```

- [ ] **Step 2: Run the config tests**

Run: `pytest tests/gateway/test_feishu.py -q`

Expected: failing assertion because bridge mode settings are not loaded yet

- [ ] **Step 3: Load bridge/Temporal settings through existing config machinery**

```python
# gateway/config.py inside Feishu env loading
config.platforms[Platform.FEISHU].extra["bridge_mode"] = os.getenv("FEISHU_BRIDGE_MODE", "").strip().lower()
temporal_target = os.getenv("TEMPORAL_TARGET_HOST", "").strip()
if temporal_target:
    config.platforms[Platform.FEISHU].extra["temporal_target_host"] = temporal_target
temporal_namespace = os.getenv("TEMPORAL_NAMESPACE", "").strip()
if temporal_namespace:
    config.platforms[Platform.FEISHU].extra["temporal_namespace"] = temporal_namespace
```

- [ ] **Step 4: Document the operator workflow in the example config and README**

```yaml
# cli-config.yaml.example
feishu:
  enabled: true
  connection_mode: webhook
  bridge_mode: temporal
  temporal_target_host: 127.0.0.1:7233
  temporal_namespace: default
```

```md
## Feishu durable bridge

When `feishu.bridge_mode` is set to `temporal`, inbound Feishu messages are routed through the bridge router instead of directly launching a long-running agent turn. Run a Temporal server and a Hermes Temporal worker before enabling this mode in production.
```

- [ ] **Step 5: Run the final focused verification suite**

Run: `pytest tests/gateway/test_feishu_bridge_models.py tests/gateway/test_feishu_bridge_store.py tests/gateway/test_feishu_bridge_router.py tests/gateway/test_feishu_temporal_workflow.py tests/gateway/test_feishu.py tests/test_project_metadata.py -q`

Expected: all targeted bridge and Feishu tests pass

- [ ] **Step 6: Commit the documentation and config slice**

```bash
git add gateway/config.py cli-config.yaml.example README.md README.zh-CN.md tests/gateway/test_feishu.py tests/test_project_metadata.py
git commit -m "docs: add feishu temporal bridge setup guidance"
```

## Scope Notes

- This plan intentionally reuses the existing Feishu adapter instead of adding a brand-new platform implementation.
- The first implementation pass uses a simple JSON-backed store to get the router, run lifecycle, and idempotency behavior under test before any database swap.
- The Temporal client wrapper is deliberately thin at first so the bridge routing logic can be landed and tested before full worker deployment hardening.

## Verification Checklist

- `干完了吗` during an active run returns a status snapshot and does not start a new agent run.
- Duplicate inbound Feishu message IDs do not create duplicate runs.
- A waiting run resumes through a signal path rather than a new run path.
- Cancel requests target the active run instead of creating a new one.
- Progress and final replies are deduplicated via reply keys.
- Existing Feishu adapter webhook/websocket tests continue to pass after bridge handoff wiring.

## Self-Review

- Spec coverage:
  - Architecture split is covered by Tasks 1 through 4.
  - Message classification is covered by Tasks 1 and 3.
  - Persistence and idempotency are covered by Tasks 2 and 5.
  - Temporal workflow and signals are covered by Task 4.
  - Operator config and rollout guidance are covered by Task 6.
- Placeholder scan:
  - No `TODO`, `TBD`, or deferred implementation placeholders remain inside executable steps.
- Type consistency:
  - `FeishuInboundKind`, `FeishuRunStatus`, `FeishuBridgeStore`, `FeishuBridgeRouter`, `FeishuTemporalClient`, and `FeishuAgentRunWorkflow` use the same names across all tasks.

## Execution Handoff

Plan complete and saved to `docs/plans/2026-06-14-feishu-temporal-bridge-implementation-plan.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
