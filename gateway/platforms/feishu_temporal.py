from __future__ import annotations

import asyncio
import inspect
import json
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable

from gateway.platforms.feishu_bridge_models import FeishuRunStatus

logger = logging.getLogger(__name__)

try:
    from temporalio import workflow
except ImportError:  # pragma: no cover - optional until Temporal is installed
    class _WorkflowShim:
        @staticmethod
        def defn(obj):
            return obj

        @staticmethod
        def run(fn):
            return fn

        @staticmethod
        def signal(fn):
            return fn

        @staticmethod
        def query(fn):
            return fn

        @staticmethod
        async def wait_condition(predicate: Callable[[], bool]) -> None:
            while not predicate():
                await asyncio.sleep(0.01)

    workflow = _WorkflowShim()


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


@dataclass(frozen=True)
class FeishuRunInput:
    run_id: str
    conversation_id: str
    chat_id: str
    thread_id: str | None
    trigger_message_id: str
    normalized_user_text: str
    resume_from_checkpoint: dict = field(default_factory=dict)
    bridge_metadata: dict = field(default_factory=dict)
    max_steps: int = 8


@dataclass(frozen=True)
class HermesStepActivityInput:
    run_input: FeishuRunInput
    checkpoint_payload: dict
    checkpoint_version: int
    user_reply: str | None = None


HermesStepExecutor = Callable[[HermesStepActivityInput], HermesStepResult | Awaitable[HermesStepResult]]
_activity_executor: HermesStepExecutor | None = None


def set_hermes_step_activity_executor(executor: HermesStepExecutor | None) -> None:
    global _activity_executor
    _activity_executor = executor


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


async def _default_run_agent_step(step_input: HermesStepActivityInput) -> HermesStepResult:
    from run_agent import AIAgent

    prior_checkpoint = dict(step_input.checkpoint_payload or {})
    conversation_history = list(prior_checkpoint.get("conversation_history") or [])
    if step_input.user_reply:
        user_message = step_input.user_reply
    elif conversation_history:
        user_message = (
            "Continue the current task from the existing transcript. "
            "If more user input is required, ask one concrete question."
        )
    else:
        user_message = step_input.run_input.normalized_user_text

    agent = AIAgent(max_iterations=1)
    result = await asyncio.to_thread(
        agent.run_conversation,
        user_message,
        conversation_history=conversation_history,
    )

    response = str(result.get("final_response", "") or "")
    summary = response[:200]
    checkpoint_payload = {
        "conversation_history": result.get("messages", []),
        "turn_exit_reason": str(result.get("turn_exit_reason", "") or ""),
        "api_calls": int(result.get("api_calls", 0) or 0),
        "last_response_preview": summary,
    }
    turn_exit_reason = checkpoint_payload["turn_exit_reason"]
    if turn_exit_reason.startswith("max_iterations_reached"):
        return HermesStepResult(
            outcome_type=HermesStepOutcome.CHECKPOINT,
            assistant_text=response,
            progress_summary=summary or "checkpoint saved",
            checkpoint_payload=checkpoint_payload,
            waiting_reason="",
            next_action_hint="continue",
        )
    if result.get("failed"):
        return HermesStepResult(
            outcome_type=HermesStepOutcome.RETRYABLE_ERROR,
            assistant_text=response or "agent step failed",
            progress_summary=summary or "agent step failed",
            checkpoint_payload=checkpoint_payload,
            waiting_reason="",
            next_action_hint="retry",
        )
    return HermesStepResult(
        outcome_type=HermesStepOutcome.COMPLETED,
        assistant_text=response,
        progress_summary=summary,
        checkpoint_payload=checkpoint_payload,
        waiting_reason="",
        next_action_hint="finalize",
    )


async def run_hermes_step_activity(step_input: HermesStepActivityInput) -> HermesStepResult:
    executor = _activity_executor or _default_run_agent_step
    result = await _maybe_await(executor(step_input))
    if not isinstance(result, HermesStepResult):
        raise TypeError("Hermes step executor must return HermesStepResult")
    return result


def _store_from_state_path(state_path: str):
    from gateway.platforms.feishu_bridge_store import FeishuBridgeStore

    return FeishuBridgeStore(Path(state_path))


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


def publish_progress(
    store,
    run_id: str,
    *,
    status,
    current_step: str,
    progress_summary: str,
    waiting_reason: str,
    checkpoint_version: int,
) -> str | None:
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
    store.update_run(
        run_id,
        status=final_status,
        current_step=final_status.value,
        progress_summary=final_text,
    )
    reply_key = f"run:{run_id}:final"
    if store.mark_reply_sent(reply_key, final_text):
        return reply_key
    return None


async def persist_progress_activity(
    state_path: str,
    run_id: str,
    *,
    status: str,
    current_step: str,
    progress_summary: str,
    waiting_reason: str,
    checkpoint_version: int,
) -> str | None:
    store = _store_from_state_path(state_path)
    return publish_progress(
        store,
        run_id,
        status=FeishuRunStatus(status),
        current_step=current_step,
        progress_summary=progress_summary,
        waiting_reason=waiting_reason,
        checkpoint_version=checkpoint_version,
    )


async def finalize_run_activity(
    state_path: str,
    run_id: str,
    *,
    final_status: str,
    final_text: str,
) -> str | None:
    store = _store_from_state_path(state_path)
    return finalize_run(
        store,
        run_id,
        final_status=FeishuRunStatus(final_status),
        final_text=final_text,
    )


@workflow.defn
class FeishuAgentRunWorkflow:
    def __init__(self) -> None:
        self._user_reply: str | None = None
        self._cancel_requested = False
        self._snapshot = {
            "status": "queued",
            "current_step": "queued",
            "progress_summary": "",
            "waiting_reason": "",
            "checkpoint_version": 0,
        }
        self._store = None
        self._step_executor: HermesStepExecutor | None = None

    def bind_store(self, store) -> None:
        self._store = store

    def bind_step_executor(self, executor: HermesStepExecutor) -> None:
        self._step_executor = executor

    @workflow.signal
    def user_reply(self, text: str) -> None:
        self._user_reply = text

    @workflow.signal
    def cancel_run(self) -> None:
        self._cancel_requested = True

    @workflow.query
    def snapshot(self) -> dict:
        return dict(self._snapshot)

    async def _execute_step(self, step_input: HermesStepActivityInput) -> HermesStepResult:
        if self._step_executor is not None:
            return await _maybe_await(self._step_executor(step_input))

        execute_activity = getattr(workflow, "execute_activity", None)
        if execute_activity is not None:
            return await execute_activity(
                run_hermes_step_activity,
                step_input,
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(seconds=30),
            )
        return await run_hermes_step_activity(step_input)

    def _consume_user_reply(self) -> str | None:
        reply = self._user_reply
        self._user_reply = None
        return reply

    def _update_snapshot(
        self,
        *,
        status: str,
        current_step: str,
        progress_summary: str,
        waiting_reason: str,
        checkpoint_version: int,
    ) -> None:
        self._snapshot.update(
            {
                "status": status,
                "current_step": current_step,
                "progress_summary": progress_summary,
                "waiting_reason": waiting_reason,
                "checkpoint_version": checkpoint_version,
            }
        )

    async def _persist_progress(
        self,
        run_input: FeishuRunInput,
        *,
        status: str,
        current_step: str,
        progress_summary: str,
        waiting_reason: str,
        checkpoint_version: int,
    ) -> None:
        if self._store is not None:
            publish_progress(
                self._store,
                run_input.run_id,
                status=FeishuRunStatus(status),
                current_step=current_step,
                progress_summary=progress_summary,
                waiting_reason=waiting_reason,
                checkpoint_version=checkpoint_version,
            )
            return

        state_path = str(run_input.bridge_metadata.get("state_path", "") or "")
        if not state_path:
            return
        execute_activity = getattr(workflow, "execute_activity", None)
        if execute_activity is not None:
            await execute_activity(
                persist_progress_activity,
                state_path,
                run_input.run_id,
                status=status,
                current_step=current_step,
                progress_summary=progress_summary,
                waiting_reason=waiting_reason,
                checkpoint_version=checkpoint_version,
                start_to_close_timeout=timedelta(minutes=2),
            )
            return
        await persist_progress_activity(
            state_path,
            run_input.run_id,
            status=status,
            current_step=current_step,
            progress_summary=progress_summary,
            waiting_reason=waiting_reason,
            checkpoint_version=checkpoint_version,
        )

    async def _persist_final(self, run_input: FeishuRunInput, *, final_status: str, final_text: str) -> None:
        if self._store is not None:
            finalize_run(
                self._store,
                run_input.run_id,
                final_status=FeishuRunStatus(final_status),
                final_text=final_text,
            )
            return

        state_path = str(run_input.bridge_metadata.get("state_path", "") or "")
        if not state_path:
            return
        execute_activity = getattr(workflow, "execute_activity", None)
        if execute_activity is not None:
            await execute_activity(
                finalize_run_activity,
                state_path,
                run_input.run_id,
                final_status=final_status,
                final_text=final_text,
                start_to_close_timeout=timedelta(minutes=2),
            )
            return
        await finalize_run_activity(
            state_path,
            run_input.run_id,
            final_status=final_status,
            final_text=final_text,
        )

    @workflow.run
    async def run(self, run_input: FeishuRunInput) -> dict:
        checkpoint_payload = dict(run_input.resume_from_checkpoint or {})
        checkpoint_version = int(checkpoint_payload.get("checkpoint_version", 0))
        self._update_snapshot(
            status=FeishuRunStatus.RUNNING.value,
            current_step="starting",
            progress_summary="",
            waiting_reason="",
            checkpoint_version=checkpoint_version,
        )
        if self._store is not None:
            self._store.update_run(
                run_input.run_id,
                status=FeishuRunStatus.RUNNING,
                current_step="starting",
                progress_summary="",
                waiting_reason="",
                checkpoint_version=checkpoint_version,
            )

        steps_taken = 0
        while steps_taken < run_input.max_steps:
            if self._cancel_requested:
                self._update_snapshot(
                    status=FeishuRunStatus.CANCELLED.value,
                    current_step="cancelled",
                    progress_summary=self._snapshot.get("progress_summary", ""),
                    waiting_reason="",
                    checkpoint_version=checkpoint_version,
                )
                await self._persist_final(
                    run_input,
                    final_status=FeishuRunStatus.CANCELLED.value,
                    final_text=self._snapshot.get("progress_summary", "") or "cancelled",
                )
                return self.snapshot()

            step_input = HermesStepActivityInput(
                run_input=run_input,
                checkpoint_payload=checkpoint_payload,
                checkpoint_version=checkpoint_version,
                user_reply=self._consume_user_reply(),
            )
            result = await self._execute_step(step_input)
            reduced = reduce_step_result(result)
            steps_taken += 1
            checkpoint_version += 1
            checkpoint_payload = dict(result.checkpoint_payload or {})
            checkpoint_payload["checkpoint_version"] = checkpoint_version

            self._update_snapshot(
                status=reduced["status"],
                current_step=result.next_action_hint or reduced["status"],
                progress_summary=result.progress_summary,
                waiting_reason=result.waiting_reason,
                checkpoint_version=checkpoint_version,
            )

            if reduced["terminal"]:
                await self._persist_final(
                    run_input,
                    final_status=reduced["status"],
                    final_text=result.assistant_text or result.progress_summary,
                )
                return self.snapshot()

            await self._persist_progress(
                run_input,
                status=reduced["status"],
                current_step=result.next_action_hint or reduced["status"],
                progress_summary=result.progress_summary,
                waiting_reason=result.waiting_reason,
                checkpoint_version=checkpoint_version,
            )

            if reduced["status"] == FeishuRunStatus.WAITING_USER.value:
                wait_condition = getattr(workflow, "wait_condition", None)
                if wait_condition is None:
                    return self.snapshot()
                await wait_condition(lambda: self._user_reply is not None or self._cancel_requested)
                continue

        self._update_snapshot(
            status=FeishuRunStatus.FAILED.value,
            current_step="max_steps_exceeded",
            progress_summary="max steps exceeded",
            waiting_reason="",
            checkpoint_version=checkpoint_version,
        )
        await self._persist_final(
            run_input,
            final_status=FeishuRunStatus.FAILED.value,
            final_text="max steps exceeded",
        )
        return self.snapshot()
