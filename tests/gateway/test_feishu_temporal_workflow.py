import asyncio

import pytest

from gateway.platforms.feishu_temporal import (
    FeishuAgentRunWorkflow,
    FeishuRunInput,
    HermesStepOutcome,
    HermesStepResult,
    finalize_run,
    publish_progress,
    reduce_step_result,
)
from gateway.platforms.feishu_bridge_models import FeishuRunStatus
from gateway.platforms.feishu_bridge_store import FeishuBridgeStore


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


def test_completed_outcome_is_terminal():
    result = HermesStepResult(
        outcome_type=HermesStepOutcome.COMPLETED,
        assistant_text="done",
        progress_summary="completed",
        checkpoint_payload={},
        waiting_reason="",
        next_action_hint="finalize",
    )
    reduced = reduce_step_result(result)
    assert reduced["status"] == "completed"
    assert reduced["terminal"] is True


def test_publish_progress_updates_run_snapshot_and_reply_key(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")

    reply_key = publish_progress(
        store,
        run.run_id,
        status=FeishuRunStatus.RUNNING,
        current_step="installing",
        progress_summary="deps",
        waiting_reason="",
        checkpoint_version=1,
    )

    assert reply_key == f"run:{run.run_id}:progress:1"
    saved = store.get_run(run.run_id)
    assert saved is not None
    assert saved.status == FeishuRunStatus.RUNNING.value
    assert saved.current_step == "installing"


def test_finalize_run_marks_terminal_and_returns_reply_key(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")

    reply_key = finalize_run(
        store,
        run.run_id,
        final_status=FeishuRunStatus.COMPLETED,
        final_text="done",
    )

    assert reply_key == f"run:{run.run_id}:final"
    saved = store.get_run(run.run_id)
    assert saved is not None
    assert saved.status == FeishuRunStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_workflow_run_completes_after_checkpoint_then_completion(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")
    workflow = FeishuAgentRunWorkflow()
    workflow.bind_store(store)

    results = iter([
        HermesStepResult(
            outcome_type=HermesStepOutcome.CHECKPOINT,
            assistant_text="working",
            progress_summary="checkpoint 1",
            checkpoint_payload={"stage": "checkpoint"},
            waiting_reason="",
            next_action_hint="continue",
        ),
        HermesStepResult(
            outcome_type=HermesStepOutcome.COMPLETED,
            assistant_text="done",
            progress_summary="done",
            checkpoint_payload={"stage": "complete"},
            waiting_reason="",
            next_action_hint="finalize",
        ),
    ])

    async def step_executor(step_input):
        return next(results)

    workflow.bind_step_executor(step_executor)
    snapshot = await workflow.run(
        FeishuRunInput(
            run_id=run.run_id,
            conversation_id="conv-1",
            chat_id="oc_1",
            thread_id=None,
            trigger_message_id="om_1",
            normalized_user_text="帮我装好 ComfyUI",
        )
    )

    assert snapshot["status"] == "completed"
    assert store.get_run(run.run_id).status == FeishuRunStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_workflow_waiting_user_signal_resumes_to_completion(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")
    workflow = FeishuAgentRunWorkflow()
    workflow.bind_store(store)

    calls = {"count": 0}

    async def step_executor(step_input):
        calls["count"] += 1
        if calls["count"] == 1:
            return HermesStepResult(
                outcome_type=HermesStepOutcome.WAITING_USER,
                assistant_text="need path",
                progress_summary="blocked",
                checkpoint_payload={"stage": "need_path"},
                waiting_reason="path required",
                next_action_hint="await signal",
            )
        assert step_input.user_reply == "/Users/xiaofu/Documents/comfy"
        return HermesStepResult(
            outcome_type=HermesStepOutcome.COMPLETED,
            assistant_text="done",
            progress_summary="done",
            checkpoint_payload={"stage": "complete"},
            waiting_reason="",
            next_action_hint="finalize",
        )

    workflow.bind_step_executor(step_executor)
    task = asyncio.create_task(
        workflow.run(
            FeishuRunInput(
                run_id=run.run_id,
                conversation_id="conv-1",
                chat_id="oc_1",
                thread_id=None,
                trigger_message_id="om_1",
                normalized_user_text="继续装",
            )
        )
    )
    await asyncio.sleep(0.05)
    workflow.user_reply("/Users/xiaofu/Documents/comfy")
    snapshot = await task

    assert snapshot["status"] == "completed"
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_workflow_cancel_signal_marks_run_cancelled(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")
    workflow = FeishuAgentRunWorkflow()
    workflow.bind_store(store)

    async def step_executor(step_input):
        return HermesStepResult(
            outcome_type=HermesStepOutcome.WAITING_USER,
            assistant_text="need path",
            progress_summary="blocked",
            checkpoint_payload={"stage": "need_path"},
            waiting_reason="path required",
            next_action_hint="await signal",
        )

    workflow.bind_step_executor(step_executor)
    task = asyncio.create_task(
        workflow.run(
            FeishuRunInput(
                run_id=run.run_id,
                conversation_id="conv-1",
                chat_id="oc_1",
                thread_id=None,
                trigger_message_id="om_1",
                normalized_user_text="继续装",
            )
        )
    )
    await asyncio.sleep(0.05)
    workflow.cancel_run()
    snapshot = await task

    assert snapshot["status"] == "cancelled"
    assert store.get_run(run.run_id).status == FeishuRunStatus.CANCELLED.value
