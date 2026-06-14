import asyncio

import pytest

from gateway.platforms.feishu_bridge_models import FeishuRunStatus
from gateway.platforms.feishu_bridge_store import FeishuBridgeStore
from gateway.platforms.feishu_temporal_client import FeishuTemporalClient


class _WorkflowHandle:
    def __init__(self) -> None:
        self.signal_calls = []
        self.cancel_calls = 0

    async def signal(self, signal_method, *args):
        self.signal_calls.append((signal_method.__name__, args))

    async def cancel(self):
        self.cancel_calls += 1


class _TemporalClientStub:
    def __init__(self, handle: _WorkflowHandle) -> None:
        self.handle = handle
        self.workflow_ids = []

    def get_workflow_handle(self, workflow_id: str):
        self.workflow_ids.append(workflow_id)
        return self.handle


@pytest.mark.asyncio
async def test_cancel_run_uses_workflow_signal_not_direct_cancel():
    handle = _WorkflowHandle()
    client = FeishuTemporalClient(temporal_client=_TemporalClientStub(handle))

    client.cancel_run("run-123")
    await asyncio.sleep(0.05)

    assert handle.signal_calls == [("cancel_run", ())]
    assert handle.cancel_calls == 0


@pytest.mark.asyncio
async def test_start_run_failure_marks_run_failed_in_store(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")

    class _FailingClient:
        async def start_workflow(self, *args, **kwargs):
            raise RuntimeError("boom")

    client = FeishuTemporalClient(temporal_client=_FailingClient())

    await client._start_run(
        run.run_id,
        conversation_id="conv-1",
        chat_id="oc_1",
        thread_id=None,
        trigger_message_id="om_1",
        text="帮我装好 ComfyUI",
        bridge_metadata={"state_path": str(store.state_path)},
    )

    saved = store.get_run(run.run_id)
    assert saved is not None
    assert saved.status == FeishuRunStatus.FAILED.value
    assert saved.current_step == "temporal_start_failed"
