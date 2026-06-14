import asyncio

import pytest

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
