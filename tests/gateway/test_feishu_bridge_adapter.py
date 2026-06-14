from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource


def _make_event(text: str) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            chat_id="oc_1",
            chat_name="Feishu Chat",
            chat_type="dm",
            user_id="ou_user",
            user_name="tester",
            thread_id="thread_1",
        ),
        raw_message={"event_id": "evt_1"},
        message_id="om_1",
    )


def _make_command_event(text: str) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.COMMAND,
        source=SessionSource(
            platform=Platform.FEISHU,
            chat_id="oc_1",
            chat_name="Feishu Chat",
            chat_type="dm",
            user_id="ou_user",
            user_name="tester",
            thread_id="thread_1",
        ),
        raw_message={"event_id": "evt_cmd"},
        message_id="om_cmd",
    )


@pytest.mark.asyncio
async def test_bridge_status_query_sends_status_without_calling_handle_message(tmp_path):
    from gateway.platforms.feishu import FeishuAdapter
    from gateway.platforms.feishu_bridge_router import RouteResult
    from gateway.platforms.feishu_bridge_store import FeishuBridgeStore

    adapter = object.__new__(FeishuAdapter)
    adapter.config = PlatformConfig(enabled=True)
    adapter.config.extra = {"bridge_mode": "temporal"}
    adapter._chat_locks = {}
    adapter._bridge_router = MagicMock()
    adapter._bridge_router.route_message.return_value = RouteResult(action="status_reply", run_id="run-1")
    adapter._bridge_router.store = FeishuBridgeStore(tmp_path / "bridge.json")
    adapter._bridge_router.store._state["runs"]["run-1"] = {
        "run_id": "run-1",
        "conversation_id": "conv-1",
        "chat_id": "oc_1",
        "thread_id": "thread_1",
        "trigger_message_id": "om_1",
        "status": "running",
        "current_step": "installing",
    }
    adapter.send = AsyncMock()
    adapter.handle_message = AsyncMock()

    await adapter._handle_message_with_guards(_make_event("干完了吗"))

    adapter.send.assert_awaited_once()
    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_bridge_disabled_falls_back_to_existing_handle_message():
    from gateway.platforms.feishu import FeishuAdapter

    adapter = object.__new__(FeishuAdapter)
    adapter.config = PlatformConfig(enabled=True)
    adapter.config.extra = {}
    adapter._chat_locks = {}
    adapter.send = AsyncMock()
    adapter.handle_message = AsyncMock()

    await adapter._handle_message_with_guards(_make_event("普通消息"))

    adapter.handle_message.assert_awaited_once()
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_bridge_mode_does_not_swallow_existing_command_messages():
    from gateway.platforms.feishu import FeishuAdapter
    from gateway.platforms.feishu_bridge_router import RouteResult

    adapter = object.__new__(FeishuAdapter)
    adapter.config = PlatformConfig(enabled=True)
    adapter.config.extra = {"bridge_mode": "temporal"}
    adapter._chat_locks = {}
    adapter._bridge_router = MagicMock()
    adapter._bridge_router.route_message.return_value = RouteResult(action="start_run", run_id="run-1")
    adapter.send = AsyncMock()
    adapter.handle_message = AsyncMock()

    await adapter._handle_message_with_guards(_make_command_event("/new"))

    adapter.handle_message.assert_awaited_once()
    adapter.send.assert_not_awaited()
    adapter._bridge_router.route_message.assert_not_called()


@pytest.mark.asyncio
async def test_bridge_busy_status_sends_visible_reply(tmp_path):
    from gateway.platforms.feishu import FeishuAdapter
    from gateway.platforms.feishu_bridge_router import RouteResult
    from gateway.platforms.feishu_bridge_store import FeishuBridgeStore

    adapter = object.__new__(FeishuAdapter)
    adapter.config = PlatformConfig(enabled=True)
    adapter.config.extra = {"bridge_mode": "temporal"}
    adapter._chat_locks = {}
    adapter._bridge_router = MagicMock()
    adapter._bridge_router.route_message.return_value = RouteResult(action="busy_status", run_id="run-1")
    adapter._bridge_router.store = FeishuBridgeStore(tmp_path / "bridge.json")
    adapter._bridge_router.store._state["runs"]["run-1"] = {
        "run_id": "run-1",
        "conversation_id": "conv-1",
        "chat_id": "oc_1",
        "thread_id": "thread_1",
        "trigger_message_id": "om_1",
        "status": "running",
        "current_step": "installing",
        "progress_summary": "still working",
    }
    adapter.send = AsyncMock()
    adapter.handle_message = AsyncMock()

    await adapter._handle_message_with_guards(_make_event("继续"))

    adapter.send.assert_awaited_once()
    adapter.handle_message.assert_not_awaited()
