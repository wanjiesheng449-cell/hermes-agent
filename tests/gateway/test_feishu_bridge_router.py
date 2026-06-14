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
    assert result.run_id == run.run_id
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
    assert result.run_id == run.run_id
    temporal.signal_user_reply.assert_called_once_with(run.run_id, "/Users/xiaofu/Documents/comfy")


def test_cancel_request_cancels_existing_run(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    run = store.create_run("conv-1", "oc_1", None, "om_1")
    store.update_run(run.run_id, status=FeishuRunStatus.RUNNING, current_step="installing")
    temporal = MagicMock()
    router = FeishuBridgeRouter(store=store, temporal_client=temporal)

    result = router.route_message(
        conversation_id="conv-1",
        chat_id="oc_1",
        thread_id=None,
        message_id="om_cancel",
        event_id="evt_cancel",
        text="取消",
    )

    assert result.action == "cancel_run"
    assert result.run_id == run.run_id
    temporal.cancel_run.assert_called_once_with(run.run_id)


def test_duplicate_inbound_is_ignored_before_routing(tmp_path):
    store = FeishuBridgeStore(tmp_path / "bridge.json")
    temporal = MagicMock()
    router = FeishuBridgeRouter(store=store, temporal_client=temporal)

    first = router.route_message(
        conversation_id="conv-1",
        chat_id="oc_1",
        thread_id=None,
        message_id="om_dup",
        event_id="evt_dup",
        text="帮我装好 ComfyUI",
    )
    second = router.route_message(
        conversation_id="conv-1",
        chat_id="oc_1",
        thread_id=None,
        message_id="om_dup",
        event_id="evt_dup",
        text="帮我装好 ComfyUI",
    )

    assert first.action == "start_run"
    assert second.action == "duplicate_ignored"
    temporal.start_run.assert_called_once()
