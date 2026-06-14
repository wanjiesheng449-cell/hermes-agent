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


def test_progress_reply_key_is_unique_per_checkpoint(tmp_path):
    store = FeishuBridgeStore(tmp_path / "feishu_bridge_state.json")
    assert store.mark_reply_sent("run:abc:progress:1", "step 1") is True
    assert store.mark_reply_sent("run:abc:progress:1", "step 1") is False
    assert store.mark_reply_sent("run:abc:progress:2", "step 2") is True
