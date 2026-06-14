from gateway.platforms.feishu_bridge_models import (
    FeishuInboundKind,
    FeishuRunStatus,
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
