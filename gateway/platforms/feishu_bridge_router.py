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

    def route_message(
        self,
        *,
        conversation_id: str,
        chat_id: str,
        thread_id: str | None,
        message_id: str,
        event_id: str,
        text: str,
    ) -> RouteResult:
        inbound = self.store.record_inbound(
            platform="feishu",
            tenant_id="default",
            chat_id=chat_id,
            thread_id=thread_id,
            message_id=message_id,
            event_id=event_id,
            normalized_text=text.strip(),
        )
        if not inbound.accepted:
            return RouteResult(action="duplicate_ignored")
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
            self.temporal_client.start_run(
                run.run_id,
                conversation_id=conversation_id,
                chat_id=chat_id,
                thread_id=thread_id,
                trigger_message_id=message_id,
                text=text.strip(),
                bridge_metadata={"state_path": str(self.store.state_path)},
            )
            return RouteResult(action="start_run", run_id=run.run_id)

        return RouteResult(action="busy_status", run_id=active.run_id)
