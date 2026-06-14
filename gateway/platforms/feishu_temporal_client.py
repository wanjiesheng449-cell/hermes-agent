from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class FeishuTemporalClient:
    def __init__(
        self,
        *,
        temporal_client=None,
        target_host: str = "",
        namespace: str = "default",
        task_queue: str = "feishu-bridge",
    ) -> None:
        self._client = temporal_client
        self._connect_task = None
        self._target_host = target_host
        self._namespace = namespace or "default"
        self._task_queue = task_queue or "feishu-bridge"

    @classmethod
    def from_config(cls, gateway_config):
        extra = getattr(gateway_config, "extra", {}) or {}
        return cls(
            target_host=str(extra.get("temporal_target_host", "") or ""),
            namespace=str(extra.get("temporal_namespace", "") or "default"),
            task_queue=str(extra.get("temporal_task_queue", "") or "feishu-bridge"),
        )

    @property
    def enabled(self) -> bool:
        return bool(self._target_host)

    def _schedule(self, coro) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("[FeishuTemporalClient] No running loop; dropping scheduled Temporal call")
            return
        loop.create_task(coro)

    async def _ensure_client(self):
        if self._client is not None:
            return self._client
        if not self.enabled:
            return None
        if self._connect_task is None:
            self._connect_task = asyncio.create_task(self._connect())
        self._client = await self._connect_task
        return self._client

    async def _connect(self):
        try:
            from temporalio.client import Client
        except ImportError:
            logger.warning("[FeishuTemporalClient] temporalio is not installed; Temporal bridge disabled")
            return None
        try:
            return await Client.connect(self._target_host, namespace=self._namespace)
        except Exception:
            logger.warning(
                "[FeishuTemporalClient] Failed to connect to Temporal at %s",
                self._target_host,
                exc_info=True,
            )
            return None

    async def _start_run(
        self,
        run_id: str,
        *,
        conversation_id: str,
        chat_id: str,
        thread_id: str | None,
        trigger_message_id: str,
        text: str,
        bridge_metadata: dict | None = None,
    ) -> None:
        client = await self._ensure_client()
        if client is None:
            return
        from gateway.platforms.feishu_temporal import FeishuAgentRunWorkflow, FeishuRunInput

        workflow_id = f"feishu-run-{run_id}"
        try:
            await client.start_workflow(
                FeishuAgentRunWorkflow.run,
                FeishuRunInput(
                    run_id=run_id,
                    conversation_id=conversation_id,
                    chat_id=chat_id,
                    thread_id=thread_id,
                    trigger_message_id=trigger_message_id,
                    normalized_user_text=text,
                    bridge_metadata=bridge_metadata or {},
                ),
                id=workflow_id,
                task_queue=self._task_queue,
            )
        except Exception:
            logger.warning("[FeishuTemporalClient] Failed to start workflow %s", workflow_id, exc_info=True)

    def start_run(
        self,
        run_id: str,
        *,
        conversation_id: str,
        chat_id: str,
        thread_id: str | None,
        trigger_message_id: str,
        text: str,
        bridge_metadata: dict | None = None,
    ) -> None:
        self._schedule(
            self._start_run(
                run_id,
                conversation_id=conversation_id,
                chat_id=chat_id,
                thread_id=thread_id,
                trigger_message_id=trigger_message_id,
                text=text,
                bridge_metadata=bridge_metadata,
            )
        )

    async def _signal_user_reply(self, run_id: str, text: str) -> None:
        client = await self._ensure_client()
        if client is None:
            return
        from gateway.platforms.feishu_temporal import FeishuAgentRunWorkflow

        workflow_id = f"feishu-run-{run_id}"
        try:
            handle = client.get_workflow_handle(workflow_id)
            await handle.signal(FeishuAgentRunWorkflow.user_reply, text)
        except Exception:
            logger.warning(
                "[FeishuTemporalClient] Failed to signal user reply for %s",
                workflow_id,
                exc_info=True,
            )

    def signal_user_reply(self, run_id: str, text: str) -> None:
        self._schedule(self._signal_user_reply(run_id, text))

    async def _cancel_run(self, run_id: str) -> None:
        client = await self._ensure_client()
        if client is None:
            return
        from gateway.platforms.feishu_temporal import FeishuAgentRunWorkflow

        workflow_id = f"feishu-run-{run_id}"
        try:
            handle = client.get_workflow_handle(workflow_id)
            await handle.signal(FeishuAgentRunWorkflow.cancel_run)
        except Exception:
            logger.warning("[FeishuTemporalClient] Failed to cancel workflow %s", workflow_id, exc_info=True)

    def cancel_run(self, run_id: str) -> None:
        self._schedule(self._cancel_run(run_id))

    async def query_snapshot(self, run_id: str):
        client = await self._ensure_client()
        if client is None:
            return None
        from gateway.platforms.feishu_temporal import FeishuAgentRunWorkflow

        workflow_id = f"feishu-run-{run_id}"
        try:
            handle = client.get_workflow_handle(workflow_id)
            return await handle.query(FeishuAgentRunWorkflow.snapshot)
        except Exception:
            logger.warning("[FeishuTemporalClient] Failed to query workflow %s", workflow_id, exc_info=True)
            return None
