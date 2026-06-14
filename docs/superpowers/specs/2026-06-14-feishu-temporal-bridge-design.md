# Feishu Bridge + Temporal Durable Execution Design

- Date: 2026-06-14
- Status: Draft for review
- Scope: Minimal viable production fix for repeated replies and lost long-running tasks in Feishu/Lark conversations
- Primary goal: Preserve the existing Hermes agent runtime while adding durable execution, idempotent message handling, and reliable progress/status replies for Feishu

## 1. Problem Statement

The current Feishu/Lark chat experience shows two production issues:

1. Repeated replies to short status questions such as "干完了吗".
2. Long-running tasks die mid-execution and do not reliably resume.

These symptoms strongly suggest an integration-layer gap rather than a Hermes core failure:

- The messaging surface can redeliver or replay inbound events.
- The current bridge likely treats status queries and task-execution triggers as the same class of message.
- Long-running execution is not durably isolated from webhook/request lifecycle failures.
- Progress and final replies are not protected by outbound idempotency.

The design goal is not to replace Hermes. The goal is to wrap Hermes in a durable control plane that makes Feishu delivery reliable and recoverable with the smallest practical amount of custom code.

## 2. Goals

### In Scope

- Stop repeated assistant replies caused by duplicate inbound delivery or workflow retries.
- Make long-running agent tasks survive process restarts, worker crashes, webhook timeouts, and transient network failures.
- Support explicit message classes:
  - new task
  - status query
  - user follow-up for waiting tasks
  - cancel request
- Preserve Hermes as the main execution engine.
- Use Temporal as the durable workflow engine.
- Keep Feishu bridge logic thin and focused on translation, idempotency, and routing.

### Out of Scope

- Building a full first-class Feishu platform adapter inside Hermes gateway in this phase.
- Replacing Hermes with a new agent framework.
- Reworking Hermes core prompts, tools, memory, or model orchestration.
- Building a generalized multi-platform durable bridge beyond Feishu in this phase.

## 3. Recommendation

Adopt:

`Hermes + Feishu bridge + Temporal workflow orchestration`

Rationale:

- Hermes already provides the agent core, tool execution surface, and long-lived runtime patterns.
- Temporal provides durable execution, retries, signals, heartbeats, and restart-safe progress tracking.
- A thin bridge layer minimizes risky changes in Hermes core while solving the actual production gap.

This design is intentionally biased toward the user's preferred strategy: reuse mature upstream systems and avoid inventing a new task engine from scratch.

## 4. High-Level Architecture

The system is split into four layers.

### 4.1 Feishu Bridge

Responsibilities:

- Receive Feishu/Lark webhooks
- Verify signatures and normalize payloads
- Persist inbound events with idempotency keys
- Classify incoming user messages
- Query Temporal or the run store
- Send fast acknowledgment responses
- Publish outbound replies through a dedicated activity path

Non-responsibilities:

- Running the agent inline
- Holding long-lived task state in memory
- Deciding agent reasoning behavior

### 4.2 Conversation Orchestrator

Responsibilities:

- Determine whether a message is:
  - a new task
  - a status query
  - a reply to a waiting task
  - a cancel request
- Resolve the active run for a conversation
- Route commands to Temporal workflows via start or signal APIs

This layer stays deliberately thin. It is a control-plane state router, not a second agent.

### 4.3 Temporal Durable Workflow Layer

Responsibilities:

- Persist run lifecycle
- Coordinate retries and recovery
- Store step-level checkpoint references
- Wait for user input via signals
- Handle cancellation
- Expose queryable progress snapshots

Temporal is the source of truth for in-flight task durability.

### 4.4 Hermes Runtime Adapter

Responsibilities:

- Invoke Hermes to execute work segments
- Capture structured progress and completion states
- Return checkpointable execution results to the workflow

Hermes remains the execution engine. Temporal controls when and how Hermes is invoked and resumed.

## 5. Message Classification

Every inbound Feishu message must be classified before any model execution is considered.

### 5.1 New Task

Examples:

- "帮我装 ComfyUI"
- "排查这个报错"
- "把这个需求实现掉"

Handling:

- Create or reuse a conversation record
- Create a new run if no active run exists
- Start a Temporal workflow

### 5.2 Status Query

Examples:

- "干完了吗"
- "现在到哪一步了"
- "什么情况"

Handling:

- Do not invoke Hermes for new reasoning
- Read the active run snapshot
- Return a short structured status reply

This is the key behavior change that prevents repeated answers to the same short question.

### 5.3 User Reply for Waiting Task

Examples:

- Providing a missing path
- Confirming a yes/no choice
- Sending a token or account detail

Handling:

- If the active run is `waiting_user`, send a Temporal signal to resume the run
- Otherwise treat it as a normal new message

### 5.4 Cancel Request

Examples:

- "停止"
- "取消"
- "别跑了"

Handling:

- Signal cancellation to the workflow
- Mark the run as `cancelled`
- Stop future progress messages except cancellation confirmation

## 6. Conversation and Run Model

Each Feishu conversation may have many historical runs but at most one active run.

### 6.1 Active Run Constraint

For a given `conversation_id`, there may be at most one run in:

- `queued`
- `running`
- `waiting_user`
- `retrying`

This prevents overlapping long tasks from fighting over status and reply streams.

### 6.2 Run Lifecycle States

- `idle`: no active run
- `queued`: message accepted and awaiting workflow scheduling
- `running`: Hermes is actively progressing the task
- `waiting_user`: workflow is paused for user input
- `retrying`: temporary failure under automatic retry
- `completed`: task finished successfully
- `failed`: task ended with final failure
- `cancelled`: task was cancelled by the user or system policy

## 7. Temporal Workflow Design

### 7.1 Workflow Name

Suggested name:

`FeishuAgentRunWorkflow`

### 7.2 Workflow Input

- `run_id`
- `conversation_id`
- `chat_id`
- `thread_id`
- `trigger_message_id`
- `normalized_user_text`
- `resume_from_checkpoint`
- `bridge_metadata`

### 7.3 Workflow Responsibilities

- Own the durable lifecycle of a single run
- Orchestrate Hermes execution in resumable segments
- Persist progress snapshots
- Receive and apply user signals
- Handle cancellation and retry policy
- Ensure finalization and terminal state persistence

### 7.4 Workflow Signals

- `user_reply`
- `cancel_run`
- `poke_status` optional; useful later if explicit workflow-level query triggers are needed

### 7.5 Workflow Query

At minimum expose a query returning:

- current status
- current step
- last progress summary
- waiting reason if any
- last updated timestamp

The Feishu bridge uses this query path for fast "干完了吗" handling.

## 8. Hermes Execution Model

The workflow must not assume one Hermes invocation equals one whole task. Instead, Hermes should be wrapped as a segmented activity.

### 8.1 Segment Boundary Rule

Each execution segment may end in one of these outcomes:

- `completed`
- `waiting_user`
- `checkpoint`
- `retryable_error`
- `fatal_error`

### 8.2 Required Structured Result

Each Hermes execution segment returns:

- `outcome_type`
- `assistant_text`
- `progress_summary`
- `checkpoint_payload`
- `waiting_reason`
- `next_action_hint`
- `tool_context_summary`

This allows the workflow to resume from structured state rather than replaying the full conversation blindly after failures.

## 9. Activity Design

Suggested initial activity set:

### 9.1 PersistInboundMessageActivity

Responsibilities:

- Store the normalized inbound event
- Calculate and enforce inbound idempotency

### 9.2 LoadOrCreateRunActivity

Responsibilities:

- Load active run for the conversation
- Create a new run if allowed
- Reject unsafe concurrent run creation

### 9.3 LaunchHermesStepActivity

Responsibilities:

- Invoke Hermes for one resumable execution segment
- Collect structured outcome
- Emit checkpoint data

### 9.4 PublishProgressActivity

Responsibilities:

- Update run snapshot
- Persist run event timeline
- Decide whether to emit an outbound progress message based on throttling policy

### 9.5 SendFeishuReplyActivity

Responsibilities:

- Send outbound Feishu reply
- Use reply idempotency keys
- Record delivery results

### 9.6 FinalizeRunActivity

Responsibilities:

- Mark terminal run status
- Write final timestamps and result summary
- Trigger final outbound message if needed

## 10. Persistence Model

The durable system requires explicit storage beyond Temporal workflow history. At minimum, add four persistence objects.

### 10.1 `inbound_messages`

Purpose:

- Deduplicate webhook events
- Preserve auditability of inbound triggers

Suggested fields:

- `id`
- `platform`
- `tenant_id`
- `chat_id`
- `thread_id`
- `message_id`
- `event_id`
- `sender_id`
- `message_type`
- `normalized_text`
- `raw_payload`
- `dedupe_key`
- `received_at`

Constraints:

- `dedupe_key` unique
- Prefer `platform + tenant_id + message_id`
- Fall back to `event_id` only where message IDs are unavailable

### 10.2 `agent_runs`

Purpose:

- Hold the application-facing run record
- Provide fast status lookup outside Temporal internals

Suggested fields:

- `run_id`
- `conversation_id`
- `chat_id`
- `thread_id`
- `trigger_message_id`
- `workflow_id`
- `status`
- `current_step`
- `progress_summary`
- `checkpoint_version`
- `checkpoint_payload`
- `waiting_reason`
- `last_error`
- `started_at`
- `updated_at`
- `completed_at`

Constraints:

- Single active run per conversation across active states

### 10.3 `outbound_messages`

Purpose:

- Prevent duplicate outbound replies
- Record send outcomes

Suggested fields:

- `outbound_id`
- `run_id`
- `chat_id`
- `thread_id`
- `reply_type`
- `reply_key`
- `content`
- `sent_status`
- `platform_message_id`
- `created_at`

Constraints:

- `reply_key` unique

Example reply keys:

- `run:{run_id}:start`
- `run:{run_id}:progress:{checkpoint_version}`
- `run:{run_id}:waiting`
- `run:{run_id}:final`

### 10.4 `run_events`

Purpose:

- Timeline reconstruction
- Debugging and observability

Suggested fields:

- `id`
- `run_id`
- `event_type`
- `event_payload`
- `created_at`

## 11. Idempotency Strategy

### 11.1 Inbound Idempotency

Every Feishu event is persisted before task execution starts. If the dedupe key already exists:

- do not create a new run
- do not signal a workflow twice
- do not send duplicate progress replies unless explicitly allowed

### 11.2 Outbound Idempotency

Every externally visible reply is sent through a unique reply key. If an activity retries or a workflow is replayed:

- the send path checks existing `reply_key`
- if already sent successfully, the activity becomes a no-op

### 11.3 Status Query Idempotency

Status queries must not mutate the run. They are read-only lookups. If Feishu repeats the same query twice, the user receives the latest snapshot twice, not two different agent runs.

## 12. Progress and UX Policy

The bridge should avoid noisy progress spam.

### 12.1 When to Send Updates

- On task start
- On meaningful stage transition
- When entering `waiting_user`
- On long quiet intervals if the task is still healthy
- On terminal completion, failure, or cancellation

### 12.2 When Not to Send Updates

- On every tool event
- On every workflow heartbeat
- On every inbound status query

### 12.3 Status Reply Template

A status reply should be short and structured:

- current status
- current step
- latest progress summary
- whether user action is required
- last update time

This avoids the current failure mode where the assistant re-generates a long repetitive explanation each time the user asks for progress.

## 13. Failure Handling

### 13.1 Temporary Failure

Examples:

- transient Feishu API error
- temporary network issue
- Hermes subprocess exits unexpectedly once

Handling:

- automatic activity retry
- run enters or remains `retrying`
- user is not immediately spammed unless retries exhaust or the quiet interval threshold is crossed

### 13.2 Recoverable Block

Examples:

- missing path
- missing credential
- missing confirmation from user

Handling:

- persist waiting reason
- set run to `waiting_user`
- pause workflow until signal arrives

### 13.3 Final Failure

Examples:

- invalid configuration
- repeated Hermes recovery failure
- dependency corruption not recoverable by retry policy

Handling:

- run enters `failed`
- send one structured final failure message containing:
  - where it failed
  - what was attempted
  - what the user needs to do next

## 14. Cancellation

When a user requests cancellation:

- signal the workflow
- attempt graceful stop of the current Hermes segment
- persist `cancelled`
- send one cancellation confirmation
- suppress future progress updates for that run

## 15. Deployment Topology

Recommended deployable units:

1. `feishu-bridge` service
2. `temporal` service cluster or managed deployment
3. `temporal-worker` service
4. shared persistence store for application records
5. Hermes runtime dependency environment on workers

The Feishu bridge should remain stateless apart from database writes and Temporal API use. Workers may be restarted independently without losing task state.

## 16. Implementation Strategy

### Phase 1: Bridge Safety Foundation

- Implement inbound persistence and dedupe
- Add conversation/run records
- Add status-query classification path
- Add outbound idempotency for messages

### Phase 2: Temporal Integration

- Introduce `FeishuAgentRunWorkflow`
- Implement segmented Hermes activity wrapper
- Add progress snapshot updates and workflow query path

### Phase 3: Recovery and UX Hardening

- Add waiting-user resume path
- Add cancellation
- Add throttled progress updates
- Add retry policy tuning and observability

### Phase 4: Production Validation

- Load-test duplicate webhook delivery
- Crash-test worker restart during active runs
- Verify status queries during long execution
- Verify no duplicate outbound messages on retries

## 17. Alternatives Considered

### 17.1 Hermes + Lightweight Queue Only

Pros:

- lower initial complexity
- simpler deployment

Cons:

- more custom retry/recovery logic
- weaker observability
- more homegrown durability code over time

Conclusion:

Reasonable as a stopgap, but not preferred for this use case.

### 17.2 Replace Hermes with a New Agent Framework

Pros:

- possible long-term unification

Cons:

- high migration cost
- high behavior regression risk
- does not solve Feishu delivery semantics by itself

Conclusion:

Not recommended for the current scope.

## 18. Risks

- Hermes may need a thin adapter layer to expose segment-level checkpoints cleanly.
- Feishu message semantics may vary between message types and threading surfaces; dedupe-key selection must be validated against real payloads.
- If status-query classification is too naive, user messages may be misrouted. This should be rule-based first, then refined with explicit commands if needed.
- Temporal workflow history growth must be controlled by checkpoint discipline and continue-as-new if needed for very long conversations.

## 19. Success Criteria

The design is successful when all of the following hold:

1. A duplicated inbound Feishu event does not create a duplicated run.
2. Asking "干完了吗" during a running task never launches a new agent execution.
3. A worker restart during an active run does not lose the run.
4. A progress message or final message is not emitted twice due to retries.
5. A run blocked for user input pauses cleanly and resumes from signal, not from conversational guesswork.

## 20. Final Recommendation

Proceed with a minimal-but-production-oriented architecture:

`Keep Hermes as the execution engine. Add a thin Feishu bridge. Use Temporal as the durable control plane.`

This is the smallest credible design that directly addresses the current production issues without replacing the mature parts of the system.
