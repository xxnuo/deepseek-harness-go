# DeepSeek Harness Go 语义对齐记录

当前基线：上游 `deepseek-harness` commit `dd6322d604e00eec1ba5e0c8541159906a21094a`（`dsh-v0.1.2-alpha.3`）。此前 rc.2 与 alpha.1 的完成记录保留为迁移历史；本轮继续按源码执行路径、状态机、取消、持久化、事件和结果语义核对，不按文件数量或同名文件判断。

## 对齐顺序

1. 普通 Agent tool scheduler：`parallel`/`exclusive`、有界并发、exclusive barrier、模型顺序提交、abort drain、未派发调用的 synthetic result。
2. 通用 ToolRuntime：`additionalContexts`、`concludesTurn`、`finalizeContent`、`presentationMeta`、输出 materialization、标准 abort 结果。
3. `run_code`：复用通用 scheduler 和结果管线，不再旁路 native tool dispatch。
4. plan、goal、schedule、session cancel/search、Cordis inspect、attachments、subagent、web、shell、terminal、sandbox/approval。
5. 每项均须有上游源码对照、Go 生产调用链、行为测试和 runtime-assets 包装器验证；未验证项不得标记完成。

## 进度

- [x] 固定上游基线并完成现状语义审计。
- [x] 普通 Agent tool scheduler。
- [x] 通用 ToolRuntime 结果管线。
- [x] `run_code` native dispatch。
- [x] plan-mode 状态、prompt、review 和 projection 语义。
- [x] goal domain、model tools 和 autonomous round driver 语义。
- [x] schedule domain、tools、durability 和 runtime 语义。
- [x] session cancel 与 session-query 语义。
- [x] session-reference URI、discovery、projection、budget、durable context 和 Cordis facade 语义。
- [x] attachments/read-image、request-image、ACP/MCP image admission 语义。
- [x] Agent Team roster、mailbox、task board、wait、tool scope 和 runtime disposal 语义。
- [x] web capability、HTTP fetch policy 和 search providers 语义。
- [ ] 其余 capability 逐项对齐。
- [ ] 全量验证和收口。

上面两项是跨版本的长期对齐清单；本节的 alpha.1 专项已单独完成并按下述边界收口。

## dsh-v0.1.2-alpha.1 跟进计划与逐步记录

验收总门禁：`make verify-upstream test vet race smoke-standalone smoke-clean-archive`。所有 runtime 测试均通过 `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- ...` 使用当前 pinned 上游资产；静态编译、清单计数和镜像/资产生成不能代替真实 CLI、RPC、provider request 与持久化行为验证。

1. [已完成] 固定并审计 alpha.1 基线：已确认 `upstream.lock`、上游 `HEAD` 与 tag 精确对应 `cd5ef8148158c3a752a658978873241fdf8e2bbc` / `dsh-v0.1.2-alpha.1`，且上游工作树干净；已按 package API、调用链、状态与事件语义完成本轮差异核对。未验证的完整 npm/Cordis 生态和真实外部 provider 不计入已完成项。
2. [已完成] PTC canonical 与历史兼容：工具展示配置只接受 `native|ptc|both`；新 header、selection event、fork/subagent 继承写 `ptc`；旧磁盘 header/event 中的 `code` 只在运行时解析为 `ptc`，冷加载不改写原 JSONL；保留 `tool/code-dispatch*`、`tools-code-mode` 与 `run_code.code` 持久契约。验证：`go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -count=1 -run '^TestPTCCompatibility' ./internal/harness`。
3. [已完成] `modelSelectionSettings`：实现 `subagent-model-selection` 设置、顶层 session 在 publication/late tool installation 边界冻结设置、子 session 从父 durable policy 或冻结快照继承、恢复 seed 不重采样、route preflight、subagent tool route 参数和 `list_subagent_models`。验证：`go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run '^TestSubagentModelSelection' -count=1 ./internal/harness`；`go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run '^TestSubagentModelSelection' -count=1 ./internal/harness`。
4. [已完成] DeepSeek request extensions、session log、plugin package inventory 与 pi-ai 0.84.2：已补齐 registry/acceptance、增量投递、基于 Loader tree base 的 immutable package provenance、Baseten wire、thinking budget 与 finish reason；SDK JSON-RPC 仅对字符串/数字 id dispatch，notification 和非法 id 丢弃。每项均保留定向测试证据；httptest wire 仅证明协议时序，不等价真实外部 provider/credential 验证。
5. [已完成] alpha.1 workspace inventory：按 pinned tree 重新生成 264-root 正式清单，分类 `frontend=41`、`hybrid=9`、`ported=178`、`replaced=23`、`support=13`，无 `missing`/`partial`；`packages/sdk/client` 仅有 Go SDK server/runtime surface，故标为 `hybrid`，不声明公开 TypeScript SDK client 对等；webhook 两包仅标为可选 support，不声明已移植 ingress/rule runtime。
6. [已完成] 同步 runtime assets、public facade 和生成物后，已运行专项测试、CLI/RPC/provider wire 验收及完整 Makefile 门禁；命令与退出结果记录在本节末尾。httptest wire 仅覆盖确定性协议边界，不替代真实外部 provider/credential 验证。
7. [已完成] 已复核 diff、dirty worktree 保留边界、未实现分类和验证证据；本轮只保留用户既有及 alpha.1 对齐改动，未回退无关修改。完整 npm/Cordis 生态、平台 ripgrep 打包和低频插件配置继续作为明确 deferred scope。

## 已完成：普通 Agent tool scheduler

上游入口：`packages/core/agent-loop/src/tool-calls.ts`。

Go 入口：`internal/harness/agent.go`、`internal/harness/agent_tools.go`。

验收：并行工具受 `maxParallel` 限制；exclusive 调用等待已运行调用并阻塞后续调用；结果按模型调用顺序落盘；调用取消后已启动调用完成 drain，未启动调用写入 `ABORTED_BEFORE_DISPATCH` 结果；现有串行行为和 durable event 顺序保持不变。

Go 实现：`internal/harness/tool_scheduler.go`。`Tool.IsConcurrencySafe` 仅在精确返回 `true` 时进入 parallel；缺失、`false` 和 panic 均回退 exclusive。`Config.MaxParallelToolCalls` 默认 10，负值拒绝，零值按 Go 配置惯例视为未设置。普通 Agent 已从逐个 `executeToolCall` 切换到统一 scheduler；上游声明可并行的 fs read/read_image、web、session-query 和 subagent delegation 已接入 classifier。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run '^TestToolScheduler' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run '^TestToolScheduler' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness`
- `go vet ./...`
- `make verify-upstream`
- `make verify-public-facade`

仍属下一工作项的差异：通用 ToolRuntime 的结果 materialization、`additionalContexts` 批次接收、`concludesTurn`、`finalizeContent`、`presentationMeta`，以及 `run_code` 复用该管线。

## 已完成：通用 ToolRuntime 结果管线

上游入口：`packages/core/tools/src/index.ts`。

Go 入口：`internal/harness/agent_tools.go`、`internal/harness/tool_scheduler.go`、`internal/harness/workflow.go`。

验收：工具成功值按声明 schema 无损快照并渲染；错误、取消、post decision、最终内容和 presentation metadata 只 materialize 一次；附加 context 按模型顺序进入下一 step；`concludesTurn` 在提交当前批次后结束 turn；普通 Agent 与 `run_code` 使用同一结果语义。

Go 实现：`ToolRunContext` 承载 `deferContext` 和 `concludeTurn`；成功值、metadata、render 和 finalizer 均在有序提交前规范化。finalizer 在生产 scheduler 中恰好调用一次，异常或 panic 物化为 `INVALID_TOOL_OUTPUT` 工具结果而不是击穿 turn。post/finalizer 失败和失败结果不能携带 `concludesTurn`。同一模型批次中的所有调用先按顺序提交，再由聚合后的 terminal marker 结束 turn；不会因首个 terminal 工具跳过后续调用。工具体 defer 的 context 在 post block 时丢弃，block 自身的 hook context 保留。普通 Agent 的旧串行旁路已删除。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'Test(ToolScheduler|ToolRuntime|ToolLoop)' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run 'Test(ToolScheduler|ToolRuntime|ToolLoop)' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness`

## 已完成：`run_code` native dispatch

上游入口：`packages/core/tools/src/code-mode.ts`。

Go 入口：`internal/harness/workflow.go`。

验收：嵌套调用按实时 classifier 使用有界 rolling pool 和 exclusive barrier；pre/post/finalizer 与普通 native 调用共用结果语义；post 和 context 按程序提交顺序提交；nested `additionalContexts`、最终 image content 和成功 `concludesTurn` 转运到外层 `run_code`；程序结束后停止新派发并 drain 已启动调用；start/settle 事件保持在外层调用结束前。

Go 实现：`internal/harness/code_tool_scheduler.go`。嵌套调用不再使用硬编码工具名和 `RWMutex`；每次启动前从调用 session 的实时可见工具集重新分类，使用 `MaxParallelToolCalls` 有界 rolling pool，exclusive 调用形成 barrier。prepare 串行，只有工具体并行；post、finalizer、context、terminal marker、settle event 和 Promise resolution 按程序提交顺序提交。嵌套 presentation metadata 因 parent call 被抑制；最终 image content 作为 `tools-code-mode` deferred context 转运，不混入 `run_code` 自身文本结果。程序失败、超时或取消物化为 `CodeRunFailedError/CODE_RUN_FAILED` 并携带捕获日志。取消后已启动调用 drain 并记录 settle，排队调用不产生 start/settle；每次补充池前直接检查 context，避免 settle/cancel 竞态启动下一项。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'TestRunCode|TestToolRuntime|TestToolScheduler' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'TestRunCodeCancellationDrainsStartedAndAbandonsQueued|TestRunCodeBoundsNestedParallelBodies|TestRunCodeCommitsNestedFinalizersAndContextsInSubmissionOrder' -count=20 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run 'TestRunCode|TestToolRuntime|TestToolScheduler' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness`
- `go vet ./...`

## 已完成：plan-mode

上游入口：`packages/plan/plan-mode/src/index.ts`，并逐项对照 `plan-mode.spec.ts`、`integration.spec.ts` 和 `projection.spec.ts`。

Go 入口：`internal/harness/plan_mode.go`、`internal/harness/commands.go`、`internal/harness/prompt.go`、`internal/harness/agent.go`、`internal/harness/projections.go`。

验收：`plan/mode` 仍是最后事件胜出的持久状态；idle `/plan` 立即提交，开放 turn 内只保留 latest intent，并在下一 accepted step 的 request assembly 前提交；provider transport retry 复用原 assembly，不会提前采纳并发选择。模式切换只改变 `plan:policy`，不改变 native 或 code-mode 工具目录。用户触发的切换根据最后一个 `request/header` 注入一次 plugin narration；`exit_plan_mode` 审批成功则静默排队退出，拒绝、自定义反馈、dismiss、取消和无效 plan 均保持 logged plan mode。

Go 实现：默认和 preset 均提供部署拥有的 plan policy；preset 的空 section、非映射配置和未知字段在加载时失败。`exit_plan_mode` 始终注册，在包含 plan-mode 插件的 preset 中稳定可见，只接受以一级 `# ` 标题开头的完整 plan，通过现有 `question/requested` 通道发送 `plan-review` intent。批准结果在当前工具批次内不改 logged state，下一 step 边界先写 `plan/mode` 再组装 prompt。projection 继续完全由 `command/run`、配对的 `command/done` 和 `plan/mode` 冷重放得到 `{active,pending}`。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'TestPlanMode|TestExitPlanMode|TestApprovedPlanReview' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run 'TestPlanMode|TestExitPlanMode|TestApprovedPlanReview|TestToolScheduler|TestToolRuntime' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'TestShippedPresetFiltersModelToolCatalog|TestPermissionPlanAndStatsProjections|TestPlanProjectionPairsCommandSettlement|TestCommandCatalogAndLifecycle' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness`

## 当前工作项：其余 capability 逐项对齐

### Skill 生态补强（本轮）

对照 `packages/skill/tool-skill` 与 `packages/skill/skill-filesystem` 的实际调用链，补齐了四态调用策略：`modelInvocable` 与 `userInvocable` 独立生效；目录只收录可供模型调用的 skill，直接用户 `/name` 手势只注入可供用户调用的 skill，并保持注入上下文位于本步请求末尾。文件系统发现同时支持 `<name>/SKILL.md` 与平铺 `<name>.md`，frontmatter 的布尔策略接受上游约定的 true/false、yes/no、on/off、1/0 形式；旧驼峰键和非法策略值按 fail-closed 排除该 skill。行为测试覆盖 model-only、user-only、user-disabled、平铺文件与目录生命周期。

## 已完成：web capability/providers

上游入口：`packages/web/web/src/index.ts`、`packages/web/tool-web/src/{index,search,fetch}.ts`、`packages/web/web-fetch-http/src/{index,provider,policy}.ts`，以及 DeepSeek、Exa、Perplexity search provider 实现和对应 tests。

Go 入口：`internal/harness/web.go`、`internal/harness/web_search_providers.go`。

验收：provider selection 保留结构化错误；single/multi query 在去重后并发执行，首个失败取消 sibling 并 drain，结果按 round-robin 合并和全局 `maxResults` 截断。工具 timeout、parallel-safe、schema、render 和 presentation metadata 与通用 ToolRuntime 对接。fetch 只允许 HTTP(S)，实施 credential、redirect、content-type、charset、byte/char、timeout 和 depth policy；HTML 转 Markdown 后再执行完整输出 cap。DeepSeek native block/citation join 和 request event、Exa/Perplexity request/mapping/error path 均按上游控制流核对。Go Engine 的 Host provider 注册现在遵循 PluginInventory：插件启用即注册，selection 不参与装载；ApplyRuntimeConfig 会先 disposer 再按 Loader roster 重建。动态 Cordis provider 同时支持 search/fetch、Promise、AbortSignal、provider-specific error code，并在 stop/update/HMR 前完成 disposer。provider id 允许在 composition 阶段未知，missing/unavailable/ambiguous 均延迟到执行期；未配置时按注册顺序报告 ambiguity，但不会按顺序隐式选择。shipped config 默认关闭 fetch、search timeout 60 秒，与上游 base patch 一致。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(Web|HTTPWebFetch|RenderWebFetch|DeepSeekWebSearch|ExaSearch|PerplexitySearch)' -count=1 -timeout=180s`

## 已完成：jobs

上游入口：`packages/jobs/jobs/src/*`、`packages/jobs/jobs-local/src/*`、`packages/jobs/tool-jobs/src/*` 及对应 tests。

验收：controller admission 是 owner-relative；global run 注册的 controller 服务所有 owner，session-scoped run 只服务同一 session composition。listeners 同样按 owner scope 过滤。start 在 starter 前完成 active-limit 和 controller admission，失败不分配 id/资源；starting reservation 防止并发超限。settlement first-wins，非法/拒绝 outcome 归 `failed`，waiters、changed、done 的提交顺序与 reported 语义保持一致。stream output 使用消费 cursor、保留 tail 并标记 lossy；final-output 只在 terminal 后暴露。read/list/get/kill/wait 按 exact owner session fence，stopping 保留 active slot，取消异常不改变状态。owner/session disposal 取消、等待并移除 owned records，teardown records 标记 reported；Engine close 取消并 drain 全部 jobs。completion notice 按 busy next-step、idle wakeup（每 owner 连续 3 次后 quiet）投递，并由 user claim 重置预算。

Go 入口：`internal/harness/jobs.go`、`internal/harness/dynamic_lifecycle_services.go`、`internal/harness/sdk.go`。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test.*Jobs|Test.*Job|TestBackgroundJobCompletionQueuesOwnerNotice|TestGlobalDynamicCordisProfileHostServices' -count=1 -timeout=240s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -count=1 -timeout=300s`

## 已完成：goal

上游入口：`packages/goal/goal/src/index.ts`、`packages/goal/tool-goal/src/index.ts`、`packages/goal/tool-goal/src/authority.ts`、`packages/goal/goal-round-driver/src/index.ts`，并逐项对照三组 package tests。

Go 入口：`internal/harness/goal.go`、`internal/harness/model_tools.go`、`internal/harness/prompt.go`、`internal/harness/agent.go`。

验收：goal change 按 exact revision CAS 持久化和冷重放；恢复与 fork 后 active goal disarm；preset 的 `defaultMaxGoalRounds`、`blockedAfterConsecutiveRounds` 和 driver composition 进入 session runtime；三种 goal tool 只在开放 driver 内运行，create/edit/pause/resume 只接受 root direct-human authority，complete/blocked 额外接受 exact admitted goal round；错误保留结构化 code；自动 round 有进程内不可伪造 reservation、revision/content/round fence，排队后竞争 human prompt 使旧 reservation stale；hook 拒绝写 canonical `prompt-rejected` blocker；取消、错误、max-tokens 和 round-limit 使用对应 pause/disarm/block 结果；autonomous terminal update 在下一 step 注入一次 grounded wrap-up，direct-human terminal update 不注入。

Go 实现补充：`tool:goal` policy 按 preset threshold 注入；未组合 `goal-round-driver` 的 preset 不自动继续。自动 reservation 使用 process-local 标记，恢复出来或外部伪造的 `source.kind=goal` 不能取得 round authority。竞争 human prompt 标记旧 reservation stale，human turn 完成后重新基于当前 revision 预留。blocked wrap-up 的 summary 使用公开 action `blocked`，不暴露内部 mutation 名 `block`。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'TestGoal|TestAutonomousGoal|TestQueuedHumanPrompt|TestForgedGoal|TestDirectHumanGoal|TestPresetPrompt|TestShippedPreset|TestToolRuntime' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run 'TestGoal|TestAutonomousGoal|TestQueuedHumanPrompt|TestForgedGoal|TestDirectHumanGoal' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness`
- `go vet ./...`

## 已完成：schedule

上游入口：`packages/schedule/schedule/src/domain.ts`、`tools.ts`、`transaction.ts`、`persistence.ts`、`runtime.ts`、`invariant.ts`，并逐项对照 domain/tools/runtime/restart/invariant tests。

Go 入口：`internal/harness/schedule.go`、`internal/harness/session_persistence.go`、`internal/harness/session_persistence_sqlite.go`、`internal/harness/runtime_invariants.go`。

验收：v1 create/delete/dispatch durable stream exact-shape fold，fork 只读取 owned suffix，id 永不复用；after/at/every 严格输入、四位年份 UTC、显式 offset、IANA local time、DST gap/overlap 和 safe-integer 边界；every 使用 creation-aligned fixed rate，只选择 latest missed occurrence；due one-shot 按 target 和 create order 优先，recurring 按 distinct rule 合成一个 batch；framing 对 id/prompt 使用 JSON escaping。三个 model tool 为 exclusive，具有 closed value/error output union，先验证 shape，再进入可取消 FIFO；create/list/delete 均在读取前执行 persistence preflight，实际 mutation 后执行 durability barrier，失败返回带 operation/id 的 `persistence_uncertain`，取消等待不会落盘。Schedule 只属于 root agent，preset composition 能正确暴露三个工具。

runtime 只在 owned suffix 确有 schedule event 时 checkpoint；长 timer 分段并在唤醒后重读 wall clock；durability preflight 后取得 session maintenance 边界，再重读 fold/clock，依次写 framing followup 和 dispatch，释放后完成 barrier；preflight 失败保留 overdue work 等待下一 trigger，barrier 失败不重复已排队 reminder；append/partial batch failure fault closed；dispose 取消等待并等待 runtime 退出。JSONL 提供同步 durability acknowledgement，SQLite coordinator 的 `Flush` 强制排空 session write queue。Schedule invariant 在 append 前拒绝非法 transition，并容忍 fork seed 尚未完整写入的构造窗口。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'TestSchedule|TestShippedPresetFiltersModelToolCatalog' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run 'TestSchedule' -count=1 ./internal/harness`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness`
- `go vet ./...`

## 已完成：session cancel

上游入口：`packages/core/agent/src/index.ts`、Agent loop driver、subagent/team interrupt 调用链及对应测试。

Go 入口：`internal/harness/agent.go`、`internal/harness/sdk.go`、`internal/harness/subagents.go`、`internal/harness/agent_team.go`、动态 Cordis agent 服务。

验收：首个 wake 在 worker 启动前同步 claim，取消不会落入 goroutine 启动窗口；同一 activity 的首次 typed cause 胜出，支持 `user`、`parent`、`hook`、`disposed`；host `CancelSession` 使用 user + keepInbox，subagent/team interrupt 使用 parent + keepInbox，dispose 使用 disposed。`KeepInbox:false` 清除 next-turn/next-step 并写 canceled splice；取消后新 prompt 在旧 turn 收敛后继续执行；idle cancel 不污染后续 prompt。流式文本、reasoning、半截 tool call、retry 和 scheduler drain 均保留取消语义。Engine 关闭时只结算非 nil claimed/pending prompt。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'TestCancel|TestPromptQueuesDistinctTurns|TestPersistedInboxPromptRunsAfterSessionAttach|TestDynamicCordisAgentMaintenanceWakeAndCancelSemantics|TestToolSchedulerCancellation' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(Subagent|AgentTeam|DynamicCordisAgent|SDK).*' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'TestFinishClosedSessionWorker|TestCancel' -count=1`

## 已完成：session-query

上游入口：`packages/session-query/session-query/src/*`、`packages/session-query/session-query-sqlite/src/*`、`packages/session-query/tool-session-query/src/*` 及三组 package tests。

Go 入口：`internal/harness/session_query.go`、`internal/harness/prompt.go`、JSONL `SessionStore` 与独立 storage/query SQLite backend。

验收：5 个 cursor-free 工具按 caller 的精确 header cwd 授权；null-cwd 只允许 self，显式空 target 不退化为 self，缺失和跨 workspace target/parent 不可区分。search 为 exclusive 且默认 30 秒 timeout，trace/read 为 parallel；order 113 prompt 与 generic call presentation 已接入。参数支持 safe integer、非空数组、精确 ISO 时间和亚毫秒整数域映射，read window 默认上限 50。logical corpus 使用 live precedence，cold/persisted-only 会话读取当前持久化快照，报告准确 availability，并检测 immutable header source conflict；阻塞后端返回后仍保留取消。搜索使用 Unicode token phrase、去音调大小写归一、匹配次数/文档长度/时间/seq 排名和 240 code-point snippet。当前 step 排除、结果 cap、surface 分类、事件关系、深层 lineage 迭代遍历、隐藏祖先环错误脱敏、unauthorized subtree pruning、标题和深事件快照均与上游模型可见语义一致。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'TestSessionQuery' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'TestSessionQuery|TestEngineDefaultSessionStoreLazyPersistenceAndRestart|TestJSONLSessionStoreLazyMaterializationAndSafeLocation|TestModelSubagentCompositionPersistsAcrossRestart' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- vet ./...`

## 已完成：session-reference/context

上游入口：`packages/context/session-reference/src/config.ts`、`index.ts`、`projection.ts`、`serialization.ts`、`types.ts`、`uri.ts` 及 `session-reference.spec.ts`。

Go 入口：`internal/harness/session_reference.go`、prompt admission、durable inbox admission、Remote 与动态 Cordis facade。

验收：canonical URI 对任意字符串和空 session id 无损往返，显式 Markdown mention 与 bare URI 按出现顺序解析并替换为可读 `@label`，畸形 canonical candidate 失败；prompt admission 自动解析 direct-human text block，非 text block 保持，失败不落盘。引用先按 session id 去重再执行 1-3 上限，首个 label 胜出且显式空 label 不退化；source read 并发、输出维持引用顺序，持久化冷会话可 exact-read，live 在持久化 inspect 期间重新出现时优先，取消覆盖 read failure。

candidate discovery 使用完整 logical corpus，self 排除，cwd rank 与 corpus 原顺序稳定；空 query 先 rank/limit 后读 title，非空 query 在 id/cwd/log-backed title 上匹配后 rank/limit。title batch 使用独立的一次持久化 list observation、最多 4 个 persisted inspect worker，单个 title 失败降级为 session id，批次取消整体失败；显式 `limit=0` 和非 safe integer 拒绝。动态 Cordis facade 保留 `SessionReferenceError.name/code` 并接入真实 AbortSignal context。

projection 只保留 current surface 上 direct user、compaction checkpoint 和 assistant text，排除 shadowed message、plugin/injected context、tool result、reasoning 与 chunk；多 text block 保留 join 分隔。每个 source 独立使用完整 UTF-8 budget，先删除最旧的非 checkpoint/非最新消息，再对最长文本 head-tail 截断并记录精确 omitted bytes；fixed envelope 放不下则整条 preparation 失败。model-visible JSON 与 `JSON.stringify` 对齐：只把 literal `<` 改为 `\\u003c`，不额外转义 `>`/`&`/U+2028/U+2029，预算使用同一序列化结果。

direct user message 与其 aggregated reference context 在同一 durable admission 中按 `[direct, context]` 紧邻落盘；queued inbox 重放保留 snapshot，source 后续 mutation、compaction、detach 或删除不会改变 target replay。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'TestSessionReference|TestPromptSessionReferences|TestPromptRejectsMalformedCanonicalSessionMention|TestDynamicCordisNativeSpillAndReferencesFromJavaScript|TestReferenceDiscoveryRemotesUseRealWorkspaceAndSessions' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- vet ./...`

## 已完成：Cordis runtime/inspect

上游入口：`packages/extensions/tool-cordis/src/*`、`packages/extensions/cordis-host-runner/src/*`、vendored Cordis effect/child lifecycle，以及两组 package tests。

Go 入口：`internal/harness/dynamic.go`、`dynamic_runner.go`、`dynamic_services.go`、`dynamic_inspect.go`、`dynamic_tools.go` 和 Agent request projection。

验收：Inspect Provider manifest 保持 Host-first/Client-latest directory、schema 验证、first-valid Client resolution 和取消；Host Service/Event 查询执行 live/catalog exact join、类型闭包和 caller tool visibility。`cordis_inspect_self` 按无 ID、plugin ID、plugin+package 三层返回：Plugin/Package 均保持创建顺序，summary 不泄漏 source/latest diagnostics，Package 层只返回存在的源码 half，并携带 Host/Client status、provides、waiting、handlers、error 和 render failure。

动态运行时补齐异步 effect setup 汇合、逆序 cleanup Promise 等待、public disposer single-shot 和 unloading-time effect rejection；stop/update/undefine 在返回前完成 teardown。missing inject 保持声明顺序。失败 update 保留 old current 与 target next，随后允许 `run` current 回滚并清除 next；`nextPackageId` 不再误作 transition lock。Client activation failure 仅在 `startedHere`/request ownership 成立时 retract，并真实清理 Host service/tool/listener，而 attached page failure不停止既有 Run。Plugin inventory 按创建顺序，Package identity 永不覆盖；Go 的动态 Plugin/Package/Run/Approval identity mint 已与上游 Registry 的 `1` 起始、后置递增语义对齐。

direct user message 中独立边界的 `@pluginId` 按首次出现去重；只在组合 Cordis tools 的 session 每个 step 临时追加 instruction context，不写 durable log、不扫描 plugin/tool/assistant message、不携带源码。reference base 按 next、current、latest-defined 优先，missing/cross-session/removed reference 注入不可用说明。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(Cordis|DynamicCordis)' -count=1 -timeout=180s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -count=1 -timeout=240s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- vet ./...`
- `git diff --check`

本轮继续补齐 skill 与搜索高影响语义：skill filesystem 同时发现目录 bundle 与平铺 `<name>.md`，严格按上游解析 YAML frontmatter 的 invocation 策略，保留 `modelInvocable`/`userInvocable` 四态并对旧驼峰键和非法布尔值 fail-closed；tool-skill 在每个模型请求末尾注入 direct user `/name` 手势命中的 user-invocable skill，目录只展示 model-invocable skill。`readStreamMinSize` 不再只是配置值，而是触发有界逐行读取并计算完整文件 digest；fs-search 的 glob/grep 结果附带可回放的 `presentationMeta`，按 `searchMetaMaxBytes` 保留结构化路径或按文件匹配分组并报告 total/truncated。

## 已完成：插件生态 Loader inventory 边界

上游入口：`packages/host/plugin-inventory/src/index.ts` 及其 `inventory.spec.ts`。原版每次读取 Loader 当前的非 group 条目，保留真实 `disabled`、`pending/loading/active/failed/unloading` Fiber 状态，并在条目移除或禁用后立即反映。

Go 入口：`cmd/dsh/profile.go:pluginInventoryEntries`、`cmd/dsh/main.go:engineConfig`、`cmd/dsh/profile_runtime.go:mountProfileRuntimePluginSet`、`internal/harness/harness.go` 的 live inventory registry，以及 `internal/harness/remote.go:remotePluginInventory`。

验收：profile composition 按 Loader entry 顺序保留 `id/name/group/disabled`，嵌套 group 使用与原版一致的 `parent:child` entry id，父 group 禁用会传递到子 entry；`!!js disabled` 使用与 profile 配置相同的运行时求值，group 行只参与结构、不进入 Remote。Engine 持有独立于 Client boot graph 的并发安全 live registry，`pluginInventory/list` 每次直接读取当前 snapshot，保留 entry id、module name、enabled 与 nullable phase；完整 replacement 可立即反映禁用、移除和顺序变化。外部 profile 插件整组挂载后读取其真实动态 Host run：缺失 inject 映射为 `pending`，被后续 provider 激活后映射为 `active`。未提供 Host composition metadata 的 library 调用保留旧 boot graph 兼容回退，但 CLI profile 不再把 Client 包清单当 Host inventory。专项测试同时发现并修复 `nil` profile args 被编码为 `null`、导致外部 web profile bootstrap 执行 `[...args]` 崩溃的问题。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'TestPluginInventoryUsesLiveHostEntries|TestTypertRemoteKnownUnsupportedDynamicMethodIsNot404|TestEngineConfigUsesActiveClientRoster|TestProfileRuntimePublishesPendingPluginInventoryPhase' -count=1 ./internal/harness ./cmd/dsh`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race -run 'TestPluginInventoryUsesLiveHostEntries|TestProfileRuntimePublishesPendingPluginInventoryPhase|TestDynamicCordis|TestGlobalDynamicCordis' -count=1 ./internal/harness ./cmd/dsh`

补充的运行时语义：profile 外部插件重载按 Loader `EntryGroup.update()` 的事务边界处理。更新失败会逆序恢复已变更插件，移除阶段收到 `OK=false` 或错误时会重建已经撤销的旧插件，并恢复旧顺序和 watcher 路径；创建失败会清理新增插件，清理错误与原始错误聚合返回。`DynamicCordisUndefine` 的非 OK receipt 不再被当作成功删除。

工具策略插件补齐：上游 `@deepseek-ai/dsh-tool-call-timeout-policy` 只是可选的 `tools/execute` 包装层，未挂载时不应强制 `ToolDefinition.timeoutMs`。Go 原先在 `executeToolRuntime` 中无条件执行声明超时，导致 profile 禁用该插件仍产生 `TOOL_TIMEOUT`。现以 `Config.ToolTimeoutPolicyEnabled`（库调用默认启用，profile 按 `timeout-policy` 行解析）控制所有已注册工具；运行时切换同步更新 Host/Agent 工具，回滚恢复旧策略。`TestToolTimeoutPolicyFollowsRuntimeConfig` 覆盖启用、禁用和原地切换。

Todo 插件配置补齐：上游 `@deepseek-ai/dsh-tool-todo` 要求显式 `allowParallelInProgress`，该值同时决定模型描述和多个 `in_progress` 项的校验。Go 原先固定允许并行且忽略 profile 配置；现由 composition 按插件名解析必填布尔值，写入 `Config.TodoAllowParallelInProgress`，运行时重载触发 Host tool 重建，单活跃策略返回与上游一致的结构化错误。新增 `TestRuntimeProfileMapsOptionalToolPolicies`、`TestRuntimeProfileRequiresTodoParallelPolicy` 与 `TestTodoWriteHonorsSingleActivePolicy`。

上游 `@deepseek-ai/dsh-skill-filesystem` 的 `customSkillDirs` 是 preset 作用域配置，不是固定的 preset 子目录约定。Go 的 preset runtime 现在编译该配置（包括 shipped `cordis` 的 `baseUrl` URL 形式），`skillRoots()` 在会话 preset 作用域加入这些目录，同时保留项目、用户和 preset 自带根目录的优先顺序及去重。

新增行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./cmd/dsh -run 'TestProfileRuntimeReconcile|TestProfileRuntimeMountCleansPartialStartup' -count=1`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'TestPresetSkillFilesystemCustomDirsAffectSessionDiscovery' -count=1`

## 已完成：插件生态 preset standing generation

上游入口：`packages/preset/agent-presets/src/index.ts`、`mount.ts` 及 `tests/mount.spec.ts`。原版按 preset composition 文件的 stamp 建立单一 standing generation；已加入的 agent 固定其 generation，后续文件编辑只影响新 agent，子 agent 通过 `composeFrom` 共享父 agent 的同一 generation，文件删除也不能使已加入的 generation 失效。

Go 入口：`internal/harness/harness.go`、`prompt.go`、`subagents.go`。此前 `runtimeForSession` 每次读取并解析当前 `agent.cordis.yml`，会让既有 session 在文件编辑后切换工具/提示词，也会让 child 重新解析父 preset。

验收：Engine 以 preset id + 文件 `mtime/size` 缓存编译后的 runtime generation；session 创建时固定 generation，后续运行复用该快照。父子创建链在父 preset 相同且父已有 generation 时直接继承，未找到父 generation 才建立当前文件的新 generation。编辑后新 session 使用新工具 roster，旧 session 与 child 保持旧 roster；已删除文件仍可服务已固定 session。回归覆盖 generation pin、child inheritance 和删除后的运行时读取。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'TestPresetRuntimeGenerationsStayPinnedAndChildrenInheritParent|TestPresetPromptAndWorkspaceInstructionsReachProvider|TestShippedPresetFiltersModelToolCatalog' -count=1 ./internal/harness`

## 已完成：attachments/read-image

上游入口：`packages/attachment/attachment/src/*`、`packages/attachment/attachment-local/src/*`、`packages/fs/tool-fs/src/read-image.ts`、`packages/acp/acp/src/content.ts`、`packages/mcp/mcp-client/src/tools.ts`，并逐项对照 admission、normalization、request-image、read-image、ACP 和 MCP tests。

Go 入口：`internal/harness/attachments.go`、`read_image.go`、`acp.go`、`mcp.go`、provider exact-model resolver 和 prompt/command admission。

验收：wire 图片先逐项执行 canonical base64 解码，再按原始提交字节执行批量 count/aggregate/media policy；全部成员完成完整解码、方向应用、单帧规范化、8-bit sRGB 转换、metadata stripping 和编码验证后，才按输入顺序提交。GIF admission 验证完整多帧容器并只规范化首帧；只有真实缩小时记录方向应用后的 `originalDimensions`。PNG/WebP alpha 按编码通道事实校验，灰度、CMYK、16-bit、EXIF/ICC 和动画输入不能错误走原字节直通。

request-image 对 in-budget normalized attachment 原字节直通；变换按 pixel/byte policy、颜色复杂度和 alpha 选择 PNG/WebP/JPEG，缓存命中重新校验 byte、dimension、depth、space 和 alpha compatibility。相同 variant 的并发调用共享底层转换，waiter 独立取消，最后 waiter 取消底层任务；Engine dispose 会取消并等待 inflight。Go 使用纯 Go lossless WebP fallback，输出字节和 Sharp 的有损候选不同，因此 transform identity 使用 `request-image-v4-go1`，避免与上游 `request-image-v4` 产生同 ID 异字节缓存或上传索引碰撞。

`read_image` 在任何 FS I/O 前按最新 `request/header`、再按 session model 解析精确 provider/model；advisory model list 缺失不能否定 exact route，解析故障和取消不能伪装成 text-only。工具保持 parallel-safe，按较紧的单图/单消息 byte cap 读取，并在 `tool/result` 前完成 durable commit、FS observation 和 image block materialization。

ACP 仅在默认 exact route 和 attachment policy 均明确支持时广告 image；wire 内容全部验证后，以已解码原始字节执行可取消批量 admission，caller-correctable image failure 返回 invalid params，storage/route verification failure 返回 internal。持久化后、enqueue 前再次读取最新 exact route；取消或路由变化只可能留下不可达内容寻址对象，不能晚提交 user message。MCP 保留 canonical raw value，图片批次原子 prepare/ordered commit；逐块 wire diagnostics 保持块顺序，route/admission/storage failure 投影为对应诊断，而工具调用取消仍按 abort 失败，不降级成普通图片诊断。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(Attachment|RequestImage|ReadImage|ImagePrompt|SharedRequestImage|ToolFSIncludesReadImage|MCPImage|ACP.*Image)' -count=1 -timeout=240s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -count=1 -timeout=300s`
- `make test`
- `make vet`
- `make verify-upstream`
- `make build-all`
- `file` 已确认 Linux amd64/arm64 为对应 ELF、Darwin amd64/arm64 为对应 Mach-O、Windows amd64/arm64 为对应 PE；非本机目标只完成真实交叉构建和格式检查，未宣称运行 smoke。
- `git diff --check`

## 已完成：Agent Team

上游入口：`packages/experimental/agent-team/src/{index,roster,mailbox,activity,lifecycle,journal,fold,invariant,session-message,task-board,task-graph,validation}.ts`、`packages/experimental/tool-agent-team/src/index.ts`，并逐项对照 `team.spec.ts`、`persistence.spec.ts`、`fold.spec.ts`、`invariant.spec.ts` 和 `tool-team.spec.ts` 的状态矩阵。

Go 入口：`internal/harness/agent_team.go`、`internal/harness/agent_team_tools.go`、`internal/harness/harness.go`、`internal/harness/prompt.go`、`internal/harness/model_tools.go`、`internal/harness/session_reference.go`、`internal/harness/workflow.go`。

验收：Team 身份按 exact live Session、父 Team owned suffix 和子会话第一条 `subagent/descriptor` 判定；普通 fork 成为新 Lead，provider child、stale/detached identity 和畸形父 Team 流不获得 Team authority。provisioning 先 durable reserve name，再创建 continuable child、checkpoint 初始 inbox，最后以 CAS 风格终结 active/failed；恢复只接受 parent、第一条 descriptor provider/mode 和初始 user receipt 全部匹配的 child，创建/恢复反向竞态及清理失败不会生成非法状态边。

mailbox 在根日志事务内完成取消检查、目标解析、容量/字节限制和 durable queue，然后释放根事务并进入 per-target FIFO dispatch；同目标 wakeup 串行，不同目标可并行，阻塞投递期间的新 admission 能看到 queued-minus-delivered 容量。quiet 不唤醒 inactive member，后续 wakeup 按 durable 顺序带出 backlog。target inbox/current history/active admission、冷恢复和异步 receipt observer 共同去重；target flush、Lead acknowledgement、inspection/continuation failure 均保留 queued，flush 后再次确认 live receipt，claimed admission 窗口不会重复插入。

task board 已覆盖 safe numeric id、deleted tombstone、active limit、CAS revision、owner/Lead 权限、claim/release/edit/dependencies/complete/reopen/reassign/delete、missing/duplicate/self/cycle blocker、write scope 规范化/重叠告警、readiness 和 deleted view。Team tools 仅对 Lead 与 durable roster member 暴露；与 legacy subagent controls 同名时按 session scope 选择 schema、执行和 `run_code` catalog，普通 subagent 保持 legacy 工具。完整共享 checkout policy、wait edge trigger/no-progress、typed cancellation 和关闭后 waiter admission 已对齐。

runtime disposal 先关闭 admission 和 waiters，取消并有界等待 admitted creations、dispatch/ack，再按 live child -> live parent -> durable roster 发现并 drain continuable children；只过滤直接或 cause-chain 的 `TEAM_DISPOSED`，保留 creation/dispatch/cleanup/discovery/drain 错误并聚合，timeout 使用稳定毫秒文本。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(FoldTeam|TeamService|AgentTeam|ContinuableModelSubagent|Subagent|PromptQueuesDistinctTurns)' -count=1 -timeout=300s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(FoldTeam|TeamService|AgentTeam|ContinuableModelSubagent|Subagent|PromptQueuesDistinctTurns)' -count=1 -timeout=300s`

## 当前工作项：subagent 其余语义

### 已完成：provider registry、descriptor、depth、assistant output

上游入口：`packages/subagent/subagent/src/{provider,descriptor,depth,output}.ts` 及其由 spawn、continuation 和 cold-resume 调用的生产链，并逐项对照相关 package tests。

Go 入口：`internal/harness/subagent_provider.go`、`subagent_descriptor.go`、`subagent_output.go`、`model_subagent_activation.go`、`subagents.go`、`agent_tools.go`。

验收：provider 保持注册顺序，注册发布 added，added listener 同步失败时只回滚同一注册代次并发布 contained removed；duplicate、missing provider、unsupported capability/schema 保持稳定结构化 code。启动前验证 maxDepth 和 object-root JSON Schema，one-shot 与 continuable 都携带从 durable log 第一条 descriptor 解析出的权威快照。当前版本 descriptor 对完整字段矩阵和 toolFilter 做严格校验，malformed 报错，unsupported version 不分类；后续伪造 descriptor 不覆盖第一条。cold resume 恢复 provider/model、persona、toolFilter，并清除上一 activation 的 maxTokens 等非持久 composition。delegation depth 以 durable depth 为单调下界并拒绝 safe-integer overflow。最终输出选择最后一个非空 assistant message 的完整 content blocks；只有不存在非空 message 时才拼接 text-delta，且 custom provider block 的未知 JSON 字段不会在持久化恢复后丢失。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(FinalAssistantOutput|Subagent|EngineSubagent|ContinuableModelSubagent|ModelSubagent)' -count=1 -timeout=300s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(FinalAssistantOutput|Subagent|EngineSubagent|ContinuableModelSubagent|ModelSubagent)' -count=1 -timeout=300s`

### 已完成：run settlement 与 continuation

上游入口：`packages/subagent/subagent/src/run-settlement.ts`、`continuation.ts`、`lifecycle.ts` 与 `packages/core/agent/src/consumed-work.ts`，并逐项对照 settlement、continuation、inheritance 和 host teardown tests。

Go 入口：`internal/harness/subagent_tools.go`、`model_subagent_activation.go`、`subagents.go`、`model_tools.go`、`acp.go`、`sdk.go`。

验收：one-shot 后台 settlement 在 completed 时只拼接 text blocks，aborted 映射 killed，其余 stop reason 映射 failed 且保留 provider diagnostic；无论 result 成败都执行 dispose，result 与 dispose 双重失败均保留。continuable followup 只接受 durable direct parent，caller cancellation 只拥有 admission 之前；human RPC interrupt 对 absent/disposal target 为 no-op，对 live foreign target拒绝，使用 user cause 且 park 未 claim inbox，ancestor tool 使用 parent cause。report 只接受 exact resident activation，区分 quiet/next-step，保留 subagent-report source，发送后不结束 child turn。

Activation terminal 使用 consumed-work 单遍 fold：只认进入 step 或 claim 后以非 completed 结束的 accounting turn；最后 accounting turn 之后被取消且未替换的 accepted inbox work 报 aborted，pre-step blocked/rejected 报 refusal，cold epoch 不复活旧回答。settlement 在 ownership release 前投递，teardown failure 覆盖为 error 并 withholding output；完整 assistant content blocks 原样进入 parent notice，不再压成 text。selected children、host scoped descendants 和 manager-wide drain 都先同步关闭全部目标 admission，再 top-down cancel、并行 child-first release，汇合调用共享同一 finish error；ACP/SDK 在释放 host session 前 drain 自己的 continuable descendant forest，其他 host tree 保持运行。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(DrainModelSubagentDescendants|DrainSubagent|ContinuableModelSubagent|ModelSubagent|SubagentReport|CancelAgent|AgentTeamClose|ACP|SDK|FinalAssistantOutput|SubagentProviderTool)' -count=1 -timeout=300s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(DrainModelSubagentDescendants|DrainSubagent|ContinuableModelSubagent|ModelSubagent|SubagentReport|CancelAgent|AgentTeamClose|ACP|SDK)' -count=1 -timeout=300s`

### 已完成：activation setup registry

上游入口：`packages/subagent/subagent/src/activation-setup-registry.ts` 及其完整 registry tests，并检查真实 consumer `tool-subagent-report`。

Go 入口：`internal/harness/subagent_activation_setup.go`、`model_subagent_activation.go`、`sdk.go`。

验收：contribution 按注册顺序同步安装到 unpublished child，transaction 在 `subagent/start` 前 commit；注册在 commit 前撤销会使 batch 以 `ACTIVATION_SETUP_REVOKED` 失败，自撤销 installer 的 escaped installation 会立即释放。later installer 失败时尝试回滚所有 earlier installation，同时保留 installer 原始错误。registration removal 与 child detach 双重所有权汇合到 exactly-once disposer；两条路径都幂等，保持 installation 顺序，并在尝试全部 disposer 后以 `ACTIVATION_SETUP_RELEASE_FAILED` 聚合 error/panic。fresh activation 与 cold resume 每个 residency epoch 独立安装；正常 settlement、rollback、selected/scoped/global drain 和 host detach 都释放 child installations，释放失败进入 Activation teardown outcome。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(SubagentActivationSetup|ContinuableSubagentSetup|ContinuableModelSubagent|ModelSubagent|DrainSubagent|ACP|SDK)' -count=1 -timeout=300s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(SubagentActivationSetup|ContinuableSubagentSetup|ContinuableModelSubagent|ModelSubagent|DrainSubagent|ACP|SDK)' -count=1 -timeout=300s`

### 已完成：list children、descendants 与 list_agents

上游入口：`packages/subagent/subagent/src/list-children.ts`、`packages/subagent/subagent/src/index.ts`、`packages/subagent/tool-subagent-control/src/list-agents.ts`，并逐项对照两组 package tests。

Go 入口：`internal/harness/subagent_listing.go`、`subagents.go`、`model_tools.go`、`rpc.go`。

验收：目录由 persistence header 与 attached live session 构成一次 live-preferred merge；Go 内部保留的 detached session 不再伪装为 live，且无 persistence 时不会把不可恢复 child 列成 inactive。direct children 只解释 durable `origin=subagent`，descendants 穿过 ordinary 与 one-shot 节点，以 createdAt/id 稳定 pre-order 迭代遍历并防止 root/cycle 重访。`hasChildren` 只读完整 corpus 的 origin-classified header，不 inspect 后代。

live child 只走 projection registry snapshot，任意 projection unit 异常按 child 降级 `corrupt`；无 identity 的 creation window 省略。cold child 先尝试完整 projection-cache row，只有合法 identity 且 `seq >= seedLength` 才采用；缓存缺失、null、malformed 或任意 unit 导致的派生行损坏都回退一次 authoritative inspect。inspect failure 为可重试 `unavailable`，enumeration lifecycle witness 不匹配或 detached fold 无 identity/失败为 `corrupt`。cold inspect 并发上限 4，完成顺序不改变输出顺序；取消在 list/inspect 前后保持，RPC 也使用请求 context。

`list_agents` 只投影 continuable child，one-shot 仍作为 descendants traversal node；status 按 resident/running 状态映射为 ready/idle/running，diagnostic 与 parent/depth 保持。空 caller 被拒绝，tool cancellation 传入 listing。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(ListModelAgents|ListAgents|SubagentListing|SubagentListRPC)' -count=1 -timeout=180s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(ListModelAgents|ListAgents|SubagentListing|SubagentListRPC)' -count=1 -timeout=180s`

### 已完成：in-process spawn/fork driver

上游入口：`packages/subagent/subagent-in-process-driver/src/*`、`subagent-spawn-in-process/src/index.ts`、`subagent-fork-in-process/src/index.ts`，并逐项对照 driver、inheritance、preset-inheritance、structured、spawn、fork 和 multi-provider tests。

Go 入口：`internal/harness/subagent_provider_in_process.go`、`model_tools.go`、`model_subagent_activation.go`、`subagent_structured_output.go`、`session_scoped_tools.go`、`agent.go`。

验收：child setup 是未发布事务，seed、delegated sandbox/approval、preset/model/depth、persona/toolFilter、structured runtime 和 descriptor hook 全部成功且 request context 仍有效后才发布；失败 child 不进入 session listing、host subscription 或持久化 corpus。spawn 不继承历史，fork 只截取最后一个 `turn/end` 之前的完整 prefix，当前开放 turn 不进入 seed；新策略事件追加在 seed 后并覆盖旧状态。parent route 缺失时接受显式 child route，maxTokens 默认继承并允许显式覆盖，durable depth 是单调下界且 safe-integer/maxDepth 在发布前校验。

one-shot descriptor 只在首个 prompt 成功 admission 后落盘，pre-step rejection 不留下伪造 identity。Go 为关闭 worker 启动前取消窗口，会在 `turn/start` 前同步写 claim splice；consumed-work fold 因此将紧邻的 pre-turn claim 归属到后续 turn，同时保持无后续 turn 不计入、completed 空 claim 不计入、rejected/error/aborted 计入。结果只读取 activation boundary 后的 assistant output 和 accounting turn；pre-step rejection 映射 refusal，max-tokens/error/aborted 保持，取消保留 partial output。published run 的 result fault 与 dispose fault 分离，dispose 并发幂等、等待 result 收敛后才 detach。

structured output 使用 child session-scoped tool/schema/instruction；有效 capture 成为 authoritative terminal，阻止同批后续调用和后续 model step，之前的调用仍可完成。invalid args 产生普通 error result 并允许 in-turn retry；capture 必须等 post-policy 和 enclosing `run_code` 成功后才 commit，失败 stage、call-id reuse 和 blocked replacement 均不能泄漏。completed 而未 capture 直接报 error，不自动 re-prompt；并发 child 各自持有 schema，dispose 后 registration 消失。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run 'Test(InProcess|FoldModelSubagentConsumedWork|ModelSubagentActivationTerminal)' -count=1 -timeout=180s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness -run 'Test(InProcess|FoldModelSubagentConsumedWork|ModelSubagentActivationTerminal)' -count=1 -timeout=180s`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -count=1 -timeout=300s`

### 已完成：shell-env、bash 参数与 PTY 取消 ownership

上游入口：`packages/shell/shell-env/src/index.ts`、`packages/shell/shell-env/tests/shell-env.spec.ts`、`packages/shell/tool-bash/src/index.ts`、`packages/shell/tool-bash/tests/tools.spec.ts`、`packages/terminal/terminal-bash/src/session.ts`。

Go 入口：`internal/harness/shell_env.go`、`subprocess_env.go`、`tools.go`、`terminal_pty_unix.go`、`terminal_pty_windows.go`、`dynamic_builtin_services.go`。

验收：普通 bash 每次前台/后台调用都重建 trusted `DSH_*` 环境，清除 ambient 值并注入绝对 `DSH_HOME`、`DSH_SHELL`、owner 的 `DSH_SESSION_ID` 和 JSONL 定位；动态 Cordis 提供 `ctx.shellEnv.register/collect/list`，注册时校验 contributor/key/description/重复所有权，收集时按名称排序并拒绝未声明或非字符串返回。bash command/description/timeoutMs 的值校验与上游一致；PTY send 取消在 signal/drain 完成前保留 active ownership，避免下一次 send 抢占同一会话。当前普通 bash 文本结果仍保留 Go 既有的 `[exit code: 0]`/`[exit code: -1]` 格式，已确认这是现有测试契约，与上游 renderer 的 clean-success/timeout marker 形态存在显式差异。

行为验证：

- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run 'Test(ScrubbedChildEnv|ShellEnvironmentForSession|BuiltinShellRejectsEmptyDescription|PersistentShell|LinuxPTY|Terminal)' -count=1 ./internal/harness`

### 进行中：web capability

继续按上游真实服务和调用链分组检查 web、shell/job、terminal、sandbox/approval。每组先列上游可观察契约，再定位 Go 生产调用链和行为测试；不能以存在同名工具或文件判定完成。

### 进行中：插件生态与 profile patch reload

本轮继续对齐插件配置语义：

- `@deepseek-ai/dsh-tool-fs` 的 `readLimit`、`readMaxLineLength`、`readMaxBytes`、`readStreamMinSize` 现在进入 preset runtime generation；session 工具 schema 与 read 执行均读取 pinned 值。
- `@deepseek-ai/dsh-tool-fs-search` 的必填 `sampleOverCapGlobResults` 及 `globMaxResults`、`grepMaxMatches`、`grepMaxLineBytes` 进入 pinned runtime；glob over-cap 页面按配置进行顶层轮询采样，grep 预览和保留条数按配置截断。
- `@deepseek-ai/dsh-tool-bash`/`tool-pwsh` 的 `enableRunInBackground` 现在影响 schema 和强制后台调用拒绝；persistent bash 的 `timeoutMs`、`maxOutputChars` 保持 session 固定。
- `@deepseek-ai/dsh-tool-skill` 的 `catalogDescriptionMaxLength` 现在固定到 preset generation；每个 session 在首个模型步骤前发布持久 `skill-catalog`，只在可见名称/描述变化时追加替换目录，空目录在已有目录后发布 tombstone。
- `@deepseek-ai/dsh-tool-fs-search` 的 `rawOutputMaxBytes` 与 `timeoutMs` 进入 session 工具运行时；超过原始输出上限返回 `SEARCH_RAW_OUTPUT_OVERFLOW`，搜索超时使用配置预算。
- `@deepseek-ai/dsh-tool-jobs` 的 `waitTimeoutMs`、`maxWaitTimeoutMs`、`completionDelivery`、`maxConsecutiveWakes` 已从 profile 映射到 Engine jobs 配置，保留 owner/wakeup 状态。
- `@deepseek-ai/dsh-compaction-tool-result-pruner` 的 `thresholdChars`、`headChars`、`tailChars` 现进入 preset pinned generation；会话裁剪只在该插件挂载时生效，未挂载 preset 不再误用 Host 全局 pruner 配置。
- `@deepseek-ai/dsh-tool-ralph` 已按 `packages/workflow/tool-ralph/src/index.ts` 对齐：`subagentProvider`、`maxRounds`、`maxHandoffChars`、`maxResultChars` 在 preset 加载时严格校验并固化到 session generation（插件默认分别为 `spawn`、`256`、`16384`、`16384`，standard preset 的 `maxRounds: 64` 仍作为部署 ceiling）。Ralph 执行不再直接 `CreateSubagent` 或把 schema 拼进 prompt，而是通过 `StartSubagent` provider registry 启动 one-shot child，要求 `OutputSchema` 且 `InheritsParentContext()==false`，等待 `SubagentCompleted` 的 `Structured` 结果并始终 `Dispose`；handoff/result 上限和严格报告 schema 校验保持在对应边界。
- `@deepseek-ai/dsh-tool-workflow` 与 `@deepseek-ai/dsh-workflow-worker-thread` 已按两个 package 的真实调用链对齐：`toolName`、`maxResultChars`、worker `provider`、`maxConcurrentAgents`、`maxTotalAgents`、`maxItemsPerCall`、`syncTimeoutMs`、`disposeGraceMs` 进入 preset pinned generation；custom `toolName` 同时替换 native/code tool catalog 和 prompt guidance。workflow `agent()` 不再直接创建 session 或把 schema 拼入文本，而是经配置的 `SubagentProvider` 启动 child，转发 provider/model `agentOptions`，structured call 读取并再次校验 `SubagentResult.Structured`，普通失败仍按 workflow hook 契约解析为 `null`。并发、总量、items 和同步 JS watchdog 使用 session generation 的限制。顶层 `tool-workflow/*` recorder 改为 run-scoped 旁路：任一 durable append 失败只停用该 run 的后续面板记录，不影响 workflow 执行与返回值。workflow/Ralph result cap 现按上游 JavaScript `string.length` 的 UTF-16 code units 截断，不再按 UTF-8 字节切坏多字节字符。
- `@deepseek-ai/dsh-compaction-basic` 已按 `packages/compaction/compaction-basic/src/{config,index,summarizer}.ts` 的真实服务和 hook 链对齐：插件是否挂载、`auto`、默认 policy、精确 `provider/model` override、retention 形式、summary provider/model pair、generation cap 和 retry ceiling 全部在 preset 加载时严格校验并固化到 session generation。自动 pressure 只读取最近一次 durable `request/header` 的 provider/model/system/tools；首个尚未路由的请求不提前压缩。pressure 的 context capacity 通过已注册 provider 的 exact resolver/Models 解析，不再限制为 DeepSeek route；摘要可以转发到独立 provider/model，summary event 记录实际路由和 cap。`auto:false` 只关闭 pressure/overflow hook，manual compaction seam 保留；未挂载 backend 的 preset 不自动压缩。`@deepseek-ai/dsh-command-compact` 也作为独立 Agent 插件控制 `/compact` catalog 与执行，command-only preset 在 composition load 时因缺 backend 被拒绝。`tokenMeter` 的 ownership 按 shipped preset 源码保留在 Host，不错误要求 Agent preset 重复挂载；preset 只决定该 agent 是否组合 compaction backend。
- Host-owned `@deepseek-ai/dsh-token-meter` 的 compaction gating 路径已按 `src/index.ts`、`surface-fold.ts`、`estimate.ts` 对齐：Go 新增 durable event replay measurement，维护最新 canonical request/header、step 起点、逐节点 surface token 和最新成功请求 anchor。assistant 的 provider output 从精确 `sourceEventSeqs` chunk 重组；provider usage 只有不低于完整启发式 anchor 时才采用，避免异常低 usage 低估压力。后续 append/prune/compaction replace 以有符号 surface delta 更新 anchor，因此 compaction 判定能立即观察 durable replacement，而不是重新从零估算或只看最后 usage。Header route/envelope 改变会使旧 usage anchor 失效并回退完整启发式计价。
- `@deepseek-ai/dsh-compaction-tool-result-pruner` 已继续按 `src/{config,index,invariant,types}.ts`、`tool-result-pruner.spec.ts` 和真实 Loader composition 对齐：字符预算只计 text Unicode code points，跨 text block 保留统一 head/tail，非 text block 保持原位；replacement 现在直接在 clone 后的原始 JSON block map 上只替换嵌套 `content`，不会再因转成 Go `ContentBlock` 丢失 core block、message 或 event 的未来扩展字段。候选来自一次 stable current-surface snapshot；每个 `compaction/prune` shadow-price 与单节点 replacement 在同一 session 临界区相邻提交，replacement 精确 cite 原 seq。后一 replacement 失败不回滚已提交 prefix，且已提交的 price event 保留；第二次 pass 幂等。preset config 拒绝未知字段、fractional/字符串整数和非法预算，同时接受数值上为整数的 YAML float。
- `@deepseek-ai/dsh-tool-fs-search` 已改为直接执行 `rg` 子进程并固定注入 `--no-config`，隔离环境配置注入；`grep` 严格校验 `--json` match 记录字段，启动失败、非法模式、原始输出超限和 malformed record 均映射到稳定 `SEARCH_*` 错误码。
- `@deepseek-ai/dsh-tool-fs-search` 进一步按 `src/{index,search-core,glob,grep,presentation,direct-call}.ts` 和真实 subprocess tests 对齐：preset 的全部 cap 使用严格整数解析，`graceMs` 遵守 `2147483647` 上限；显式空 `path/include` 与缺省值分离。glob 保留 `rg --sort=modified` 的 oldest-first canonical 顺序，不再反转；over-cap top-level sampling 按 upstream 的 group-flatten 顺序生成同一正文页和 meta 页。相对 rg 输出原样保留，只有 workdir 内绝对路径才相对化。stderr 改为保留诊断尾部并在截断时标记，取消在 wait 返回后再次检查。超限 glob/grep 在 post-policy 阶段从完整 canonical value 保存 `glob-results.txt` / 完整 previewed grep 结果，正文 locator 指向完整结果；存储不可用仍保留成功的 capped inline result，通用 final spill 继续独立处理最终正文预算。

行为验证：`TestPresetFilesystemConfigsArePinnedIntoSessionRuntime`、`TestPresetFilesystemSearchRejectsInvalidConfig`、`TestGlobToolKeepsCompleteMtimeSortedValue`、`TestGlobSamplingUsesSameGroupedPageForTextAndMeta`、`TestGrepToolKeepsCompleteValueAndBoundsPreview`、`TestSearchRejectsExplicitBlankOptionalArguments`、`TestSearchUsesRetainedStderrTailForClassification`、`TestSearchSpillKeepsCompleteCanonicalResult`、`TestPresetPersistentShellConfigIsPinnedIntoSessionRuntime`、`TestPresetEditorConfigIsPinnedIntoSessionRuntime`、`TestPresetRalphConfigIsPinnedIntoSessionRuntime`、`TestRalphUsesConfiguredFreshProviderAndCaps`、`TestRalphRejectsProviderWithoutFreshStructuredCapabilities`、`TestRalphDefensivelyRejectsOversizedAndMalformedStructuredReports`、`TestPresetWorkflowConfigsArePinnedIntoSessionRuntime`、`TestWorkflowUsesPinnedToolNameProviderAndLimits`、`TestWorkflowHonorsConfiguredConcurrencyAndSyncTimeout`、`TestWorkflowRecordingFailureDoesNotAffectExecution`、`TestWorkflowResultCapsUseUTF16WithoutSplittingUTF8`、`TestPresetCompactionConfigIsPinnedIntoSessionRuntime`、`TestPresetCompactionRejectsInvalidConfig`、`TestPresetWithoutCompactionBasicDoesNotAutoCompact`、`TestAutomaticPressureWaitsForDurableRoutedRequest`、`TestPresetCompactionAutoFalseKeepsManualService`、`TestPresetCompactionExactPolicyRoutesSummaryAndUsesArbitraryCapacity`、`TestPresetCompactCommandRequiresBackend`、`TestTokenMeasurementUsesProviderAnchorAndSignedSurfaceDelta`、`TestTokenMeasurementDoesNotTrustUsageBelowHeuristicAnchor`、`TestPresetToolResultPrunerRejectsInvalidConfig`、`TestToolResultPrunerPreservesRichBlockAndEventData`、`TestToolResultPrunerKeepsCommittedPrefixWhenLaterReplacementFails`、`TestToolResultPrunerCommitsPriceAndReplacementAdjacently`，以及完整 `internal/harness`、`cmd/dsh` 测试套件。

本轮继续完成外部 Loader/Cordis 运行时语义：对照 `vendor/loader/src/index.ts`、`vendor/loader/src/config/entry.ts`、`vendor/cordis/src/fiber.ts`、`vendor/cordis/src/service.ts` 和 `vendor/cordis/src/utils.ts`，Go profile runtime 现在使用同样的 default/`__esModule` 解包、Standard Schema 同步校验与错误聚合、普通函数/class 的 `isConstructor` 判定、`cordis.initHooks`/`cordis.init` 生命周期，以及 `ctx.effect` 返回 disposer 的 fiber ownership。class 构造器通过受限 `reflect.provide` 桥接，不暴露 `root/registry/fiber` 等动态沙箱内部；服务跨插件传递时保留实例原型链方法，并在 provider unload 时级联失效。

模块解析对照 Node package exports 行为补齐：`resolveProfilePluginPath` 现在尊重 exports 的声明顺序条件，支持 wildcard subpath，拒绝未导出的 sealed subpath 和越过包根目录的 target；profile `inject` 接受 Loader 支持的数组、单值和对象依赖键（对象 intercept value 当前只用于依赖等待，尚未模拟完整 intercept 合并）。动态 effect 支持单 disposer、Promise、同步 iterable 与异步 iterable，按 upstream 逆序等待 cleanup。`@deepseek-ai/dsh-tools` 等完整 npm runtime（ToolRuntime/事件/真实 Cordis registry）仍不是可由这组轻量桥接证明的范围，下一阶段需单独建立模块替换或完整运行时兼容层。

行为验证：`TestProfileRuntimeUsesLoaderDefaultExportUnwrapSemantics`、`TestProfileRuntimeValidatesAndNormalizesPluginConfig`、`TestProfileRuntimeConstructsClassPluginsAndRunsInit`、`TestProfileRuntimeSupportsCordisServiceImports`、`TestProfileRuntimeCollectsPluginApplyDisposer`、`TestProfileRuntimeReconcileRollsBackFailedUpdate`、`TestProfileRuntimeMountCleansPartialStartup`、`TestResolveProfilePluginPathUsesNodeExportsSemantics`、`TestProfileEntryInjectAcceptsObjectDependencyMap`、`TestDynamicCordisEffectAwaitsAsyncSetupAndCleanup`、`TestDynamicCordisEffectConsumesSyncAndAsyncIterables`，以及 profile runtime/动态生命周期专项套件。

本轮明确保留的后续范围：fs-search 仍使用 Go 发行环境的 `rg`/`DSH_RIPGREP_PATH`，尚未像 npm upstream 一样随包携带平台 ripgrep；若干低频插件配置尚未接入 profile 映射。以上不影响本轮已覆盖的 pinned runtime 行为，不能据此宣称完整 npm 包交付等价。

本轮 skill-filesystem 语义补齐：skill root 顺序调整为 project-dsh/project-agents、preset/custom、user、bundled，重复名称保留高优先级首个定义；发现目录同时跟随目录型和扁平 Markdown 型符号链接；保留不存在 root 以匹配上游后续 watcher 创建感知。`ReadDir` 的非缺失错误现在标记为 incomplete，skill catalog 不会因一次权限/I/O 故障发布缩水结果，保留 last-good catalog。

行为验证：`TestSkillDiscoveryUsesProjectBeforeCustomAndUserRoots`、`TestSkillDiscoveryFollowsSymlinkedSkillEntries`，以及 `internal/harness`、`cmd/dsh` 全套测试、`gen_public_facade -check`、`go vet ./...`、`git diff --check`。

进一步补齐 preset scoped skill provider 语义：`skill-filesystem` 的 `includeDefaultRoots`、`dshHome`、`agentsHome`、`bundledSkillDir` 进入 pinned generation；未挂载 `skill-filesystem` 的 preset 不再自动发现项目/user skill，只保留 host bundled root。新增 `TestPresetSkillFilesystemCanDisableDefaultRootsAndSelectBundledRoot` 与 `TestPresetWithoutSkillFilesystemDoesNotDiscoverProjectSkills`。

上游入口：`apps/cli/src/profile-boot.ts`、`packages/boot/app-boot/src/index.ts` 的 `watchUserPatches`，以及 `packages/boot/app-boot/tests/user-patches.spec.ts`、`apps/cli/tests/built-bin.e2e.ts`。

差异根因：原版对同一个 Cordis Loader tree 调用 `entry.update`，因此已有 Host context、service、session/agent、订阅和 app 参数快照继续存在；Go 原实现通过 `harness.New(nextConfig)` 替换 Engine，旧树中的内存状态和外部 profile plugin run 会一起被关闭。

本轮最小修复：`cmd/dsh/web_runtime.go` 与 `profile_runtime.go` 复用现有 Engine，外部插件按 entry id 执行新增、`DynamicCordisRun(..., "update")` 和 `DynamicCordisUndefine`；`internal/harness/harness.go` 增加 `ApplyRuntimeConfig`，原地更新配置投影、subagent provider roster 和 Host plugin inventory。非法 patch 仍不提交候选配置，保留 last-good 状态。

行为验证：`TestProfilePatchWatchReloadsWebAndKeepsLastGoodEngine` 验证同一 Engine、已有 session、model/provider 更新、失败回滚；`TestStandaloneCustomProfileMJSArgsHelpAndHMR` 验证自定义 profile 的插件更新与退出；两项 `-race` 均通过。后续范围仍包括：更新阶段运行中 plugin 的故障注入回滚、删除后旧 fiber disposal 顺序，以及 LSP/MCP/telemetry 等非插件配置项的更深原地重配置覆盖。

本轮插件生态核对补充：Host inventory gating 只适用于 Host composition 所拥有的实现（shell、fs、jobs、web、schedule、LSP 等）；`tool-ask-user`、`tool-cordis`、persistent shell、preset 内的 terminal/tool rows 属于 Agent composition，Go 继续全局注册后由 `runtimeForSession` 的 preset tool roster 过滤，不能误用 Host inventory 将其整体删掉。shell executor 另按 `runtime.GOOS` 只接受对应的 `tool-bash` 或 `tool-pwsh`，避免非原生 dialect 暴露。新增 `TestHostPluginInventoryGatesModelPluginRegistration` 覆盖 Host 禁用与平台 shell 语义。

同时统一 `disabled: !!js ...` 的求值：`activePluginEntries`、`enabled` 与 Host inventory 现在都基于同一运行时表达式结果，避免 Client roster/配置派生把平台条件禁用项误判为 active。

继续补齐 Host reload：`ApplyRuntimeConfig` 现在维护内建 Host tool 到插件模块的 ownership 表，配置替换时会按 Loader disposal 语义移除旧 Host 工具，再依据新 inventory/config 重建 builtin、web、workflow、schedule、LSP tool；Agent-preset 全局工具与动态 Cordis 工具不参与 reset。Session-title provider 和 telemetry coordinator 也在配置变化时取消旧工作、等待释放并发布新实例。新增 `TestApplyRuntimeConfigReconcilesDisabledHostTools`、`TestApplyRuntimeConfigReconcilesSessionTitleProvider`、`TestApplyRuntimeConfigReplacesTelemetryCoordinator`。

本轮继续按源码 ownership 修正 terminal：上游 `@deepseek-ai/dsh-terminal`、`@deepseek-ai/dsh-terminal-bash` 与 `@deepseek-ai/dsh-tool-terminal` 只出现在 minimal 的 `persistent-shell` Agent isolate group，base Host composition 不提供 `tool-terminal`。Go 的 terminal schemas 因而不再受 Host inventory gating；仍由 `runtimeForSession` 的 preset roster 决定可见性，并在 `TerminalTool` 配置实际变化时重建全局 schema。`TestAgentOwnedTerminalToolsDoNotDependOnHostInventory` 验证未挂载 terminal preset 不泄漏、挂载后即使 Host inventory 无 terminal 也可见。

插件 inventory 语义补齐：上游 `packages/host/plugin-inventory/src/index.ts` 的 `list()` 明确跳过 `entry.options.group`，只返回非 group Loader entry；Go `composition.pluginInventoryEntries()` 现在保持 group 参与祖先禁用传播和子 ID 构造，但不再发布 group 行本身，避免客户端观察到原版不存在的 group 条目。`TestCompositionPluginInventoryUsesLoaderOrderAndEffectiveDisablement` 已按该行为验证。

Runtime reload 回滚强化：`ApplyRuntimeConfig` 保存上一代 subagent provider roster/order，并在 provider 注册失败时恢复原顺序；回滚阶段的 MCP、telemetry、title、Host registry 和 provider restore 错误使用 `errors.Join` 保留，不再静默丢弃。各阶段完整故障注入和旧 fiber/工具/连接可观察状态验证列为后续范围，不作为本轮 alpha.1 已验证证据。

本轮继续对齐并验证：

- `ApplyRuntimeConfig` 的 MCP、telemetry、title、Host、terminal 失败路径统一进入固定顺序回滚入口；恢复 config/inventory、MCP、coordinator/provider、Host/terminal registry 和 subagent provider roster 时聚合恢复错误。
- profile 外部插件 reconcile 对齐 Loader `EntryGroup.update()`：变更、创建、删除分别记录可逆操作；候选更新失败、stale 删除返回非 OK、后续删除失败时逆序恢复旧插件、旧顺序和 watcher 路径；部分启动失败会逆序清理已启动插件和 bootstrap。
- preset standing generation 现在覆盖 `@deepseek-ai/dsh-skill-filesystem` 的 `customSkillDirs`，包括 `cordis` preset 的 `baseUrl` URL 表达式；session skill discovery 使用已固定 generation，preset 文件修改不会改变旧 session 的 skill 根目录。
- `@deepseek-ai/dsh-tool-subagent` 接受上游 `maxDepth: provider-managed` 哨兵；subagent provider 列表保持 composition 注册顺序。

行为验证：

- `GOTMPDIR=$PWD/.tmp-go go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness ./cmd/dsh -count=1 -timeout=600s`
- `GOTMPDIR=$PWD/.tmp-go go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -race ./internal/harness ./cmd/dsh -run 'Test(ProfileRuntime|PresetSkillFilesystem|ApplyRuntimeConfig|CompositionPluginInventory|ExternalPluginEntries|ProfileSubagentProvidersReachEngine|ProductSubagentToolProfileConfig|SubagentToolProfileMapsPersonaAndToolFilter)' -count=1 -timeout=360s`
- `go run ./scripts/gen_public_facade -check && go vet ./...`
- `git diff --check`

嵌套插件身份补齐：Loader 的嵌套 entry root fiber 使用 `parent:child` 形式的稳定 ID；Go `activePluginEntries()` 现以同样的限定 ID 构造 external profile runtime，避免不同 group 中同名 child 在 reconcile map 中互相覆盖，并由 `TestExternalPluginEntriesUseQualifiedNestedIDs` 固定行为。

外部 profile runtime 生命周期补齐：上游 Loader 的 `EntryTree.entries()` 对嵌套 group 使用深度优先顺序，inventory 与 profile plugin 卸载均需保持该顺序；Go inventory 已改为递归深度优先且跳过 group 行，`profileRuntimeMount` 保存 plugin order 后按旧顺序删除。候选 profile plugin 在 define/start 任一阶段失败时会清理已登记的失败项、已启动兄弟项和 bootstrap，避免 reload 后残留 `state=failed` 的 dynamic plugin。新增 `TestProfileRuntimeMountCleansPartialStartup`。

telemetry sink 比较增加不可比较动态值保护：接口 concrete type 不是 `Comparable` 时按 changed 处理，避免 runtime reload 在接口相等比较处 panic。MCP profile reconcile 增加 desired server 名称排序，并在新增连接失败时汇总新增连接关闭与旧连接恢复错误；测试覆盖 unchanged identity 保留、changed replacement、删除撤销工具及失败后的 last-good 恢复。相关测试：`TestEquivalentTelemetryConfigHandlesNonComparableSink`、`TestMCPReconcilePreservesIdentityReplacesAndRollsBack`。

## 已完成：dsh-v0.1.2-alpha.1 workspace inventory

上游基线：tag `dsh-v0.1.2-alpha.1`，commit `cd5ef8148158c3a752a658978873241fdf8e2bbc`，检查时上游工作树干净。

执行记录：

1. 按 alpha.1 的 `pnpm-workspace.yaml` 展开全部 workspace root，并以 pinned commit 的 `git ls-tree` blob identity 重新计算每个非 frontend root 的文件数和 path+blob SHA-256；旧 rc.2 清单不能用增量刷新，因为 alpha.1 新增 root 不存在于旧 rows。
2. 用完整 264-root 结果替换 `upstream_inventory.tsv`，保留五列 TSV、frontend 的 `-` 占位和每个 runtime row 的 Go surface 校验。
3. 将 `deepseek-llm-api-extensions`、`plugin-package-inventory-deepseek`、`session-log-deepseek` 标记为 `ported`：Go 已实现 extension registry、并发 prepare、冲突检测、JSON snapshot、取消、幂等 acceptance、内建 `dsh_session_log`/`dsh_plugin_packages` 注册，以及 DeepSeek HTTP 2xx 后、SSE 消费前的 acceptance；专项测试覆盖 registry lifecycle、取消和增量 session-log acceptance。
4. 将 `webhook` 与 `webhook-github` 标记为 `support`：两者是 alpha.1 新增的可选 CLI example/profile 集成，未进入 base、web、headless、acp、sdk 或 sdk-minimal shipped bundle；该分类不声明 Go 已移植 webhook rule runtime 或 GitHub ingress adapter，Go surface 保持 `-`。
5. `packages/sdk/client` 调整为 `hybrid`：Go 对应面是 `internal/harness/sdk.go` 提供的 server/runtime，不等价于上游公开 TypeScript client，因此不再将该包整体声称为 `ported`。最终分类为 `frontend=41`、`hybrid=9`、`ported=178`、`replaced=23`、`support=13`，共 264 root；verified baseline 中无 `missing` 或 `partial`。

行为验证：

- `go test ./internal/harness -run '^TestUpstreamContractWorkspaceInventory$' -count=1`
- `make verify-upstream-inventory`
- `go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test ./internal/harness -run '^TestDeepSeek(ExtensionRegistryLifecycle|ExtensionRegistryCancellationStopsWaiting|SessionLogIncrementalAcceptance)' -count=1`

## alpha.1 最终收口与边界

最终验证记录（2026-08-29）：

1. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-target go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -run '^TestDeepSeekPluginPackageInventory' -count=1 ./internal/harness`：PASS。
2. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-sdk-test go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -count=1 -run '^TestJSONRPCSDK' ./internal/harness`：PASS；对应 `-race`：PASS。
3. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-target go run ./scripts/with_runtime_assets --upstream deepseek-harness -- test -count=1 -run 'Test(DeepSeek|UpstreamInventory)' ./internal/harness`：PASS；其中 SDK client overclaim 断言确认 `hybrid=9`、`ported=178`。
4. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-final-test make test`：PASS。
5. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-final-vet make vet`：PASS；`GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-final-standalone make smoke-standalone`：PASS。
6. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-final-race make race`：PASS（harness 及脚本包全部通过）。
7. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-final-verify make verify-upstream`：PASS，包含 pinned checkout、runtime assets、pi-ai catalog、dynamic inspect catalog、workspace inventory、public facade 和 upstream contract。
8. `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha1-clean-archive make smoke-clean-archive`：PASS；归档在仓外临时目录中完成全套测试和三类命令构建。
9. `go run ./scripts/gen_public_facade -check` 与 `git diff --check`：PASS；上游 `deepseek-harness` 工作树仍干净并固定在 `cd5ef8148158c3a752a658978873241fdf8e2bbc`。

收口边界：`packages/sdk/client` 仅以 `hybrid` 记录 Go 的 SDK server/runtime，对应的公开 TypeScript client 尚未移植；SDK notification 已按上游 transport 只接受字符串/数字 id。httptest provider wire 只证明请求扩展、SSE 时序和错误边界，不证明真实外部 provider 或 credential 可用。完整 npm/Cordis `ToolRuntime`/registry、对象 intercept 合并、平台 ripgrep 随包交付、低频插件 profile 配置、webhook ingress/rule runtime，以及更深的 reload 故障注入覆盖均保留为后续范围。现有 Go bash 成功文本格式和纯 Go WebP 编码差异已在对应章节明确，不伪称与上游字节级一致。

## dsh-v0.1.2-alpha.3 跟进计划与逐步记录

当前状态：alpha.3 生产差异审计、实现和最终全量门禁均已完成；Remote Stream/Event 与 first-parent 补项已通过定向普通、race 及完整 Makefile 门禁，未验证范围继续明确列为边界，不计入完成声明。

验收原则：本节只记录当前 pinned alpha.3 源码、Go 生产调用链和已实际执行的验证。真实外部 provider、完整 npm/Cordis runtime、connection recovery 和前端-only 行为不标记完成；工作区已有的无关 dirty changes 保留。

首轮同步记录：

1. [已完成] 固定上游基线并检查来源：`upstream.lock` 已锁定 tag `dsh-v0.1.2-alpha.3`、commit `dd6322d604e00eec1ba5e0c8541159906a21094a`；`deepseek-harness` 当前 `HEAD` 与 tag 精确对应，工作树干净。
2. [已完成] 盘点 alpha.3 workspace：按 pinned `pnpm-workspace.yaml` 重建 `upstream_inventory.tsv`，共 267 个 root，分类为 `frontend=42`、`hybrid=9`、`ported=178`、`replaced=26`、`support=12`；移除已删除的 agent-spine demo/SQLite persistence root，新增 `ui-schedule`、`session-turn-outline`、`util/deque`、`util/time`、`util/values`。`TestUpstreamContractWorkspaceInventory` 在更新模式下已成功重生成，分类断言已更新。
3. [已完成] 对齐 alpha.3 profile 合同：sdk-minimal 的显式 agent kernel、invariant rows、session projection/title、timer/tools 等 rows，以及 web 的 `session-turn-outline` 已写入合同测试；profile bundles/reload policy 与上游 `PROFILE_TEMPLATES` 保持一致。
4. [已完成] 对齐 forwarded remote event 合同：上游已从 `SESSION_CONTROLLER_REMOTE_EVENTS` 运行时展开改为 `API_REMOTE_FORWARDED_EVENTS` 显式 event entries；合同测试改为读取显式条目，并保持 Go allowlist 集合校验。
5. [已完成] 清理版本漂移断言：CLI/config/runtime bridge、插件 provenance fixture 和相关注释已从 alpha.1 更新到 alpha.3；测试 fixture 对当前 `harness.Version()` 使用动态断言的地方不再硬编码旧版本。
6. [已完成] 已执行最小验证：`GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha3-contract go test -count=1 -run '^TestUpstreamContract(ForwardedRemoteEvents|DefaultProfiles|WorkspaceInventory)$' ./internal/harness`（其中 WorkspaceInventory 在更新模式下运行）；以及版本/fixture 定向测试（`cmd/dsh` 与 `internal/harness`）均通过。
7. [已完成] 汇合并验证 projection/read-image/session-turn-outline 等 Go 行为实现：已有 projection cell 前进时逐项校验 `observedSeq+1` 到目标 seq 的连续事件并只在 apply 成功后推进 watermark；首次 lazy build 保持上游 `buildCell` 的传入 slice-order 语义，不额外把稀疏 fork/suffix fixture 当作损坏；late-cell 历史重放失败不会缓存半成品，wire view 使用 previous/current 双槽抑制不可见变化；`read_image` 支持无扩展名和 content-addressed 路径，在格式检测前执行 byte cap，并将截断签名归一为可诊断的 malformed-image 错误。专项 wrapper 测试与 `-race` 定向测试均通过。
8. [已完成] Remote Stream/Event、DeepSeek endpoint guidance 和 first-parent 补项合入当前工作树后，已重新执行 `make verify-upstream test vet race smoke-standalone smoke-clean-archive`、`git diff --check` 和 pinned upstream 状态检查，全部通过；旧结果只作为历史记录。

alpha.3 关键源码变化与当前 Go 对齐证据：

- `packages/bundle/sdk-minimal/cordis.patch.yml` 删除 agent-spine demo，改为显式 session/agent/tool kernel 与四类 invariant；Go profile contract 已同步这些 rows。该项不等价于完整 npm/Cordis runtime 对等，需依赖后续 runtime 门禁。
- `packages/bundle/web-app/cordis.patch.yml` 新增 `session-turn-outline` 和 disabled `ui-schedule` client row；Go inventory/contract 已记录，前端行为仍属于 frontend-only deferred scope。
- `packages/api/remotes/src/remote-events.ts` 改为显式列出 session-controller events；Go forwarded allowlist 集合已通过定向合同测试。
- `packages/api/gateway` 的 `/api/remote.mux` 与 `$events` 已补入 Go：四类逻辑 stream 共用 WebSocket carrier；Remote waterfall 对 active Clients 使用同一 `eventId` fan-out，首个 result/rejected 胜出并只取消仍持有 delivery 的其他 Client，`next` 仅移除当前 delivery，全部 active delivery 均返回 `next` 后才回落，重复或已结算结果为幂等 no-op。Remote Stream 组在 runtime-assets wrapper 下普通测试及 `-race -count=20` 均通过。
- `host/session-added` 的 Remote 摘要改为按 ID 构造单条 `SessionSummary`，不再为每个新 session 扫描并投影全表；3000 层 lineage 定向 race 从此前约 7 分 52 秒超时路径降至 2.301 秒并通过，完整 `internal/harness` race 用时 239.427 秒并通过。
- `#3238` DeepSeek web search 在请求实际派发后的 transport、HTTP、decode 和空结果错误中附带实际 endpoint，并给出独立 search 配置恢复路径；credential/config 等派发前错误不伪造 endpoint guidance。
- `packages/session/session-turn-outline` 的 Go projection/registry 改动来自共享工作区并行实现，已通过 projection 定向测试、完整 race 和 clean-archive；这证明当前 Go 调用链与回归边界，不宣称完整 npm/Cordis runtime 字节级等价。

历史验收记录：新增补项前，完整 `make race`、`make smoke-standalone` 和 `make smoke-clean-archive` 曾通过。一次 standalone 初始失败仅因传入的外部 `GOTMPDIR` 目录不存在，创建目录后重跑通过，非代码失败；当前最终声明以本轮重新执行的门禁为准。

保留边界：上游已删除 `session-persistence-sqlite`，Go 同步删除其实现、公开 facade 和专属测试；`storage-sqlite`、`session-query-sqlite` 与其 `modernc.org/sqlite` 依赖属于不同能力，继续保留。Go Remote mux server 的 Ping/Pong grace 和 socket-close stream cancellation 已覆盖；Client generation 自动重连、stalled-host UI 状态、长生命周期后台唤醒和前端-only `ui-schedule` 交互仍待后续专项验证。完整 npm/Cordis registry、真实外部 provider/credential、平台 ripgrep 随包交付，以及上述未覆盖的专项行为仍不作为本轮已证实结果。

2026-09-01 门禁复核：`GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha3-final2 make verify-upstream`、`make test`、`make vet`、`make race`、`make smoke-standalone` 和 `make smoke-clean-archive` 均通过；catalog、inventory、public facade 与 `git diff --check` 也通过。第一次完整 `make race` 仅出现一次 `TestACPImagesUseExactDefaultAndLatestSessionRoutes` 的 2 秒等待超时，定向 `-race` 连跑 20 次及随后完整 `make race` 均通过，未观察到稳定复现。复核后本地 dirty 文件集合未增加，上游 checkout 仍为 `dd6322d604e00eec1ba5e0c8541159906a21094a` 且工作树干净。

### 2026-09-01 first-parent 完成性审计

审计范围：alpha.1 `cd5ef8148158c3a752a658978873241fdf8e2bbc` 到 alpha.3 `dd6322d604e00eec1ba5e0c8541159906a21094a` 共 351 个 commit、2200 个 changed file；文件数仅用于定位，完成判断按 first-parent 合入项的上游入口、Go 生产入口、状态/错误/持久化语义和行为测试逐项给证据。

PR 矩阵：

- `#2742/#2774/#3065` projection migration、mandatory seam、Schedule web projection：通用 registry 连续序列、view identity 和 turn outline 已覆盖；`ScheduleEnabled` 时注册 `schedule` key/state version 1，wire 返回 active records，reload reconcile 与 checkpoint replay 已有定向测试。
- `#3238` web search endpoint guidance：Go 仅在请求已派发后附带实际 DeepSeek search endpoint、UI 配置路径、环境变量和 profile key；派发前 credential/config 错误保持原分类，定向 provider error 测试已覆盖。
- `#3279` Windows absent probe：上游优化的是 TypeScript JSONL `exists()` 在 POSIX ENOENT 后不再额外 stat parent；Go `JSONLSessionStore` 没有该逐候选 probe，按 `filepath.WalkDir` 枚举并直接打开 header，因此不存在对应重复 stat 路径，无需补代码。
- `#3292` linear stream queues：上游把六处 JavaScript `Array.shift()` 队列换为 circular Deque；Go Remote/session/workspace stream 通过 channel 直接收发，出队为 O(1)，没有 array head move 路径，无需引入第二套 deque。
- `#3293` Gateway Remote 与 RemoteError：所有 wire error 先经 `canonicalRPCErrorCode` 归一为 namespaced code，`details` 保持 object；`/api/remote.mux`、四类逻辑 stream、Remote Event fan-out/settlement/cancellation/replay 与多 Client delivery ownership 已落到 Go。内部兼容调用点仍可使用旧短 code，但不会泄漏到 wire。
- `#3305/#3384` connection recovery/stalled hosts：Go server 已覆盖 heartbeat 两次宽限、Pong reset、失联终止和 socket close 取消全部逻辑 stream；Client generation 自动重开及 stalled-host UI 状态仍作为明确边界。
- `#3316` preset composition/plugin inventory：`pluginInventory.list()` 同时返回 Host Loader entries 和每个 agent preset 的 composition rows；attached preset 使用最新 live generation，cold preset 解析当前文件，broken reason 保留。Remote authoring 按首个 user root 复制完整目录（含 skills/assets）、解引用 symlink、重写 metadata、收紧权限，并固定 not-found/invalid/read-only details。
- `#3325` ignorable external events：unknown event 仅在 `ignorable=true` 时接受，已覆盖。
- `#3208/#3277` steer/follow-up images、extension-less `read_image`：durable image admission、queued steer 内容保留、无扩展名格式检测和 byte-cap-before-sniff 已覆盖；continuable/materialized subagent image capability disposal race 在 `-race -count=20` 下通过。
- `#3128` remove agent-spine：profile/inventory 已移除对应 root 与 rows，已覆盖。
- `#3339` JSONL-only session persistence：已删除 `session_persistence_sqlite*`、`SQLiteSessionStore` 公开 facade 和专属测试；默认/恢复路径只使用 JSONL。独立的 `storage-sqlite`、`session-query-sqlite` 及 `modernc.org/sqlite` 保留。
- `#3328` turn outline：projection、registry、wire view 和定向测试已覆盖。
- `#3319` runtime dependency ownership：上游行为目标是减少 npm peer relay，同时保留 Cordis/constructor/module-state identity；Go 为静态单体注册，没有 npm 安装布局、重复包实例或 peer relay。现有结构化 error/code、注册 token 和 engine-owned registry 已承担相应运行时身份边界，因此该项记录为无对应生产改动，不按 import/package.json 移动伪造 Go 机制。

完成性审计执行计划：

1. [已完成] Schedule projection：复用 `foldScheduleEvents`，按 `Config.ScheduleEnabled` 注册 key `schedule`、stateVersion 1；active records、checkpoint replay 和 reload reconcile 均有定向测试。
2. [已完成] RemoteError：wire 统一输出 namespaced `{code,message,details}`；preset、workspace、directory、subagent 和 gateway 生产路径已覆盖稳定 details，旧短 code 仅作为内部兼容输入。
3. [已完成] preset composition/authoring：inventory 覆盖 live generation 与 cold file；copy/delete/read/list/select Remote endpoints、完整目录复制、可写根、权限和错误 shape 已对齐。
4. [已完成] JSONL-only：删除旧 session persistence SQLite surface，保留独立 storage/query SQLite 能力，并验证 JSONL 默认、恢复和查询路径。
5. [已完成] Gateway Remote Stream/Event：HTTP upgrade、四类 logical stream、heartbeat、关闭取消、同一 event fan-out、first-result cancellation、all-next 与重复 result no-op 已由定向普通及 race 测试覆盖。
6. [已完成] first-parent 性能/包装补项：`#3279` 无对应 Go probe，`#3292` 已由 channel O(1) 出队覆盖，`#3319` 属 npm package ownership 且 Go 无 peer relay；均记录证据而不添加无效抽象。
7. [边界保留] Client generation 自动 recovery/stalled-host、完整 npm/Cordis runtime、前端-only `ui-schedule` 和真实外部 provider/credential 未由本轮 Go 测试证明，不纳入完成声明。
8. [已完成] 最终门禁：已重跑 `make verify-upstream test vet race smoke-standalone smoke-clean-archive`、生成物检查和 `git diff --check`，并确认 upstream 干净、现有 dirty 集合未被误覆盖。

2026-09-02 本轮收口状态：使用仓外 `TMPDIR=/home/xxnuo/.cache/dsh-go-alpha3-tmp` 与 `GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha3-final2` 执行 `make verify-upstream test vet race smoke-standalone smoke-clean-archive`，退出码为 0；普通测试 `internal/harness` 用时 50.538 秒，完整 race 用时 239.427 秒，clean archive 中 `internal/harness` 用时 47.308 秒。catalog、inventory、public facade、standalone embedded runtime、archive build/test 和 `git diff --check` 均通过；上游仍固定在 `dd6322d604e00eec1ba5e0c8541159906a21094a` / `dsh-v0.1.2-alpha.3` 且工作树干净，本地 dirty 路径集合未增加，无残留测试进程。alpha.3 本轮收口完成。

## dsh-v0.1.2-alpha.4 同步（2026-09-02）

基线：嵌套且唯一可信的 `./deepseek-harness` 已位于 `4e84901e6471b79ec0338099867ebb4606d12bb5` / `dsh-v0.1.2-alpha.4`，工作树干净；Go 主仓库保留 alpha.3 的既有未提交移植结果，不回退或覆盖。alpha.3 到 alpha.4 的 first-parent 范围包含 12 个合入项、2371 个 changed file；文件数只用于定位，完成判断继续依据上游入口、Go 生产调用链、状态/错误/持久化/事件语义和行为测试。

执行计划：

1. [已完成] 固定基线并建立 first-parent 审计范围：确认 alpha.3 `dd6322d604e00eec1ba5e0c8541159906a21094a`、alpha.4 `4e84901e6471b79ec0338099867ebb4606d12bb5`、嵌套上游 clean，并记录现有 Go dirty 集合。
2. [已完成] 逐项审计 `#2907` session log read API、`#1148` experimental Python code runtime、`#3367` invariant ownership cleanup、`#3250` steer service、`#3403` model-discovery profile headers、`#3382` default web-fetch、`#3411` smooth corners、`#3346` event-seq/log-offset brands、`#3415` turn preview、`#3418` PTC 文档、`#3391` conversation performance、`#3425` PTC workflow omission；完成性矩阵见下文。
3. [已完成] 更新 `upstream.lock`、266-root 上游 inventory、runtime/client assets、动态 catalog、public facade 和版本/合同断言；生成物均来自固定 alpha.4 上游。inventory 分类为 `frontend=42`、`hybrid=9`、`ported=176`、`replaced=26`、`support=13`。
4. [已完成] 实现 Session 显式 log read/seq-offset 边界、相邻 Agent 双向 steer 消息、model discovery profile headers/credential、默认 web-fetch 和 PTC preset roster，并加入行为测试；上游删除的 `tool-subagent-report`、report prompt/config 与仅为它服务的 activation setup registry 已从生产和 public facade 删除。
5. [已完成] 先以显式退出码捕获完整门禁失败，修复 19 个普通测试暴露的 alpha.4 迁移遗漏并对同一失败集执行普通及 `-race` 回归；随后重跑 `make verify-upstream test vet race smoke-standalone smoke-clean-archive`，最终退出码为 0。
6. [已完成] 已回填 first-parent 完成性矩阵、失败修复链、保留边界、最终命令与耗时，并完成 `git diff --check`、上游 clean/pin、生成物和残留进程终态检查。

first-parent 完成性矩阵：

1. `#2907` session log read API：Go `Session` 增加 `Seq()`、常量时间 `EventAt(SessionSeq)`、显式 `[from,to)` 的 `SnapshotEvents(SessionLogOffset...)`；快照会分离事件 data/surface/source slice，负数、倒序和越界位置被拒绝。`SessionStore.ReadFrom` 改用 `SessionLogOffset`。
2. `#1148` experimental Python code runtime：上游 Python 资产移动至 `packages/experimental/code-runtime-python/py`，Go 的 runtime asset 同步脚本与测试同步改路；Go 已有 Python runtime 生产实现，本轮不把未挂载的上游 experimental composition 声称为默认启用。
3. `#3367` invariant ownership cleanup：删除的是上游 package-owned invariant 源与陈旧 package surface；Go 保留集中式运行时 invariant 实现，只更新 inventory/catalog，不做行为删除。
4. `#3250` steer service：`send_message` schema 改为 `agent_id`，支持 parent→direct continuable child 与 resident child→direct live parent；消息使用 `agent-message/relay/senderSessionId` 与固定前缀，running/idle 均走 next-step steer，self、sibling、非 resident、stale/one-shot 路由拒绝；child message 与 settlement 共用 FIFO next-step 队列。
5. `#3403` model discovery headers：草稿 discovery 继承已配置 profile headers；草稿 key 缺失时惰性解析已存 credential，草稿 key 覆盖 profile Authorization；profile header 名和值在配置边界验证。
6. `#3382` SDK/base default web fetch：`DefaultConfig` 默认 `WebFetchProvider=http` 且 `FetchEnabled=true`，embedded base/standard/cordis/PTC profile 均验证 `web_fetch` 可见。
7. `#3411` smooth corners：仅 CSS/client 资产，随官方 frontend assets 更新；无 Go 后端生产语义。
8. `#3346` event seq/log offset brands：新增不同 Go named types `SessionSeq`/`SessionLogOffset`，`Event.Seq` 使用事件身份类型；逻辑 header 使用 `isSeeded`，Session/inspection 单独携带精确 `InheritedEventCount`。v0 JSONL 仍用可选 `seedLength`：字段缺失表示 unseeded，显式 `0` 表示 seeded-empty。
9. `#3415` turn rail preview：仅 client UI/CSS/tests，随官方 frontend assets 更新；既有 Go turn-outline projection 不额外声称 preview UI 行为。
10. `#3418` PTC Cloudflare 文档：docs-only，无 Go 生产改动。
11. `#3391` conversation performance：client list/render/search 性能改动，随官方 frontend assets 更新；Go 继续使用既有 session summary/search 生产路径，不按前端实现细节重写。
12. `#3425` PTC disable workflow：PTC runtime 隐藏 `workflow`，保留 workflow worker thread 供 `ralph` 使用；standard/cordis 继续暴露 workflow，定向 profile 测试覆盖三种 preset。

逐步失败与修复记录：

1. 第一次 `make sync-runtime-assets` 失败于旧路径 `packages/code-runtime/code-runtime-python/py` 不存在；确认 upstream move 后改为 `packages/experimental/code-runtime-python/py`，同步与检查通过。
2. 第一次 `make verify-upstream` 在 `TestUpstreamContractDefaultProfiles` 失败，指出 Go 合同仍包含上游已删除的 `tool-subagent-report`；完整删除 report 配置/工具/prompt/public API 与 activation setup 后门禁通过。
3. Session named seq 类型首次编译暴露所有 int/seq 混用位置；逐处在日志身份与数组位置边界显式转换，`go test ./internal/harness ./cmd/dsh -run '^$'`、Session JSONL/上游 packed fixture 测试及完整普通测试均通过。
4. 2026-09-02 已通过：`make verify-upstream`；`make test vet`（`internal/harness` 及全仓普通测试通过）；alpha.4 语义定向 `-race`（`internal/harness` 4.827 秒，`cmd/dsh` 1.101 秒）。
5. 第一次用独立退出码文件执行完整组合门禁（`/home/xxnuo/.cache/dsh-alpha4-final.KRT422`）以退出码 2 结束，普通测试共暴露 19 个失败：旧 `subagent_id` 断言、Python `LogMessage` mirror 缺 `open/truncated`、动态 Session/Agent 仍使用 `seedLength` 且未接收 `inheritedEventCount`、named seq 在 projection/search/outline 的 `any`/数组边界未转为位置整数，以及 PTC 仍期待 `workflow`。逐项修复后，同一失败集合普通测试和完整 `-race` 定向测试均通过。
6. 最终门禁使用仓外 `TMPDIR=/home/xxnuo/.cache/dsh-go-alpha4-tmp`、`GOTMPDIR=/home/xxnuo/.cache/dsh-go-alpha4-gotmp`，在 `/home/xxnuo/.cache/dsh-alpha4-final2.pdkM92` 捕获命令、日志和退出码；`make verify-upstream test vet race smoke-standalone smoke-clean-archive` 退出码为 0。普通测试 `internal/harness` 用时 51.672 秒；race 的根包、`cmd/dsh`、`internal/harness` 分别用时 39.044、16.040、254.477 秒；standalone smoke 用时 3.725 秒；clean archive 的根包、`cmd/dsh`、`internal/harness` 分别用时 2.274、13.101、47.444 秒，随后 archive build 通过。

保留边界：experimental Python composition 只同步其运行时资产，不声称已成为 Go 默认 preset；`#3411` smooth corners 与 `#3415` turn rail preview 只由固定上游的官方 frontend 资产覆盖，本轮未另做浏览器像素级验收；`#3418` 为文档专属变更。最终 `git diff --check` 和生成物检查通过，嵌套上游仍干净并固定在 `4e84901e6471b79ec0338099867ebb4606d12bb5` / `dsh-v0.1.2-alpha.4`，inventory 为 266 个分类 root，未发现 report 旧生产符号、alpha.3 旧锁定值或残留门禁进程。alpha.4 本轮收口完成。
