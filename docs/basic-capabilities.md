# 六项基本能力补齐与验收

范围调整日期：2026-09-13。用户已确认六项全部纳入 [Gateway v0.1 范围](scope-v0.1.md#六项基本能力gateway-范围内)，作为必须交付能力。当前六项基础范围实现与验收通过，最后完整回归为 **1827 passed**；逐项证据、失败修正及保留限制见[最终核对](six-capability-final-audit.md)。范围登记与验收分别记录，不代表整个生产版 v0.1 已完成。
Prompt 按 [ADR-0148](adr/0148-gateway-owned-basic-prompt-management.md) 纳入 Gateway。

| 能力 | 当前状态 | 补齐后的验收门槛 |
| --- | --- | --- |
| 自有协议 | 同步文本、Gateway 流事件、HTTP/TCP 与 Docker 传输已验证；真实 DeepSeek 同步/流式已验证 | 请求、结果、Usage、错误和流事件使用 Gateway 类型；供应商 DTO 不进入应用层；错误不泄露上游正文；未实现能力明确拒绝。 |
| Provider 抽象 | 两个独立兼容协议 Provider 已通过 API/Docker 契约；真实 DeepSeek 已验证 | 同一逻辑 Alias 下至少两个不同候选通过同一契约测试；换候选不改变调用方请求；实际模型保留可观测性，不能伪装成从未切换；供应商专有字段在 Adapter 内处理。 |
| Prompt 资产 | 已实现存储、权限、发布及同步调用；专项、Docker/DeepSeek 已验证 | 创建、读取、不可变版本、显式发布和受限变量渲染；调用锁定具体版本；缺少变量、非法模板、越权及超限在 Provider I/O 前失败；已有 messages 路径不回归；日志不含模板/变量/渲染正文。 |
| 重试与 fallback | 同步双候选 API/Docker 通过；默认真实配置仍单候选 | 故障注入覆盖可重试、仅 fallback、必须失败和 uncertain；能力不匹配候选不能执行；遵守预算/截止时间/取消；不以“都能聊天”声称质量等价；降级必须显式披露，不能静默牺牲能力。 |
| 流式边界 | ASGI/数据库、真实 TCP、排空及 Docker 恢复/故障已验证；默认入口与 DeepSeek 流式已验证 | 首个对外 delta 前允许合规恢复；提交后失败发明确错误并关闭，不拼接新模型输出、不发成功 DONE；一个不可变终态；覆盖断连、取消、超时、乱序及缺失终态。 |
| Trace 与可观测性 | W3C/业务 ID、Agent 声明、Gateway/Adapter 阶段计时、OTLP 三信号与 Prometheus 已部署；四场景完整链路通过，最后全量结果见最终核对 | request_id/call_id/trace_id 关联；记录实际模型、每次 Attempt、Token、费用完整性与计价版本；按传播策略处理 Trace 上下文及生成 spans；按明确时钟测量阶段延迟；缺失费用保持 unknown/partial，不能写零冒充完整。 |

## 实施依赖顺序

当前结论以[最终逐项核对](six-capability-final-audit.md)为准。以下日期小节按实施先后保留历史证据，其中“待部署/未完成”由后续验收记录更新，不作为当前状态。FR-105 不要求 Gateway 自建观测存储平台；FR-333 要求无显式允许策略时禁止上游传播，当前保持禁止，不以未实现的远程扩展冒充已交付能力。

1. 明确 Prompt 领域、外部 DTO、权限、版本/发布事务、渲染与指纹边界；保持原 messages 请求可用。
2. 实现 Prompt 资产及版本锁定，并补齐请求身份/Trace 关联，为后续调用提供可检索证据。
3. 补齐共享 Provider 协议和双候选故障验收；同一请求的 Prompt/参数不随 retry/fallback 改变。
4. 接通 Provider 事件、SSE 提交点和失败/取消终态，接入相同生命周期及持久化约束。
5. 完成 Token/Cost/Latency/Trace 对应关系和 Docker/API 验收，逐项记录证据后才能标记完成。

Prompt 管理 API 不得直接把模板引擎或数据库对象引入 Model Adapter。新增模板相关身份不能绕过现有租户隔离，
也不能让模板发布隐式发布模型配置。具体事务模型需在实现前明确，不从历史已废弃设计恢复默认。

## 验收证据要求

- 自动化：协议、纯策略、Adapter 契约、数据库事务、并发发布/调用、HTTP/SSE 故障与隐私测试。
- 集成：Docker 启动、显式配置/Prompt 发布、固定版本调用、流式成功与中断、双候选恢复。
- 真实 Provider：已授权的小规模调用证明可用性；故障分支采用确定性测试，不靠付费请求碰运气。
- 状态：HTTP 200 不等于有正文，数据库时间戳差值不等于分阶段 Latency，Trace ID 不等于分布式 Trace 已实现。

## 2026-09-13 Prompt 实现证据

- 规则/API/权限/发布事务/指纹契约见 [模板契约](prompt-template-contract.md)，增量迁移为 `0015_prompt_assets.sql`。统一 dev Gateway 已接入管理及模型路径。
- 全量 PostgreSQL 回归：1473 passed，4 项现有 Uvicorn 弃用警告。随后增强在途发布并发测试，`test_gateway_postgres.py` 再次通过；compileall 通过。
- 数据库测试覆盖跨租户 404、读写/发布/使用权限与过期、事务回滚、两个发布只有一个 CAS 成功、版本/历史 UPDATE/DELETE 拒绝。HTTP 测试覆盖字段/变量错误不泄密，未发布和缺变量不触发 Provider。
- Gateway 集成测试在 Provider 等待期间发布另一个版本，原调用仍提交原来的版本和消息；持久化来源/指纹 profile 可核对，重启不重放。
- Docker 镜像构建、独立迁移、服务恢复通过；Gateway/PostgreSQL healthy。运行 `deploy/prompt_smoke.py --invoke`，管理侧发布第二版本后调用固定第一版本，真实 DeepSeek 返回非空正文、stop，共 49 tokens（输入 37、输出 12，reasoning 10 是输出子集）。仅一次 Provider 请求。
- 真实调用 `3fb458eb-508d-47e6-bfc1-2e31ffc66cd4` 的数据库终态为 completed，actual_model 为 deepseek-flash，固定版本 `7892c119-bfff-4337-9e6e-8bea82410678`，与响应相符。资产保留在开发数据库便于复查，不包含用户私有内容。

## 2026-09-13 双候选恢复实现证据

- 修复统一启动器的 full-jitter 类型错误：原 random.uniform 返回 float，而策略要求整数毫秒，真实重试路径此前无法正确执行；现采用 randint，并由完整 API 的 retry/retry-fallback 场景覆盖。
- 统一启动器的已知凭证引用来自 Bootstrap 显式挂载清单，支持两个供应商使用独立材料。已失效的共享凭证仍阻止后续尝试，没有通过 fallback 绕过安全检查。
- 新增数据库驱动恢复摘要：成功 JSON 的 gateway.recovery 及成功/已结算失败的响应头提供 attempts/retries/fallback_used。只读已提交证据，不伪造预准入调用次数。规则见 [恢复契约](recovery-contract.md)。
- 全量回归 1484 passed，4 项现有 Uvicorn 弃用警告；随后新增 refusal 不切换与不合规后备候选不能执行场景，完整 `test_gateway_recovery.py` 13 项再次通过；compileall 通过。
- 13 项完整 API 场景覆盖 success、retry、fallback、retry→fallback、invalid_request、uncertain、协议错误、首选不匹配、总预算耗尽、Retry-After 超限、共享失效凭证、不匹配后备候选、refusal。每种都在首个 Provider 请求期间发布新模板；后续尝试保留原指令和参数，并核对持久化版本/尝试记录。
- 隔离 Docker 项目使用正常 Gateway、真实 PostgreSQL/DNS/egress/HTTP 传输和两个合成供应商。verify 退出 0，调用 `81dfd96b-f640-4ede-a80e-234986f94a01` 经 binding-a failed/retry → binding-a failed/advance → binding-b succeeded/stop，actual-secondary，attempts=3/retries=1/fallback_used=true。三个请求的意图摘要相同，无供应商私有字段透传，真实 Provider 调用数为 0。
- 可复现夹具与命令见 [Docker 恢复验收](../deploy/acceptance/README.md)。验收容器/临时数据库/合成材料卷已清理，记录留在本文件；默认 Gateway 已应用新镜像，healthy，直连 readyz=200，原配置/Prompt/数据卷保留。

六项总验收仍未完成：实际 SSE 流式提交/失败终态、完整 request/trace/spans 和阶段时延仍需继续补齐。不能用同步恢复验证代替流式或完整观测验收。

## 2026-09-13 流事件、Adapter 与执行器增量

- 新增 Gateway 自有 StreamDelta/StreamCompleted/StreamFailed 和 StreamingCompletionPort，不向应用层传供应商 SSE/JSON。失败事件保留已知 Usage，正文/推理内容不进入事件 repr。
- 兼容协议 Adapter 支持真正增量读取、严格 UTF-8/CRLF/SSE 字节边界和完整终态校验；缺失/重复终态、模型变化和乱序不会变成完成事件。取消关闭本次响应，不误关共享连接客户端。
- 应用层 StreamingAttemptExecutor 在首个业务增量交付后锁死 retry/fallback；可重试错误、拒绝增量后的错误、输出写入不确定都不能拼接另一个模型。接口保留失败 Usage，持久化 Adapter 待实现。
- 流式专项 55 项；非 PostgreSQL 全集 1348 passed、193 deselected；compileall 通过。没有付费调用、数据库迁移或 Docker 更新。
- 具体契约与剩余接线见 [流式契约](streaming-contract.md)。当前 HTTP 仍拒绝 stream=true，不能把上述模块测试当成 SSE 端到端验收完成。

## 2026-09-13 失败流结算与凭证接线证据

- 新增共享流式 Attempt journal，将失败流 Usage/actual_model 写入既有 Attempt、费用和终态事务；failed/uncertain 不变为 completed，结算拒绝与 Attempt 记录不一致的模型或 Token。数据库现有约束已允许这些观测，本轮没有新增迁移。
- 凭证租约 runtime 已支持流式调用，complete/stream 共用单次 Attempt 限制；成功、提前关闭和取消释放当前响应及 Adapter 引用，401/403 阻止复用同一失效凭证版本。未开放 HTTP streaming capability。
- 专项：80 passed（流式、凭证、协议及非数据库结算），另 `test_streaming_settlement.py` 13 passed（其中 8 项真实 PostgreSQL）。compileall 通过。新增测试总数 18。
- 全量：系统临时目录 ACL 导致首跑夹具错误；改用仓库内独占 basetemp 后，1558 passed、1 failed、4 项现有 Uvicorn 弃用警告。唯一失败为恢复矩阵 refusal 场景在模型调用前等待 Model 就绪超时；随后串行复跑整个 `test_gateway_recovery.py`，13 passed。该偶发启动问题尚未定位，不能据复跑通过宣称已修复，也不将此次全量写成全绿。
- 测试使用独立临时 PostgreSQL 和合成供应商响应；没有真实 Provider 调用，没有修改或重部署默认 Docker Gateway。测试容器及仅含测试材料的独占临时目录已清理。
- 剩余工作仍包括数据库首增量提交约束、HTTP SSE/背压/断连、取消后迟到观测，以及完整 Trace/阶段时延和最终 Docker 验收。

## 2026-09-13 数据库流提交约束与 Docker 升级

- 新增 `0016_stream_commit.sql`：不可变、无正文的首增量交付预留。提交必须属于 running 调用的最新未完成 Attempt；直接 SQL 也不能在提交后新建 Attempt、选择 retry/advance 或更换已知实际模型。提交不是客户端收件确认。
- 执行器强制要求 StreamCommitPort，首增量必须先等待事务提交，再交给输出端；提交故障不会发送业务增量，也不会重试。失败流共享结算测试已使用真实 PostgresStreamCommit。
- 全量 PostgreSQL 回归 1566 passed，4 项现有 Uvicorn 弃用警告；随后补入提交/新 Attempt 竞争及 0015 升级保留数据场景，`test_stream_commit.py` 9 passed。compileall 通过。本轮共新增 9 项测试。
- 默认 Docker 镜像构建成功，优雅停止 Gateway 后显式迁移成功，再启动新镜像。Gateway/PostgreSQL healthy，管理端 `http://127.0.0.1:8001/readyz` 为 200；schema 为 0016，流提交记录为 0。先前真实调用 `3fb458eb-508d-47e6-bfc1-2e31ffc66cd4` 仍 completed，其 Prompt 资产仍有两个版本。
- 未调用真实 Provider；`stream=true` 仍为 422，没有提前宣称 HTTP SSE 可用。临时 PostgreSQL 容器和仅含合成材料的 pytest 目录已清理；默认数据卷与凭证保留。
- 接下来仍需把 HTTP 流、背压、断连、取消/迟到观测接入同一持久化生命周期，再完成完整 Trace/阶段时延与最终端到端验收。

## 2026-09-13 流取消与迟到观测接线

- StreamingAttemptExecutor 已接入共享 AttemptExecutionControl：作用域内取消、逐 Attempt context、观测与 journal checkpoint 清理。Adapter 在完整合法帧后保存无正文的内存 Usage/实际模型快照；快照不等于成功结果。
- 新增 `0017_late_stream_observation.sql`，允许 failed/uncertain 迟到观测保留 Usage/实际模型，其余取消身份、追加不可变、幂等及 Token 子集约束继续执行；不得改写终态或重新计费。
- 真实 PostgreSQL + Adapter + AdmittedExecutionLifecycle 测试覆盖正文输出中、Usage 到达后、终态 journal 等待中取消。均关闭响应/释放控制，只有一个 Attempt，迟到元数据保留但无 completed 终态。另验证 0016 升级保留旧记录，以及重复追加失败观测不修改原账本。
- 本轮新增 5 项测试；流取消相关专项 74 passed（随后新增的观测/升级场景包含在全量中）；全量 1573 passed，4 项现有 Uvicorn 弃用警告；compileall 通过。
- Docker 已构建、优雅停机、显式迁移并恢复。schema 为 0017，Gateway/PostgreSQL healthy，管理端 readyz=200；先前真实调用仍 completed，原 Prompt 的两个版本保留。HTTP stream=true 仍 422，无真实 Provider 调用。
- 临时 PostgreSQL 和 pytest 合成材料目录已清理。HTTP 流/背压/断连尚未接入；deadline 到期后的部分观测仍需补齐；完整 Trace/阶段时延及最终六项验收仍未完成。

## 2026-09-13 deadline 观测与结算

- 执行控制在本地完全停止后保留未 checkpoint 的无正文观测；已 checkpoint 的事实不重复提供。完整服务 deadline 回调将快照传给原有中断结算事务，不新增表或迁移。
- 尚未完成的 Attempt 保留已知 Usage/实际模型并计算费用，仍标记 uncertain；调用最终为 failed/deadline_exceeded，不能把收到 finish/Usage 当成成功。不伪造主动取消预留；已有 Attempt checkpoint 保持不可变，不重复计费。数据缺失仍保持 unknown/unavailable。
- 新增 8 项测试：3 个流超时时点，3 个事务回滚/错误 Attempt/已有 checkpoint 场景，以及 2 个停止后快照可见性场景。专项 42 passed；完整服务/控制专项 39 passed；最终全量 1581 passed，4 项现有 Uvicorn 弃用警告；compileall 通过。中途发现并修正测试替身未接受 observation 新参数的问题，最终全量重新运行通过。
- 当前镜像的独立 Docker/API 双供应商恢复验收退出 0：call_id `55632347-3990-484b-8994-48376c89d774`，actual-secondary，3 Attempts/1 retry/fallback=true，三个请求意图一致，真实 Provider 调用为 0。该验收仍为同步恢复，不能代替流式 TCP 验收。
- 默认 Docker 已更新镜像，Gateway/PostgreSQL healthy、管理端 readyz=200，schema 仍为 0017。stream=true 仍 422；本轮没有真实 Provider 调用。独立验收项目、合成材料卷、临时 PostgreSQL 和 pytest 目录已清理，默认数据卷保留。
- 六项仍未整体完成：下一步接入 HTTP SSE/背压/断连及流式路由与指纹，之后完成完整 Trace/阶段时延和最终端到端验收。

## 2026-09-13 流式路由、指纹与完整后端

- TextInvocationQuery 增加严格配对的 stream/output port；输出端不进入 repr 或指纹。流式请求/执行使用 stream-text-v1，固定 Prompt 版本使用 prompt-stream-text-v1；同步黄金摘要不变。
- 静态路由按请求要求 streaming，能力未声明的候选不能执行。完整后端已选择 StreamingAttemptExecutor，并接入原有 runtime/circuit journal、首增量数据库提交、取消/deadline 控制及共享终态结算，不另建账务路径。
- 7 个真实 PostgreSQL 后端场景通过：成功、首增量前重试、拒绝、流中缺终态、取消、deadline、不支持 streaming。首增量时流提交已持久化、Invocation 尚无终态；Fingerprint 密钥已释放，退出后容量/凭证资源回收。
- 新增 14 项测试；指纹/路由/原执行专项 45 passed，后端专项 7 passed；全量 1595 passed，4 项现有 Uvicorn 弃用警告；compileall 通过。集成夹具最初缺少已验证请求字节数，补齐后重新运行专项及全量通过。
- 本轮无真实 Provider 调用、数据库迁移或默认 Docker 镜像更新；独立临时 PostgreSQL 和 pytest 材料已清理。当前统一 Gateway 的 HTTP stream=true 和配置 streaming 发布门槛仍未开放。
- 六项仍未整体完成：下一步是 HTTP SSE/背压/断连、最终流式 TCP/Docker 验收，以及完整 Trace/阶段时延。

## 2026-09-13 HTTP 传输桥接模块

- 新增 ModelStreamResponse：逐帧背压、有限写入超时、单事件上限；输出前失败返回 JSON，输出后失败关闭且不发送成功 DONE，异常正文不透传。
- 断连、写入失败/超时及重复取消均等待调用任务清理；保存调用授权上下文。13 项直接 ASGI 测试覆盖上述边界，非 PostgreSQL 全集 1383 passed、225 deselected；compileall 通过。
- 本轮未运行数据库测试、真实 Provider 调用或 Docker 部署。桥接尚未接入公开模型路由和准入身份；HTTP 中间件背压、完整后端与真实 TCP/Docker 验收仍待补。具体边界见[流式契约](streaming-contract.md)。六项总验收仍未完成。

## 2026-09-13 HTTP 路由与完整后端接线

- 模型 API 工厂新增显式 enable_streaming 开关；流式查询复用准入、指纹、Prompt 渲染、路由及结算，准入后向输出端传递本地 call_id/accepted_at。同步及流式共用安全异常映射，Accept 区分 JSON/SSE，并正确匹配 text/*。
- 响应身份中间件改为原位 ASGI 发送包装，不引入额外任务/缓冲。完整路由测试验证授权上下文、背压、稳定调用身份和提交前后错误；失败不发送成功 DONE。
- 修正传输失败的取消顺序：只取消外层调用，不直接取消增量确认对象；让原生命周期先处理持久化取消，再终止受保护执行任务。新增专项验证子任务不能在外层预留前退出。
- 新增 9 项 API 测试、7 项真实 PostgreSQL HTTP 入口场景及 1 项取消顺序测试。数据库场景覆盖成功、首增量前重试、拒绝、缺失终态、取消、deadline、能力不支持；核对原账务终态、Attempt 数与资源释放。新测试最初错误期待能力不支持为 503，按既有分类规则修正为 422/unsupported_capability，随后全量通过。
- 全量回归 1624 passed、4 项现有 Uvicorn 弃用警告；全量启动后新增的取消顺序测试另与桥接/API 专项一起运行，23 passed。compileall 通过。以上使用合成供应商和独立测试数据库，没有真实 Provider 调用或数据库迁移。
- 默认启动器及 Docker 未开启流式，也未更新镜像；仍需真实 TCP/Docker 的中断、恢复、排空和固定 Prompt 流式验收，再开放能力。完整 Trace/spans/阶段时延仍待补，六项总验收未完成。
- 本轮临时 PostgreSQL 容器及独占 pytest 目录已清理，合成测试数据已丢弃；默认数据卷和凭证保留。

## 2026-09-13 真实调用方 TCP 与流式停机排空

- 新增真实 Uvicorn/TCP → 模型 API → PostgreSQL 流式验收 7 场景：成功、首增量前重试、拒绝、缺终态、输出前断连、输出后断连、deadline。上游仍为可控 MockTransport，不代表容器内供应商 HTTP/DNS/egress 链路。
- Provider 在首增量后等待客户端放行；客户端读到数据时断言首增量提交已持久化而 Invocation 尚无终态，证明没有整段缓冲。断连后等待后台所有权释放，再核对 cancelled、唯一终态和凭证/容量资源回收。
- 拥有执行租约的 app/server 工厂接入显式 enable_streaming。新增 2 个流式真实 TCP 排空场景：期限内成功可发 DONE，排空到期为 shutdown_drain_expired，不发成功 DONE；服务停止后其他 Owner 可重新获得租约。默认开关仍关闭。
- TCP 专项 7 passed；包含原同步场景的真实服务排空专项 4 passed。Uvicorn 的现有弃用警告在新增启动场景中同样出现。默认 Docker 未更新，未调用真实 Provider；完整 Docker、双候选流式 fallback、固定 Prompt 及完整 Trace/阶段时延仍需继续验证和补齐。
- 全量回归 1634 passed、13 项同源 Uvicorn 弃用警告，compileall 通过。本轮新增测试共 9 项；没有新增数据库迁移。
- 临时 PostgreSQL 与本轮独占 pytest 目录已清理，合成数据已丢弃；默认 Gateway 容器、数据卷和凭证未改动。

## 2026-09-13 流式双候选、固定 Prompt 与 Docker 恢复

- 统一启动器提供 --enable-streaming，API 与配置发布/Active 校验使用同一开关，未传仍关闭。既有单次生命周期和结算路径不变。
- 同步/流式共 26 个恢复矩阵场景通过：每种都在首个 Attempt 期间发布新 Prompt，所有实际尝试仍使用原渲染指令及参数；覆盖 retry/fallback、能力不匹配、共享失效凭证、预算、uncertain、Retry-After 和拒绝。流式成功恢复摘要位于最终已结算帧，不能要求响应头预知总尝试数。
- 当前源码镜像构建成功。隔离 Docker 使用正常迁移、配置发布、DNS/egress、凭证和真实 HTTP 供应商夹具；同步与流式各三次 Attempt 成功，verify 退出 0，没有真实 Provider 调用。
- 同步 call_id 为 09d19ff0-3913-4ffe-80e4-2089ebd347fb，流式为 bcb2727a-755e-4e65-b7ff-da9c1d17aab9。数据库核对均 completed/actual-secondary，序列为 binding-a retry → binding-a advance → binding-b stop；仅流式存在第 3 Attempt 的流提交。两次均锁定版本 6e048232-94c4-4c1a-8886-e1e5e6649d83，流式调用前已发布另一个版本。
- 可复现命令及证据范围见[Docker 验收](../deploy/acceptance/README.md)。默认开发容器尚未更新；Docker 失败/断连组合、完整 Trace/spans/阶段时延及六项最终验收仍未完成。
- 本轮新增 13 项测试，全量 1647 passed、13 项同源 Uvicorn 弃用警告，compileall 通过。隔离 Docker 项目及合成卷、临时 PostgreSQL 和两个 pytest 目录已清理；默认容器、数据卷与凭证保留。

## 2026-09-13 Docker 失败流、断连及默认入口部署

- 扩展原隔离供应商夹具：主候选先输出 partial，再截断终态或等待连接关闭。验收脚本确认错误流只有显式 provider_protocol_error、没有成功 DONE，故障各一次 Attempt、不访问后备候选；断连时上游实际观察到 socket EOF。随后流式恢复仍成功，证明故障未耗尽可用资源。
- verify 退出 0。数据库核对 a19e2841-20dc-4725-af2e-8636617982ac 为 failed/provider_protocol_error/actual-primary，a70eb662-00ad-4d48-b40d-3e6fe61fa993 为 cancelled/context_cancelled；均为第 1 Attempt 流提交且仅一个终态。断连终态的 resolved_model 为空，已知实际模型保留在流提交记录中，后续可观测性汇总不得丢弃该事实。
- 同次同步恢复 ecca78e6-ce92-4d23-ba35-a6763b7a3a61 与流式恢复 edc8325d-a503-42d8-a369-82d5922d2632 均完成；后者锁定旧版本 2fe76c5f-321b-480e-b8f6-97ed08aab847。总共 8 次合成上游请求，真实 Provider 调用为 0。
- 默认 Compose 增加 --enable-streaming，已优雅停止并更新 Gateway，Gateway/PostgreSQL healthy，readyz=200。未修改已发布模型配置：DeepSeek Binding 仍未声明流式，探测明确返回 422/unsupported_capability；默认数据卷和凭证保留。
- 本轮没有改动应用执行代码或数据库迁移。非 PostgreSQL 回归 1393 passed、254 deselected，compileall 通过；数据库证据来自上述实际 Docker 故障验收，未声称本轮重跑全量。真实 DeepSeek 流式、完整 Trace/spans/阶段时延和六项最终验收仍待补齐。
- 独立 Docker 验收容器、网络和合成材料卷已清理，临时数据已丢弃；默认 Gateway 与持久化数据保留。

## 2026-09-13 真实 DeepSeek 流式验证

- 新增 deploy/stream_smoke.py：默认只读，显式 --publish-streaming 才发布能力，--invoke 才请求模型；不读取 Secret，无客户端自动重试。配置发布采用旧 Active 的 CAS，其他配置保持不变，失败流/缺 DONE/空正文/错误顺序不算成功。
- 通过管理 API 从 revision 1 发布 revision 2，仅把 deepseek-text 的 streaming 改为 true；导出比较确认其余内容一致（版本基准除外）。真实请求复用用户授权的现有本地凭证，由 Gateway 读取。
- call_id 28fea5f1-3f6f-462f-a964-7d2830fb53d0，HTTP SSE 非空正文、stop/DONE，实际模型 deepseek-flash；1 Attempt、0 retry、无 fallback。37 输入、15 输出、总计 52 tokens，reasoning 13 是输出子集。数据库确认 completed、revision 2、第 1 Attempt 流提交。费用 partial/estimated/USD，total_cost 未知，未写零。
- 客户端首增量约 1.905 秒，总耗时约 1.964 秒；这是客户端单次观测，不证明服务端阶段 Latency 或完整 Trace 已实现。readyz=200，已有 Prompt 和数据未清理；新增 revision/调用记录保留供复查。
- 脚本专项 6 passed；非 PostgreSQL 回归 1399 passed、254 deselected，compileall 通过。首次提权回归受系统临时目录权限影响出现 64 个夹具错误，普通环境重跑通过；未声称该环境权限问题已修复或本轮重跑数据库全量。
- 六项总验收仍未完成：完整 request_id/call_id/trace_id 关联、spans、服务端阶段时延及最终逐项审计仍待补齐。

## 2026-09-13 请求 Trace 身份与计时基础

- 新增应用层 RequestTrace、不可变 AccessTrace/TraceSpan 与无 I/O 的 Sink 契约，HTTP 入口建立请求上下文，准入后绑定 call_id；统一 Gateway 向既有 Invocation 表写同一 trace_id。未准入错误不生成 call_id，响应 X-Request-Id 对应本次访问。
- 当前实际测量 HTTP 与准入阶段，使用单调纳秒时钟和 ContextVar 父 span；并行子任务不共用可变栈。默认最多保留 64 个 span，根节点预先占位，溢出记录 dropped_spans；禁止任意属性和异常文本。Sink 错误不改变响应。
- 基础/API 专项 19 passed，26 个真实 PostgreSQL 同步/流式恢复场景核对 request_id/call_id/trace_id 与数据库一致。当前 Sink 仅为可注入边界；默认没有导出器，记录会随请求释放，不能宣称已有可检索 OTel 或结构化 Trace 日志。
- 详细约束与剩余工作见[观测契约](observability-contract.md)：其余阶段实际测量、W3C 上下文与传播策略、有界导出、语义状态/Usage/Cost 汇总、指标及 Docker 验收仍待完成。本轮不更新默认 Docker，不调用真实 Provider，不新增数据库迁移。
- 本轮新增 10 项测试，全量 1663 passed、13 项同源 Uvicorn 弃用警告，compileall 通过。临时 PostgreSQL 和两个独占 pytest 目录已清理；默认服务、数据卷和凭证保留。六项总验收仍未完成。

## 2026-09-13 主要阶段计时与失败语义

- 加入授权 Context、Prompt、请求/执行指纹、初始路由、Provider Attempt、退避、结算及取消协调的实际测量点。Attempt span 记录受限尝试序号；ProviderFailure/StreamFailed 返回值也标记 error，取消为 cancelled，正常拒绝仍保持完成语义。
- HTTP 根 span 对普通错误响应及已提交 200 的失败流标记 error；成功恢复仍为 ok，但此前失败 Attempt 保留 error。计时只观测原流程，不修改错误分类、重试预算、候选顺序或终态事务。
- 基础/执行专项 49 passed；首轮数据库/Trace 专项 55 passed，随后 HTTP 语义更新的本地专项 53 passed。范围与时钟解释见[观测契约](observability-contract.md)：流式 Attempt 包含下游等待及首增量提交，不能称为纯上游网络时延，嵌套 span 不能直接求和。
- HTTP 发送/TTFT、W3C 与传播策略、Token/Cost/实际模型汇总、导出/指标及 Docker Trace 验收仍待完成。本轮未部署、无真实 Provider 调用、无数据库迁移。
- 本轮新增 5 项测试；全量 1668 passed、13 项同源 Uvicorn 弃用警告。全量启动后发现并补充 SSE 断连根 span 标记：响应未完整发送时为 cancelled，发送完成后关闭不误记取消。最新调整另通过本地专项 38 passed、真实 TCP/数据库专项 21 passed（7 项同源警告），compileall 通过；未把较早全量当成覆盖最后调整的证据。
- 临时 PostgreSQL 与两个独占 pytest 目录已清理；默认容器、真实调用记录和凭证保留。六项总验收仍未完成。

## 2026-09-13 HTTP 发送与首业务增量计时

- HTTP 入口汇总发送调用数、等待纳秒数、异常与取消数，固定大小存储，不随分片数量占用 span 容量；1000 次发送仍保留根和结算阶段。
- 分别记录首个合法业务增量准备入队及其 ASGI 发送返回的请求相对时间。正文/refusal 触发，空成功流、首增量前失败和同步 JSON 不伪造 TTFT；发送失败、超时或断连不生成 sent 时间。发送后失败保留已知首字时间，但根 span 仍为 error、无成功 DONE。
- 新增 10 项测试，包含精确单调时钟、容量、正文、拒绝、空流、前后失败、写入错误、超时及断连。最初本地专项 46 passed；最后新增两个故障场景后的非 PostgreSQL 全集 1424 passed、254 deselected，compileall 通过。
- 计时语义与范围见[观测契约](observability-contract.md)。本轮未重跑数据库/真实 TCP 测试，未更新 Docker、未调用真实 Provider、没有新增数据库迁移。默认 Sink 仍无导出，不能宣称 Trace 已可检索。
- W3C/传播策略、Usage/Cost/actual_model 汇总、有界导出/指标、Docker 关联和六项最终逐条审计仍待完成；总目标保持未完成。

## 2026-09-13 只读调用观测摘要

- 新增应用层 InvocationObservationReader/不可变摘要 DTO 与 PostgreSQL 实现，使用独立有限超时、只读一致性快照，不在模型热路径增加 I/O，也不读取正文或凭证。
- 汇总每次 Attempt 的已知模型、Usage、恢复决策、计价配置版本与资源，保留原终态/已结算 Usage 和分币种费用。最后 Attempt 的流提交或最新迟到快照可补充缺失模型；观测冲突显式标记，不拿先前失败候选冒充最终模型。
- 迟到 Usage 单独展示，不改写账务；缺少输入/输出时不拆分供应商 total，也不把未知费用写零。不存在或尚未结算的调用不伪造终态，查询不会创建 Attempt 或结算。
- 首轮数据库恢复矩阵与真实 TCP 专项 33 passed、7 项现有 Uvicorn 弃用警告；随后新增迟到用量、未结算查询和纯规则验证，进入全量回归。完整语义见[观测契约](observability-contract.md)。
- 最终全量回归 1683 passed、13 项同源 Uvicorn 弃用警告，compileall 通过。本轮新增 5 项测试，并在既有 26 个恢复、7 个 TCP、6 个流取消/deadline 场景中增加摘要断言；全量覆盖全部最新改动。
- 本轮未修改数据库 Schema、默认 Docker 或真实凭证，未调用真实 Provider。摘要尚未接入导出器，仍需 W3C/关联头、受限导出、日志/指标及 Docker 最终验收；六项总目标未完成。
- 本轮临时 PostgreSQL 容器与两个独占 pytest 目录已清理，合成测试数据已丢弃；默认服务及持久化数据保留。

## 2026-09-13 W3C 入站关联与业务头校验

- 新增 HTTP Trace Context 解析和应用层关联 DTO；延续有效 trace_id/远端 parent-id，新的访问仍生成自己的 request_id。无效 parent 重新建链，无效 state 不破坏有效 parent；不导出原始 tracestate。
- 校验业务关联头的 ASCII 字符、1–128 长度及重复值；总头部超 32 KiB 在分派前拒绝。业务 ID 缺省不编造，非法值不进入访问记录；错误响应仍有新 request_id 且无准入 call_id。
- 按边界隔离 HTTP 解析与应用上下文；同一分布式 trace_id 的不同请求不共享本地父 span。默认不把 Trace 或业务头发送给 Provider，26 个同步/流式双候选恢复场景逐次检查上游请求头，并核对入站 Trace 与 PostgreSQL 身份一致。
- 新增 38 项测试；入口/流式专项 86 passed，非 PostgreSQL 全集 1466 passed、255 deselected，数据库恢复与真实 TCP 专项 33 passed、7 项已有 Uvicorn 弃用警告，compileall 通过。本轮未重跑数据库全量。
- 依据及保留上限见[观测契约](observability-contract.md)。业务 ID 尚未准入持久化，Agent caller 要求、显式允许的上游传播策略、受限导出/指标与 Docker Trace 验收仍未完成；六项总目标保持未完成。
- 临时 PostgreSQL 与本轮独占 pytest 目录已清理，合成数据已丢弃。默认 Docker、真实记录和凭证未变，未调用真实 Provider，未新增数据库迁移。

## 2026-09-13 业务关联准入持久化

- BusinessCorrelation 下沉为领域值并校验格式；HTTP Trace 上下文在准入前复制到 InvocationAdmission。0018 为 model_invocation 增加可空 task_id/turn_id/step_id，随原准入事务一次写入；原身份守卫使其不可变，数据库也拒绝非法格式。只读观测摘要返回持久化关联，不依赖当前 HTTP 头。
- 新增 9 项测试，涵盖内部调用格式、事务回滚、直接 SQL 格式约束、三个字段修改拒绝，以及 0017 历史所有原字段保留/新字段不回填。26 个 Gateway 同步/流式恢复场景增加持久化关联断言。专项 41 passed，非 PostgreSQL 全集 1473 passed、257 deselected，compileall 通过。
- 首轮全量 1724 passed、6 failed：六处均为旧迁移数量断言；更新为 18 项（含额外未来版本的故障夹具为 19 项），保留原校验和和旧数据检查，随后重新全量验证。此首跑不记作全绿。
- 镜像构建、优雅停机、显式迁移及恢复完成；默认 Schema=0018，Gateway/PostgreSQL healthy、readyz=200。原有 8 条调用保留，两条已知 DeepSeek 调用仍 completed；历史三个关联字段均 NULL。默认 API 的非法关联头返回 400/invalid_request、有新 request_id、无 call_id，无 Provider 调用。
- 本轮未修改已发布模型/Prompt 配置，也未调用真实 Provider。默认镜像已含 Trace 接线，但未注入导出器；Agent caller 要求、受控上游传播、有界导出/指标与最终六项审计仍待完成。
- 最终全量复跑 1730 passed、13 项同源 Uvicorn 弃用警告，覆盖全部最新源码与测试；首轮失败保留上述记录，不混淆两次结果。临时 PostgreSQL 与三个独占 pytest 目录已清理，合成数据已丢弃，默认服务与数据卷保留。

## 2026-09-13 有界 Trace 导出管线

- 新增应用层导出 DTO/输出端口和有界后台管线。请求结束仅同步入队；独立空 Context 工作任务读取摘要，核对 call_id/trace_id/业务关联，不继承授权上下文、不读取未准入调用。摘要失败仍可输出基础访问，错误文本不进入记录。
- 队列满或生命周期外调用立即拒绝；读取/输出各有超时，输出失败无重试。停机先拒收、有限时间排空，到期取消并等待同一清理任务，丢弃有计数。端口必须配合异步取消，不声称能终止恶意同步阻塞 I/O。
- Gateway 支持显式 trace_output 注入并拥有管线生命周期；排空在模型执行资源释放之后，不复用模型租约。默认启动器尚无实际输出端口，OTel 序列化/传输、日志和业务指标仍待实现，不宣称已有外部可检索遥测。
- 新增 19 项测试，覆盖容量/时限、缺失/错误/不匹配摘要、输出失败/超时后继续、空上下文、排空、重复取消和 HTTP 饱和隔离。最终非 PostgreSQL 全集 1492 passed、257 deselected；26 个完整 Gateway 同步/流式恢复场景通过真实数据库摘要与托管输出断言，compileall 通过。本轮未重跑数据库全量。
- 约束见[观测契约](observability-contract.md)。本轮未部署、未新增迁移、未调用真实 Provider；临时 PostgreSQL 与独占 pytest 目录已清理，合成数据丢弃，默认服务和数据卷保留。六项最终审计仍未完成。

## 2026-09-13 OTLP 三信号接收验证

- 完成显式 OTLP JSON 投影、受限 HTTP 输出及固定维度累计指标。队列拒收不漏记进程内请求数；发送/停机仍可能丢失遥测，不宣称无损。Collector 部分拒收不能视为成功，错误正文不保存。
- 24 项新增测试；Trace/导出专项 60 passed，非 PostgreSQL 全集 1516 passed、257 deselected，compileall 通过。本轮没有数据库全量回归。
- 独立官方 Collector 0.160.0 成功解码 2 spans、1 条关联日志、3 个指标共 11 data points。发送端三信号确认与 Collector 解码输出一致；[复现与关联身份](../deploy/acceptance/otel-smoke.md)已记录。
- 临时 Collector 已删除，临时日志不可再查询，镜像与合成夹具保留可复现。无真实 Provider 调用、迁移或默认部署变更。默认启动器接线、受限生产目的端、完整 API 导出及六项最终审计仍待完成。

## 2026-09-13 统一启动器 OTLP 接线

- 新增关闭默认的 dev Bootstrap telemetry 分支，仅允许显式 loopback origin 和端口；拒绝 DNS/外部地址、凭证与额外字段。没有扩大生产启动能力，也不替代尚待完成的生产目的端策略。
- 启动器显式持有输出生命周期，Gateway 退出并排空后再关闭 HTTP 客户端；构造失败、启动失败、正文异常和取消均覆盖清理。未配置时不创建输出客户端。
- 新增 22 个配置及生命周期测试；相关专项 80 passed，非 PostgreSQL 全集 1538 passed、257 deselected，compileall 通过。本轮未运行数据库全量、未修改默认 Bootstrap、未构建/重启默认容器、无付费调用。
- 后续仍需默认部署、完整 API → 持久化摘要 → Collector 验证、生产目的端及关联/传播策略审计；六项整体未完成。

## 2026-09-13 完整 Docker/API 观测验收

- 当前源码镜像构建通过，隔离夹具通过正常 Bootstrap 启用 OTLP，Collector 与 Gateway 共享网络；不开放宿主端口、不读取真实凭证、不注入测试 Sink。
- verify 退出 0：同步恢复、失败流、客户端断连、流式恢复共四次调用通过；8 次合成供应商请求，无真实 Provider 调用。新增观测检查器将 API call_id 与 Collector 摘要/根 span/Attempt spans 关联，核对实际模型、终态、恢复动作、未知总量、已知末次 Usage、首增量计时及三类指标，退出 0。
- PostgreSQL 独立读取终态与导出一致。检查器拒绝缺失 HTTP span、错误 actual_model、缺失指标和合成凭证泄露四种负向变异。第一次检查错误要求未知总输入非空，依据失败 Attempt 的未知消耗修正了验收断言，未改业务计量逻辑。
- [复现流程与四个调用身份](../deploy/acceptance/README.md#完整观测链路)已归档。隔离容器与合成材料卷清理，临时数据库/日志丢弃不可恢复；默认运行服务和数据保留。默认启用、生产目的端、关联/传播要求与最终六项审计仍待完成。本轮未重跑 pytest 全量。

## 2026-09-13 默认开发部署启用观测

- 默认 Bootstrap 显式配置 loopback OTLP；Compose 新增无宿主端口、只读文件系统且无业务凭证挂载的 Collector，Docker 日志按 10 MiB/3 文件轮转。源码镜像已更新到默认 Gateway，独立迁移正常，数据库卷不变。
- readyz=200，Gateway/PostgreSQL healthy。一次非法业务 ID 请求在准入前返回 400，无 call_id；Collector 的 Error span/not_admitted 日志与响应 request_id 一致，指标存在。数据库调用表仍为部署前的 8 条，零真实 Provider 调用。
- 新增默认配置断言；配置专项 38 passed，非 PostgreSQL 全集 1539 passed、257 deselected。一次提权 pytest 因系统临时目录权限发生 setup 错误，普通工作区重跑通过；未将环境错误标为业务测试通过。
- 查询及停用操作见[Docker 文档](docker-quickstart.md#本机观测输出开发部署)。默认本机查询可用不等于生产持久化存储；远程目的端政策、关联/传播要求及最终六项审计仍未完成。

## 2026-09-13 Agent 声明关联校验

- 新增闭合 Caller-Type（agent/non-agent）和 Operation-Scope（turn/step）请求头；省略保持非 Agent 兼容行为。领域关联值负责必填规则，HTTP 仅解析受限声明。Agent 缺 task/turn、独立操作缺 step、非法/重复声明在正文读取和准入前拒绝。
- 这是调用方声明，不作为认证或权限来源，不假装能识别未声明的 Agent。业务 ID 继续使用已实现的持久化路径，声明不进入 Provider 请求。
- 新增 17 项边界测试，Trace 上下文专项 55 passed；非 PostgreSQL 全集 1556 passed、257 deselected。尚未执行新增声明的完整数据库/API 回归或默认部署，未产生 Provider 调用。Provider 受控传播、生产观测边界及六项最终审计仍需继续。

## 2026-09-13 Agent 数据库矩阵与全量回归

- 26 个同步/流式双候选矩阵现在显式声明 agent/step；真实 PostgreSQL 准入记录、只读摘要与 AccessTrace 的 task/turn/step 一致，所有 Attempt 均断言无 Trace/业务 ID/调用类型/操作范围头外传。涵盖 success、retry、fallback、uncertain、协议错误、能力不匹配、预算及凭证失效等既有分支。
- 独立 PostgreSQL 16 全量测试 1813 passed，13 项现有 Uvicorn 弃用警告，耗时 282.95 秒。临时数据库容器及专属 pytest 目录已清理；合成数据丢弃，默认运行服务和数据未改动。Agent 源码增量尚未部署。
- 审计确认 FR-105 不要求 Gateway 自建存储，FR-333 要求没有显式授权时不传播；不能把可选远程扩展作为无止境新增门槛。但 FR-103 的完整聚合指标、FR-063/187 的阶段/子 span 覆盖仍缺直接证据与实现，已明确列入[观测契约](observability-contract.md#仍待完成)，不能仅因全量测试通过宣称六项完成。

## 2026-09-13 调用观测聚合指标

- 有界 worker 对身份匹配的摘要累计观测终态、Attempt、实际重试/fallback、各 Token 类型已知量和未知测量次数、按币种/完整性/确定性区分的费用记录及已知金额。没有新增数据库写入，不再计算迟到 Usage；内部 Decimal 与 OTLP 近似数值的边界已说明。
- 币种至多 16 个，超出仅增加 overflow，不把不同币种混为 other。无摘要、未知消耗及费用不补零；这些是观测集合，不冒充完整账务或唯一调用计数。
- 新增 5 项测试，相关专项 48 passed，非 PostgreSQL 全集 1561 passed、257 deselected。覆盖真实转换与恢复意图区分、子集隔离、迟到量不重复、币种容量、精确内部金额、OTLP 投影及身份标签排除。
- 尚未部署或执行真实 Collector 新指标验收；Prometheus 兼容面、资源饱和度、阶段测量与六项最终审计仍需继续。无付费调用。

## 2026-09-13 容量资源 Gauge 接线

- 准入与 Attempt 池直接维护使用量/峰值和按失败 gate 区分的拒绝次数；不从 QPS 推测饱和度，不记录身份标签。Gateway 经 Process/Backend 同步读取固定快照，导出另附队列深度/上限。
- Owner 重建可能重置容量读数，因此 OTLP 使用 Gauge，不冒充跨 Owner 单调 Counter；读取失败不影响响应或导出，缺失值不补零。无额外数据库查询或模型许可获取。
- 新增 4 项测试，相关专项 47 passed，非 PostgreSQL 全集 1565 passed、257 deselected。覆盖实例/租户拒绝、实际 Attempt gate 与可用性预检查区别、释放后归零/峰值保留、导出异常隔离及无身份标签。
- 增量仍待真实 Collector/Prometheus 接收验证及默认部署；不宣称覆盖 CPU/内存或全部背压指标。六项最终审计仍未完成，无真实 Provider 调用。

## 2026-09-13 Prometheus 接收与默认部署

- 官方 Collector 0.160.0 已有 prometheus exporter，新增共享 loopback 9464 采集面，无宿主端口。四场景完整 API → PostgreSQL 摘要 → OTLP → Prometheus 通过；8 Attempts、2 retry、2 fallback、已知输入 20/输出 4、未知费用 4，延迟桶及容量上限 200/256 一致，无身份/内容标签泄露。
- 实采发现数量 Gauge 使用单位 1 会被规范化为 ratio；修正为 `{item}`，费用单位改为 `{currency}`，补单位断言并重建重新完整验收。相关专项 33 passed（一次提权运行有 pytest cache 权限警告），compileall 通过；本轮未重跑数据库全量。
- 同一已验证镜像及 Collector 配置已更新默认开发部署，同时包含 Agent 声明、调用聚合和容量观测增量。readyz=200，Agent 缺 ID 请求 400 无 call_id，loopback scrape 成功，默认数据库仍为 8 条，无真实 Provider 调用。
- 隔离容器及合成材料卷已清理，临时数据库/日志丢弃；默认服务和数据保留。复现见[Docker 验收](../deploy/acceptance/README.md#完整观测链路)及[采集说明](docker-quickstart.md#prometheus-兼容采集)。阶段测量与六项最终审计仍需继续。

## 2026-09-13 Adapter 阶段子 Span

- 同步及流式 Adapter 新增 CLIENT 子 span；规范化失败不能误标 ok。流式首次合法正文/拒绝增量记录单调时钟偏移，重复增量不覆盖；无增量保持未知，首增量后观测时段独立投影。
- generator 暂停向下游交付时恢复 Provider Attempt 父级上下文，避免提交/结算错误嵌套到 Adapter。没有每分片 span 或无限事件集合，仍受既有容量约束。
- 新增 5 项精确边界/父子关系测试，非 PostgreSQL 全集 1570 passed、257 deselected；随后 OTLP/Adapter 专项 29 passed。同步/流式成功失败、下游父级隔离和单调时钟一次采样均覆盖。未声称可测未提供的 Provider 内部 queue/generation。
- 增量尚未部署，仍需完整 API/Collector 验证与剩余阶段审计。无真实 Provider 调用，默认服务未改动。

## 2026-09-13 Adapter 子 Span 完整链路验收

- 当前源码镜像构建通过；隔离四场景 API/Collector 检查器确认 8 次 Attempt 与 8 个 Adapter CLIENT 子 span 的父 ID 一一匹配。同步无首增量字段，三个流式调用仅实际输出的 Attempt 有首增量和后续观测时段；失败流/断连 Adapter 为 Error。Prometheus 累计断言仍通过。
- 46 项 Trace/Adapter/OTLP 专项通过，compileall 通过。四个调用身份及复现断言归档到[Docker 验收](../deploy/acceptance/README.md#完整观测链路)。本轮未重跑数据库全量。
- 已验证镜像部署到默认 Gateway，readyz=200，默认调用表仍 8 条；零真实 Provider 调用。隔离夹具已清理，临时数据丢弃，默认数据保留。下一步为最终六项逐项审计及最终全量回归，不以阶段时间推测供应商内部排队/生成。
