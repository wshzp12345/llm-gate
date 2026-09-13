# 流式文本契约与接入状态

本页记录六项基础能力中的流式实现，不把 Provider 完成等同于 Gateway 调用完成。
当前已实现领域事件、Provider SSE Adapter、应用层流式 Attempt 执行器及 HTTP/数据库接线；模型 API 工厂须显式 `enable_streaming=True` 才启用该路径。
统一启动器支持显式 `--enable-streaming`，同时打开 API 与配置能力校验；未传参数时仍关闭。默认 Compose 已启用该参数并更新容器。隔离 Docker 已验证双供应商恢复、固定 Prompt、输出后截断及断连。默认开发数据库的 DeepSeek Binding 已在 revision 2 显式发布 streaming=true，并完成一次真实 Provider 验证；初次同步 Smoke 示例仍保持 false，不能把入口开启等同于模型支持。

## Gateway 自有事件

| 类型 | 字段 | 含义 |
| --- | --- | --- |
| StreamDelta | sequence、resolved_model、kind=text/refusal、text | 一个非空文本或拒绝增量；正文不进入 repr |
| StreamCompleted | sequence、ProviderResult | Provider 的有序流已完整结束；还必须通过 Gateway 结算才能向客户端发成功终态 |
| StreamFailed | sequence、ProviderFailure、Usage、resolved_model | 失败终态；保留已报告的 Usage 和已知模型，未知值不补零 |

每次 Provider Attempt 的序号从 1 开始连续增长，最后最多一个终态。供应商 JSON、内部请求 id 和 reasoning_content 不进入这些事件。
角色/纯元数据/私有推理片段不是业务增量，不构成向客户端提交的内容。

## SSE Adapter 的边界

- `.stream()` 使用已有安全 HTTP 客户端、凭证与 Gateway 取消上下文，发送兼容协议 stream=true/include_usage=true；没有内置重试、重连或模型切换。
- 增量解析严格 UTF-8，支持分块的多字节字符、LF/CRLF/CR、初始 BOM、多行 data 与注释。id/retry 字段不会触发续传或重连。非默认业务 event 类型明确拒绝。
- 按实际字节限制单事件（默认 256 KiB）及完整响应（默认 4 MiB）；包括分块到达的 CRLF 终止符也在交付前校验。消费速度受调用方拉取控制，不后台预取整包。
- 要求兼容 chat.completion.chunk、唯一 choice index=0、稳定实际模型及已出现的供应商 id。仅接受文本/拒绝及可忽略的 reasoning_content；工具或其他未实现 delta 拒绝。
- 先校验整帧，再交付其中的增量；非法 finish_reason/Usage/推理字段不能让同帧正文先泄漏出去。
- 完成要求合法 finish_reason、DONE 和完整 EOF 一致。完成事件延迟到全部检查后；重复终态、终态后内容、缺 DONE、截断、变更模型/身份均失败。finish_reason=length 可以是明确的有限输出结果，不伪装成 stop。
- 单次最终 Usage 可与 finish chunk 一起到达，也可在其后的空 choices chunk 中到达；重复或提前的 Usage 明确拒绝。缺 Usage 保持 unavailable，失败前已收到的 Usage 留在 StreamFailed 中。
- 网络错误沿用同步规范化分类：连接建立超时可重试，发送后读取超时等 uncertain 不得重放；错误正文/网络异常文本不透传。
- 调用方提前退出时必须关闭迭代器；Adapter 释放本次响应和取消登记，不关闭共享客户端。取消、Gateway 截止超时向生命周期层传播，不捏造 Provider 成功终态。

## 应用执行器的提交规则

`StreamingAttemptExecutor` 使用已锁定的候选、RetryPolicy 和一个绝对单调时钟截止点。调用方必须提供运行时租约、StreamAttemptJournal、StreamOutputPort、StreamCommitPort；已准入调用还应传入共享 AttemptExecutionControl，由执行器管理 scope 和单次 Attempt context。

首个业务 delta 之前，合规 Provider 失败可以按策略重试或 fallback；uncertain 始终停止。首个 delta 交给输出端前设置不可逆标记：即使输出写入抛错，也不能证明客户端没收到字节，因此绝不能借此换模型。
文本和拒绝 delta 都使用相同提交保护。拒绝终态不会被当作恢复错误。

首个业务 delta 交付前，必须等待 StreamCommitPort.first_delta 完成。PostgresStreamCommit 在独立事务中写入 `invocation_stream_commit`，只含 call_id、Attempt 序号、实际模型、text/refusal 类型和时间，不包含正文。它是不可撤销的交付预留，不证明客户端已经收到字节；提交或后续输出失败都不能重试。

增量迁移 `0016_stream_commit.sql` 将提交、Attempt 启动/结束与 Invocation 取消/结算通过同一行锁串行化：只能提交最新未完成且处于 running 状态的 Attempt；记录不可改删；已提交调用拒绝新 Attempt，原 Attempt 只能 stop，已知实际模型不能变更。约束在数据库触发器中执行，直接 SQL 也不能绕过。旧 Invocation 不补造提交记录。

执行器二次验证事件类型/连续序号/稳定模型、完成结果与已交付正文的一致性以及输出大小。重复、乱序、缺失终态或凭空增加完成正文都变成协议失败；提交后的失败不启动新候选。
每个 Attempt 的 journal.finished 接收完整 StreamCompleted/StreamFailed，因此不会在接口处丢弃失败 Usage。
Provider iterator 与运行时租约在退出时关闭，退避发生在释放租约之后。输出故障和取消交给上层持久化生命周期处理，不静默转换成功。

## 取消与迟到观测

- 流式执行器使用同一个 AttemptExecutionControl，管理本次执行的取消登记、Attempt handle、终态观测和写库检查点。发生取消时，在控制作用域退出前尝试记录迟到观测；已完成 journal checkpoint 的观测清除，不重复追加。
- 兼容 Adapter 在完整合法帧解析后，将当前已知 Usage/实际模型同步交给内存观测入口。此快照不携带正文，也不代表完成；即使 Usage 已到达，没有合法 DONE/EOF 也不能伪装为 succeeded。
- AdmittedExecutionLifecycle 先赢得持久化取消预留，再停止 Provider/等待清理。正文输出中、Usage 到达后及终态 journal 等待中取消，均使用相同路径，不创建新 Attempt。
- `0017_late_stream_observation.sql` 允许 failed/uncertain 迟到记录保留 Usage 和实际模型；原有取消身份、追加不可变、幂等和 Token 子集约束保留。迟到记录不更新原终态和费用账本，不重新计费。
- deadline 到期后，执行控制只在本地完全退出后暴露未 checkpoint 的无正文快照。超时结算在同一事务中补写缺失 Attempt 的 Usage/实际模型、费用及 deadline_exceeded 终态；Attempt 仍 uncertain，不因收到 finish/Usage 就判成功。若结果已 checkpoint，则沿用已提交记录，不覆盖或重复计费。
- deadline 路径不伪造主动取消预留，也不把观测追加成第二笔费用；数据库故障回滚所有新增账务和终态。已知 Token 可以保留，而费用因缓存/推理子集缺失仍可能 unavailable。
- 主动取消和 deadline 已接入 HTTP 调用生命周期。传输失败只取消外层调用，不能直接取消等待增量确认的执行子任务，否则会绕过持久化取消预留；专项测试覆盖此顺序。真实 socket 断连/排空验收仍待完成。

## HTTP 接线与剩余开放门槛

`ModelStreamResponse` 已接入显式开启流式的模型路由。该模块通过有界队列及逐帧写入确认传递背压，首增量前不发送 SSE 响应头；ASGI send 完成不代表客户端已收到数据。提交前失败返回 JSON，提交后失败发送安全错误并关闭且不发成功 DONE；成功依赖后端返回已结算响应。模块不执行持久化或恢复决策。

它保存创建时的调用上下文，通过本地 admitted 元数据获取身份；断连、写入错误/超时、取消均等待调用清理，重复取消不跳过清理。单事件有字节上限、每次写入有有限超时。此前独立模块阶段通过 13 项直接 ASGI 测试及非 PostgreSQL 回归 1383 项；后续接线证据以[六项验收清单](basic-capabilities.md)的最新记录为准。真实 TCP/Docker 验收尚未完成。

1. 已接通 HTTP 请求与 TextInvocationQuery.stream/stream_output；准入后传递 call_id/accepted_at 本地元数据，不产生网络输出；后端使用独立流式指纹与 streaming 能力筛选。
2. 响应身份中间件改为原位 ASGI header 转换，直接等待下层 send，不引入缓冲任务；路由专项验证背压和授权上下文保留。Accept 按 JSON/SSE 分别协商，text/* 不误用 application/*。
3. 同步及流式调用共用安全异常映射；已接入 SharedStreamAttemptJournal、最终结算和取消/deadline 观测，未结算不能发成功终态。
4. SSE 提交后失败发明确错误并关闭，不发送成功 DONE；成功终止帧包含已结算的 Gateway 元数据，不重复发送完整正文。
5. 调用方真实 TCP 已覆盖成功、首增量前重试、拒绝、提交后缺终态、输出前后断连及 deadline；拥有执行租约的服务另覆盖流式完成与系统排空取消。统一 Gateway 的双候选流式 fallback 与在途 Prompt 发布通过 API 矩阵；隔离 Docker 的真实上游 HTTP 成功恢复和固定 Prompt 已通过。仍需 Docker 故障/取消组合及默认部署更新。

测试 `test_streaming_tcp_postgres.py` 使用真实 Uvicorn/TCP 与 PostgreSQL，但上游是可控 MockTransport。它让上游在首增量后等待，客户端读到增量并检查流提交已持久化、终态尚未结算后才放行，因此整段缓冲无法通过。断连后等待后端释放所有权，再核对 cancelled 和资源清理。`test_full_service_text_process.py` 验证拥有执行租约的流式服务在排空后完成或记录 shutdown_drain_expired，并可重新获得执行租约。

这些证据不能替代完整 Docker 上游传输验收。完整 Trace/spans/阶段时延仍独立待补。

## 请求身份与共享后端

- TextInvocationQuery 要求严格布尔 stream 与输出端同时显式提供；输出端是只接收 Gateway 事件的应用层 delta port，不暴露 HTTP/framework 类型，不进入 repr、比较或指纹。
- 请求/执行分别使用 `stream-text-v1` profile；带固定 Prompt 版本时使用 `prompt-stream-text-v1`。同步 `text-v1`、`prompt-text-v1` 的序列化和已有黄金摘要不变。流式请求包含 stream/include_usage 语义，不能与同步请求共用身份。
- StaticServiceRequirement 从请求选择 streaming；没有声明该能力的候选在 Provider I/O 前排除。后备候选仍须通过同一能力、资源、安全和动态门槛。
- PostgresFullServiceTextExecution 已选择 StreamingAttemptExecutor，绑定原 runtime/circuit journal、PostgresStreamCommit 和 AttemptExecutionControl。StreamingInvocation 将合法终态投影给现有结算包装器，复用耗尽路由、Usage 汇总、绝对截止和最终提交，不另建账务路径。
- FullServiceTextBackend 的真实 PostgreSQL 集成覆盖 success/retry/refusal/缺失终态/cancel/deadline/不支持 streaming，共 7 场景。首增量输出时已存在流提交记录、尚无 Invocation 终态；Fingerprint 密钥和所有容量/凭证资源按原生命周期释放。
- 后续新增相同 7 场景的 HTTP 入口测试，核对响应与相同数据库终态/资源释放；使用 HTTPX ASGITransport 和合成 Provider，不代表真实 TCP 验收。统一 Gateway 的默认 HTTP stream=true 与发布能力门槛尚未开放。

## 本轮证据

- `tests/test_provider_streaming.py` 与 `tests/test_streaming_attempt_execution.py` 共 55 项，含真实 Adapter→执行器的组合测试（HTTPX 可控字节流），覆盖增量交付、前置恢复、提交后停止、失败 Usage、协议边界、资源释放和取消。
- `pytest -q -m 'not postgres'`：1348 passed，193 项 PostgreSQL 测试未选中；compileall 通过。本轮没有修改数据库或运行真实 Provider 调用，也未重新部署当前 Docker Gateway。
- 没有 HTTP/SSE 端到端完成声明：当前 HTTP 明确拒绝 stream=true 的回归仍通过。

## 失败流的共享结算与凭证租约增量

- `StreamFailed.as_result()` 将已知 Usage/实际模型转换成 Gateway ProviderFailure 的安全观测字段；无正文、无供应商对象。SharedStreamAttemptJournal 委托已有 Attempt journal，保留计价锁定和原事务边界。
- Attempt 和最终失败结算保留已知 Token 与实际模型。结算要求传入观测与已提交的末次 Attempt 一致，不能用同一个错误码掩盖模型/Token 篡改。失败仍为 failed/uncertain，finish_reason/service_level 为空。
- 费用完整性与成功状态独立：失败但 Usage 完整可计算 estimated 费用；缺少输出时可为 partial；输入总数已知但缓存数未知时，差异化计价可能仍为 unavailable，不补零。
- 凭证租约 runtime 提供 stream 入口；提前关闭、取消、完成都释放当前响应和 Adapter 引用；401/403 标记本次调用中已拒绝的凭证版本，后续 acquire 不得重复使用。complete/stream 共用单次 Attempt 使用限制。
- 新增 13 项失败结算测试（含 8 项真实 PostgreSQL 场景）和 5 项凭证流测试。HTTP 首增量持久化、断连桥接、完整取消/迟到观测仍未接通，当前不会因此开放 stream=true。
