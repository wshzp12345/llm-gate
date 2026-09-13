# 六项基本能力最终核对

日期：2026-09-13。对象是用户列出的六项 Gateway 基本能力及其[登记边界](scope-v0.1.md#六项基本能力gateway-范围内)，不是整个 requirements.md 的生产版全功能交付。当前状态：六项基础范围实现与验收通过；源码、专项、Docker/API 证据已核对，最后全量回归通过。

| 验收项 | 当前实现与直接证据 | 限制，不混同于完成声明 |
| --- | --- | --- |
| 自有协议 | `domain/model.py` 定义请求、结果、Usage、FailureCode；`domain/streaming.py` 定义 Delta/Completed/Failed。`test_completion.py` 验证私有对象转换、未知 Usage、非法信封和安全错误；`test_model_http.py`/`test_model_stream_http.py` 验证公开响应。 | HTTP 保持兼容形状不代表透传供应商 DTO；当前不支持的字段明确拒绝。 |
| Provider 抽象 | `application/ports.py` 的同步/流式端口仅使用 Gateway 类型；兼容 Adapter 负责鉴权、请求与结果映射。`test_gateway_recovery.py` 的 26 个数据库/API 场景与独立 Docker 双 Provider 验证同一 Alias 不变、实际模型可追溯。 | 两个独立兼容协议 Provider，不声称覆盖全部专有协议；真实 DeepSeek 同步/流式证据见原清单。 |
| Prompt 资产 | `application/prompts.py`、PostgreSQL Prompt 存储及管理 API 实现不可变版本、CAS 发布、固定版本渲染与权限。`test_prompt_management.py` 直接验证历史记录不可 UPDATE/DELETE、并发发布唯一成功、跨租户隔离；`test_prompt_invocation.py` 及恢复矩阵验证在途调用不受新发布影响。 | 基础资产管理，不扩展到 Agent 记忆或自动 Prompt 质量评测。 |
| 重试/fallback | 纯恢复策略、Attempt journal 和执行器共同控制预算、截止时间与候选资格；矩阵覆盖 retry/fallback/invalid/uncertain/凭证失效/能力不匹配/refusal，Docker 实际发生 3 次 Attempt、1 次 retry、1 次 fallback。 | 能力声明与门控不是质量等价证明；默认真实配置仍单候选，不伪称已开启多供应商容灾。 |
| 流式边界 | 执行器将已提交状态传入恢复决策；输出后失败返回明确错误且不发 DONE。`test_streaming_tcp_postgres.py` 覆盖真实 TCP 成功、重试、拒绝、截断、提交前后断连与 deadline；Docker 失败/断连无后备调用，唯一终态有持久化证据。 | ASGI send 返回不是客户端收件确认；失败流的 HTTP 200 不等于调用成功。 |
| Trace/可观测性 | 新 request_id、准入 call_id、W3C trace_id 与业务 ID 关联；只读一致快照提供 actual_model/Attempt/Token/Cost。单调阶段、Adapter 子 span、首增量及发送计时；有界 OTLP 三信号和 Prometheus 兼容面。`test_trace_context.py`、`test_request_trace.py`、`test_adapter_trace.py`、`test_otlp_output.py`、聚合/资源测试及 Docker 检查器核对实际接收。 | 未知消耗/费用保留未知；不同币种不混加，Token 子集不重复加。上游内部排队/纯生成不可推测；Trace 计时是明确边界的本地观测。 |

## 运行与复现证据

- 最新隔离四调用：同步 `a98c1fff-c772-430a-941f-ae350238602f`、失败流 `b6e28ba9-134e-4855-9b5d-2f8a76c94dfa`、断连 `bce2905a-38f9-47cb-b4d8-6357a20b00ea`、流式恢复 `0379f66b-f4af-4d7f-a39c-f748f85632ae`。Collector 中 8 个 Attempt 对应 8 个 Adapter 子 span，父身份一一匹配，失败状态与首增量属性经过断言。
- Prometheus scrape：8 Attempts、2 retries、2 fallbacks、已知输入 20/输出 4、未知费用记录 4；容量上限 200/256 和延迟直方图可采集。不是用 HTTP 200 单独作完成证据。
- 默认 Gateway 已运行此镜像，readyz=200；数据库原 8 条调用保留。Collector 仅共享 loopback 接收/采集，无外部目的端或宿主指标端口。回归使用独立临时数据库，无真实 Provider 调用。
- 复现命令与查询方式见 [Docker/API 操作](docker-quickstart.md)、[完整故障及观测验收](../deploy/acceptance/README.md)、[计时与数据边界](observability-contract.md)。临时夹具清理后，其日志/数据库不可查询，可重新执行确定性验证。

## 不得扩大或缩小的边界

六项完成不等于全部生产 v0.1、所有 FR 或所有供应商功能完成。Agent 循环、Tool 执行、Session/Memory 仍在外部。FR-105 明确不要求 Gateway 自建观测存储；本地轮转日志和采集面不冒充生产持久化平台。FR-333 无显式允许策略则不得传播上游 Trace，当前禁止外传，未实现任意透传开关。

观测导出是有界 best effort；队列满、读取失败、重启及轮转可能丢数据。已观测聚合不是完整账务，精确费用仍以不可变账务记录为准。模型质量等价需部署者的独立评测，不能因协议兼容自动宣称等价。

## 最后验证门

最终全量回归：**1827 passed，13 warnings，288.77 秒**，包含 PostgreSQL 集成测试；警告均为 Uvicorn 使用即将弃用的 `asyncio.iscoroutinefunction`。`compileall -q src` 通过。

首次最终回归为 1801 passed / 26 failed：恢复矩阵仍使用新增 Adapter span 前的精确阶段集合。修正为必须包含 `adapter_request`，并增加 Adapter 与 Attempt 数量和父 span 身份一一对应的断言；没有删除故障场景或放宽为任意阶段集合。上述 1827 项是修正后的完整重跑结果，不是只重跑失败项。

验证使用独立临时 PostgreSQL 和合成 Provider，不产生真实模型调用。默认容器 healthy；关闭环境代理后直连 `/readyz` 为 200，`llm_gateway.model_invocation` 仍为 8 条。此次没有修改默认凭证、配置、Prompt 或业务数据。

本轮隔离 PostgreSQL 容器已移除，tmpfs 测试数据已丢弃、不可恢复；可重新执行测试生成，不影响默认业务数据库。
