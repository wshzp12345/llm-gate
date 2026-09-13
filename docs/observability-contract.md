# 请求观测接线与验收状态

本页补充[六项基本能力清单](basic-capabilities.md)。当前已部署请求关联、Gateway/Adapter 阶段计时、有界 OTLP 三信号与 Prometheus 采集；最终验收状态见[逐项核对](six-capability-final-audit.md)。下文保留的阶段性“尚未部署”描述仅是实施历史，不覆盖后续部署证据。Provider 内部排队/纯生成时间未提供时保持未知。

## 身份及边界

模型 HTTP 入口创建新的 request_id 和根 span；有效入站 Trace 上下文延续 trace_id，并把入站 parent-id 作为根 span 的远端父节点，否则创建新的 32 位十六进制 trace_id。X-Request-Id 使用新的请求身份，调用方不能指定它。准入成功后才绑定 Gateway 生成的 call_id；未准入错误不生成或披露 call_id。统一 Gateway 的调用记录使用同一上下文 trace_id；同步与 SSE 子任务继承同一请求上下文，退出后恢复原上下文，不跨请求共享本地父节点。

RequestTrace/AccessTrace/TraceSpan 与 AccessTraceSink 归应用层所有，不依赖 HTTP 框架、数据库或遥测 SDK。传输层只负责建立上下文与最终 offer；Sink 契约要求同步、无 I/O 地接收至有界资源或拒绝。导出错误不改变响应、执行终态或账务。源码已有可注入的有界导出管线，但具体生产传输/序列化和默认启用仍待完成；未注入 Sink 时记录随请求释放，不能宣称日志/OTel 已可检索。

## Span 与计时

Adapter 增量已通过四场景 Docker/API/Collector 验证并部署：同步 `_complete` 和流式 `stream_completion` 生成 `adapter_request` 子 span，归属于对应 Provider Attempt。正常返回的规范化失败同样标记 error；异常/取消保留原 span 规则，不记录异常正文。OTLP 投影为 CLIENT span。

流式首次合法 StreamDelta（正文或拒绝，不含隐藏推理/元数据）记录 `gateway.adapter.first_delta_ns`，相对于 Adapter 请求入口。`gateway.adapter.after_first_delta_ns` 为首增量至 Adapter 结束的剩余观测时段。两者使用同一单调时钟；没有业务增量则不生成这两个属性，包括同步、空流和首增量前失败。它们包含网络、解析及下游暂停消费时间，不是供应商纯计算/队列时长，不能冒充 Provider 内部 generation。

Adapter generator 向调用方 yield 期间恢复父级上下文，下游提交/结算不会被错误归为 Adapter 子 span；恢复消费后继续 Adapter 自身上下文。计时仍覆盖该暂停，因此不将嵌套 span 相加当总耗时。容量沿用每请求总 span 上限，未为每个分片创建 span。

- 使用单调纳秒时钟，记录相对请求起点的 start_offset_ns 和 duration_ns；不使用数据库墙钟时间戳相减，不冒充 Token/Cost 的来源。
- 当前实际接线包括 HTTP、授权 Context 获取与有效期检查、Prompt 渲染、请求/执行指纹计算（分别一个 span）、准入事务、初始路由/证据、Provider Attempt、退避、正常/超时结算和取消协调。指纹 span 不含密钥读取；授权 span 不应被解释为覆盖所有外部身份服务 I/O。
- 父 span 使用独立 ContextVar；并发子任务为兄弟节点，不共用可变栈。默认每请求最多保留 64 个 span，构造上限 256；进入阶段时预留容量，保证根 span 不被先结束的子节点挤掉。超出容量只增加 dropped_spans，不阻塞业务。
- Provider Attempt span 记录 1–3 的 attempt_number；规范化 ProviderFailure/StreamFailed 即使作为返回值也标记 error，取消标记 cancelled。HTTP 根 span 对状态码 >=400 或规范化错误响应标记 error，因而已提交 200 的失败流也不误记 ok。SSE 响应完整发送前断连显式标记 cancelled，已完整发送后的连接关闭不改写该状态；这不改变 Invocation 的持久化终态，也不证明客户端收到全部字节。拒绝但正常完成仍为 ok；结算 span 的 ok 只表示事务正常返回，不意味着 Invocation 成功。
- Provider Attempt 计时范围是端口调用/流消费，流式包含等待下游输出和首增量提交的时间，不是纯网络上游时延。退避单独记录；正常结算覆盖最终事务，取消协调覆盖预留、停止及清理。嵌套阶段可能重叠，禁止把所有 span 时长相加当作总时延。
- Span 不接受任意属性或异常文本，不记录消息、输出、Prompt 变量、身份凭证、原始上游错误。call_id 只作为受限关联字段，不作为低基数 Metrics 标签。

## HTTP 发送与首增量时延

- `http_send` 是固定大小汇总，不为每个分片分配 span：记录 ASGI send 调用数、累计等待纳秒数、异常数及取消数，包含响应头、正文和终态发送。失败或取消的等待也计入时长；不包含分片之间的生成/排队时间，不代表客户端实际收件时间。它与 HTTP/流式 Attempt 时间重叠，不能再相加作为总时延。
- `first_delta_ready_ns` 从请求入口起算，到首个合法业务增量完成 SSE 编码、准备进入发送队列为止；此时已通过原持久化提交点。它包含授权、路由、重试及提交耗时，不是纯 Provider TTFT。
- `first_delta_sent_ns` 从同一起点算至该业务帧的 ASGI send 正常返回，包含队列等待及背压。只有正文或 refusal 增量触发；响应头、推理内容、元数据、终态及 DONE 不触发。后续增量不覆盖首值。
- 同步 JSON、空成功流及首增量前失败均保持两个值为 null；首增量已准备但发送失败/取消时只保留 ready，sent 为 null。已有首增量之后失败不抹掉已知时间，也不把失败流标成成功。ASGI 返回不是客户端收到字节的确认。
- 发送超时由外层 deadline 取消正在等待的 send，因此发送汇总计为 cancellation；外层收到 TimeoutError，HTTP 根 span 为 error。此处记录所观测的层级，不把底层取消等同于用户主动断连。

## 交付限制与验收边界

1. Agent 声明已通过 HTTP 边界和 26 场景数据库/API 矩阵并部署；Provider 显式允许的传播策略仍未实现，默认禁止外传保持不变。
2. 生产目的端策略及远程认证；当前 dev 仅允许本机 Collector。
3. 生产持久化查询存储；默认开发部署已有受 Docker 权限控制的轮转日志查询，隔离 Docker 四场景关联已通过。
4. 六项逐条审计及最后全量回归以[最终核对](six-capability-final-audit.md)为准。默认部署已启用导出并验证未准入错误；此前历史调用不补造 Trace。
5. Prometheus 兼容采集及调用/容量聚合已通过隔离 API 四场景并部署；不将观测集合当作完整账务。未知消耗/费用不得补零。
6. Adapter 请求子 span 与首业务增量测量已通过完整 API/Collector 验证并部署。上文列明实际阶段覆盖，不宣称所有生产 FR 已完成；Provider 内部 queue/generation 未提供时保留未知，不从数据库时间差或重叠 span 求和猜测。

## 入站上下文及传播边界

HTTP Adapter 按 [W3C Trace Context Level 1](https://www.w3.org/TR/2021/REC-trace-context-1-20211123/) 解析 traceparent/tracestate，再交给应用层结构化上下文：

- traceparent 名称大小写无关；无效、重复、全零身份或禁用版本会重新建链。00 版本严格长度，高版本只读已知前缀；sampling 提示保留为 parent_sampled，不改变本地容量或强制导出。
- 多个 tracestate 按顺序合并，校验键值与唯一性，最多 32 成员；本地只保留不超过 512 字节的完整集合，超出即丢弃集合。无效 tracestate 不丢弃有效 parent；无有效 parent 不保留 state。原始 state 仅存请求上下文，不进入 AccessTrace、repr 或数据库摘要，不能作为任意日志属性。
- 业务头 X-Gateway-Task-Id/Turn-Id/Step-Id 独立于 Trace；非 Agent 调用均可缺省，不编造值。重复、空值、非法字符或超过 128 字节拒绝为 400/invalid_request。头部总计超过 32 KiB 拒绝为 413（计入名称、值及每行 4 字节分隔开销），分派/Provider I/O 前完成检查，错误仍有新 request_id，无 call_id。
- 有效业务 ID 进入 AccessTrace，并以领域 BusinessCorrelation 值随 InvocationAdmission 在同一准入事务写入。0018 为 Invocation 增加 nullable task_id/turn_id/step_id；历史数据不回填，数据库格式约束与原身份不可变守卫阻止绕过 HTTP 校验或事后修改。只读摘要返回这些已持久化字段，不从当前访问头推测历史调用。Agent caller 类型判定/必填要求尚未接入。
- 不从 HTTP 头集合构造 Provider 请求：traceparent、tracestate、业务 ID 默认均不传给 Provider，重试/fallback/流式同样适用。显式允许的 endpoint/已发布 egress+telemetry 策略尚待接线；不提供绕过该策略的全局透传开关。

## 已持久化调用摘要

### 调用方声明的关联要求

模型 HTTP 入口新增 `X-Gateway-Caller-Type`：闭合值 `agent` / `non-agent`，省略等同 non-agent。`X-Gateway-Operation-Scope` 为 `turn` / `step`，省略等同 turn；step 范围仅用于 Agent。头名大小写不敏感，值严格匹配，重复、空值及其他值均拒绝。

Agent 必须提供 X-Gateway-Task-Id 和 X-Gateway-Turn-Id；声明 step 范围时还必须提供 X-Gateway-Step-Id。非 Agent 可省略全部业务 ID，保留 null，不推断或生成任务层级。错误在正文读取、授权/准入及 Provider I/O 前返回 400/invalid_request，仍有新 request_id，无 call_id。

这些头仅声明业务关联语义，不是受信身份、角色或权限，不能判断调用方是否诚实声明自身类型，不能用于授权。业务 ID 经已有准入持久化路径保留；声明本身不加入数据库身份或指标标签，也不改变兼容请求正文。Provider 请求仍按白名单构造，不透传上述声明或业务 ID。当前新增校验尚未部署到默认容器。

`InvocationObservationReader` 是应用层的受限观测读取契约，返回不可变、无正文 DTO。PostgreSQL 实现在独立、有限超时的只读 REPEATABLE READ 事务中读取同一快照，不复用模型 Owner、Attempt 租约或调用截止时间；不在模型响应路径增加查询。可由托管导出管线在请求完成后读取。

- 关联 call_id 与已持久化 trace_id，区分当前 state 与 terminal_outcome；不存在返回 None，尚未结算不伪造终态。
- 每次 Attempt 保留序号、Binding、候选内尝试数、outcome/recovery_action、Usage、锁定计价配置版本及资源 ID；最多读取既有上限 3 次 Attempt。
- 实际模型只采用最后一次 Attempt 的已知事实，不拿较早失败候选填补最终模型。终态、Attempt、流提交和该 Attempt 最新迟到快照的已知模型若冲突，actual_model 留空并标记 model_conflict；不能静默选择一个。正常取消可从流提交恢复模型。
- settled_usage 只读原结算快照。迟到 Usage 独立保留在对应 Attempt 的 late_usage，不加入已结算总量或触发再次计费。缺少父类计数保持 None；只有供应商 total 时保留 provider_reported_total，不推测输入/输出拆分。cached/reasoning 不重复相加。
- 费用直接读取不可变 invocation_cost_summary，按币种分别返回 Decimal 的精确字符串、完整性及确定性；total_cost 未知保持 None。无摘要为空元组，不代表免费或金额为零。此处不重算价格、不合并币种、不改变账务。
- 摘要仅供受限遥测，不添加到普通模型响应，也不包含 Prompt、正文、凭证或原始错误。此实现不是公开查询 API；外部披露仍需原授权边界。

## 有界导出生命周期

- `BoundedTraceExporter` 实现 AccessTraceSink；offer 仅在所属事件循环中同步入队，无 I/O/等待/子任务创建。默认队列 256，上限 4096；仅接收最多 256 span 的 AccessTrace。容量是等待记录数，另有最多一条正在处理，处理后释放引用。
- 单个工作任务以空 Context 启动，不继承授权、当前请求或原始 tracestate。未准入访问不查数据库；已准入访问的摘要必须同时匹配 call_id、trace_id 和业务关联，否则只导出基础访问记录与 identity_mismatch 状态。
- 摘要读取缺失、异常或超时分别标记 missing/unavailable，不保存异常文本；读取失败仍可导出访问 Trace。读取和输出各有默认 2 秒超时。输出失败记录 failed，不重试，不阻止下一条。
- 生命周期关闭后先拒绝新记录，默认最多等待队列排空 5 秒；到期取消正在处理的 I/O 并丢弃余项，记为 abandoned。重复取消仍等待同一清理任务，不遗留后台任务。端口必须异步且配合取消；恶意吞取消或同步阻塞的端口不在此保证内。
- accepted/dropped/exported/failed/enrichment_failed/abandoned 为无标签计数，不使用 request_id 等高基数字段。dropped 是未接收，abandoned 是已接收但停机未完成；exported 表示输出端口正常返回，不推断外部存储的持久化语义。
- Gateway 的显式 trace_output 注入启用该生命周期；与旧 trace_sink 注入互斥。启动在执行资源之外，排空在执行资源释放之后；具体输出端口资源由注入方持有。默认启动器尚未配置输出端口。本轮实现未部署到默认容器。

## OTLP/HTTP JSON 输出

- Adapter 显式投影三类信号，不序列化整个内部对象。遵守 [OTLP JSON 协议](https://opentelemetry.io/docs/specs/otlp/)：Trace/Span ID 为十六进制、纳秒及 64 位整数为十进制字符串、枚举为整数，分别 POST 到三个 `/v1/` 信号端点。
- 请求入口仅采集一次 Unix 时间锚点；各 span 的起止时间由锚点与单调时钟偏移换算，持续时间不受墙钟跳变影响。日志关联根 span，并以白名单 JSON 正文保留调用摘要；实际模型、Usage、Attempt 与费用目前位于日志摘要，不作为 Metrics 标签。
- 输出仅接受显式 origin，禁止 URL 凭证、查询和片段；默认要求 HTTPS，合成测试显式允许明文。禁用重定向、环境代理和压缩响应；每信号请求最多 1 MiB，响应最多 64 KiB，结构解析有深度及节点上限。Collector 诊断正文不保存。此 origin 校验不替代尚待接线的生产目的端准入策略。
- 三个固定信号独立发送，无重试。HTTP 200 的部分拒收仍记录失败；拒收量与仅警告响应分开计数。输出成功仅证明 Collector 接受，不保证后端持久化。
- `gateway.requests` 为固定四种 outcome 的累计计数；`gateway.request.duration` 为固定桶累计直方图；`gateway.telemetry.records` 为六种固定导出状态累计计数。使用进程实例资源身份，不使用请求、业务 ID 或模型作为指标标签。
- 访问指标在有效记录入队判断之前累加，队列满仍计入请求量。快照随后续导出发送，导出状态包含本批发送前的确认数；没有定时或持久化指标通道，停机/持续故障的尾部计数可能丢失，不宣称无损。
- 独立 Collector 0.160.0 已接受合成 Trace/日志/指标；这是输出 Adapter 的真实传输证据，不是默认 Gateway API 的部署验收。复现见 [OTLP 合成验收](../deploy/acceptance/otel-smoke.md)。

## 统一 dev 启动配置

Bootstrap 可选 `telemetry: {"endpoint": "http://127.0.0.1:4318"}`；省略或 null 保持关闭。只接受规范化的 `http://127.0.0.1:<port>` 或 `http://[::1]:<port>`，端口 1–65535。拒绝 DNS 名、外部 IP、隐式端口、URL 凭证、路径、查询、片段及未知配置字段。目的端不是调用方可控字段，不发现环境变量；这个仅本机的 dev 分支不等同于生产目的端政策。

统一 `serve` 通过 `configured_gateway` 持有输出端口，再构造和启动 Gateway。关闭顺序为 Gateway 执行资源、导出队列排空、OTLP HTTP 客户端；构造/启动失败、正文异常及取消也退出外层输出生命周期。原有直接 Gateway 注入仍由注入者持有输出端口。

此配置已在源码及配置/生命周期测试接线，但默认 `deploy/bootstrap.dev.json` 尚未设置 telemetry，默认镜像尚未更新。容器内 loopback 指容器本身；Collector 必须共享网络命名空间，不能把宿主机 Collector 的端口当成容器 loopback。生产远程目的端、默认部署及完整模型 API 关联验收仍需完成。

## 隔离完整 API 验收（2026-09-13）

最新部署增量：默认 Bootstrap/Compose 已开启仅 loopback 的 Collector，三类信号进入容量受限的本机 Docker 日志；见下节。下文镜像状态描述保留为隔离验收时的历史记录。

当前源码镜像已构建并用于隔离 Docker 项目；默认运行容器尚未更新。正常统一启动器读取本机 telemetry 配置，实际 PostgreSQL 摘要进入有界队列并由 Collector 0.160.0 接收。同步恢复、失败流、断连、流式恢复四次调用的 call_id、actual_model、Attempt 数、HTTP 根 span、Attempt spans 和持久化终态通过核对；成功流首增量时间和三类固定指标存在。独立 SQL 读得 completed/failed/cancelled/completed。失败消耗未知时全调用总量保持未知，最后成功 Attempt 的 10+2 tokens 独立保留。证据与复现见[Docker 验收](../deploy/acceptance/README.md#完整观测链路)。这不证明默认部署已启用或生产远程策略已完成。

## 默认开发部署启用（2026-09-13）

最新状态：下方标记“尚未部署”的增量已于本日后续完成隔离 Collector/Prometheus 复验与默认部署；这些小节保留原实现时边界说明。默认采集端点为共享 loopback `127.0.0.1:9464/metrics`，无宿主发布。四场景实际核对 8 Attempts、2 retries、2 fallbacks、已知输入 20/输出 4、4 个未知费用记录及容量上限；Gauge 使用 `{item}` 防止被名称转换为比例。默认 readyz=200，Agent 缺 ID 请求 `48742881-6dab-46b6-8f16-177bca82fa21` 返回 400，调用表仍为 8。阶段测量完整性仍未完成。

### 容量资源观测增量（尚未部署）

实际 AdmissionCapacity/AttemptCapacity 在原有短临界区维护使用量、历史峰值及容量拒绝次数；读取返回固定字段副本，不列出租户、Provider 或 Binding 身份。准入上限为现有 200；租户拒绝和实例拒绝按实际失败 gate 区分。Attempt 可用性预检查不算拒绝，只有 try_acquire 真正失败计数；未合成一个不成立的跨版本 Provider 总上限。

FullServiceTextProcess/Backend 提供同步无 I/O 快照，Gateway 将其注入导出 worker；另输出待处理队列深度和队列上限，不把正在处理的一条计入等待队列。输出使用 `gateway.resource.*` Gauge：执行 Owner 重建可令峰值/拒绝读数重置，因此不伪装成 exporter 全生命周期单调 Counter。无执行后端时缺省这些读数，而非伪造完整空闲状态；读取异常不改变调用或导出终态，不保存异常内容。

快照随访问记录导出，不是定时采样；当前值可能错过短暂峰值，进程内历史峰值和拒绝次数保留已经观测到的压力。它覆盖容量 gate 与导出队列，不冒充 CPU/内存、连接池或所有背压指标。Prometheus 接收和完整部署验证仍待完成。

### 调用观测聚合增量（尚未部署）

有界导出 worker 在摘要身份核对成功后、发送之前，按一次访问的观测增加 `gateway.observed.*` 累计指标。覆盖观测数量/终态、Attempt 数、实际 retry/fallback 转换、各 Token 类型的已知计数及 known/unknown 测量次数、费用记录可用性、已知费用。摘要缺失不伪造一次调用计量。发送失败不回滚已观测累计值，后续快照可携带这些累计值。

retry 仅计连续实际 Attempt 使用同一 Binding，fallback 计连续实际 Attempt 切换 Binding；只记录 retry 意图但未真正开始下一次不计。input/output/cached/reasoning 各自独立；不把子集再加成总量、不把最后一次 Attempt 冒充全调用总量、不再累计 late_usage。未知字段增加未知次数，不输出伪造的零消耗。

费用来自持久化调用费用摘要，按币种、完整性、确定性区分。内部 Decimal 累加，OTLP 数值投影为浮点近似，仅供趋势观测；精确账务仍以 Ledger/日志精确字符串为准。每进程最多 16 个币种，超出增加 cost_currency_overflow 并跳过该费用，不混合币种。没有任何费用摘要时增加 cost_records_missing。未知金额只增加 unknown 记录数，不产生金额样本。

这些是成功富化的访问观测集合，不是完整数据库或无损账务聚合：入队丢弃、缺失摘要或进程退出会造成覆盖缺口；重放/重复访问不能用它推断唯一调用计费。指标不携带请求、租户、模型、Binding 或价格资源标签；原有 dropped/enrichment_failed 等计数应一起观察。Prometheus 接收验证、资源饱和度及默认部署仍待完成。

默认 Gateway 已更新至隔离验收通过的源码镜像，Collector 0.160.0 共享其网络并仅监听 127.0.0.1:4318，无发布端口、无远程导出、无业务凭证挂载。Docker 日志上限 10 MiB × 3 文件；Docker 运维者可查询，轮转或删容器即可能丢失，不视为生产存储。

readyz=200；非法业务 ID 的准入前请求返回 400，无 call_id。请求 `7d21a8e9-7f9e-4080-9715-199237dc724a`、入站 Trace `1234567890abcdef1234567890abcdef` 在 Collector 中对应 Error 根 span 和 not_admitted 日志，指标输出存在。数据库调用数部署前后均为 8，无 Provider 调用，既有配置/Prompt/数据卷保留。操作与停用方法见[Docker 文档](docker-quickstart.md#本机观测输出开发部署)。
