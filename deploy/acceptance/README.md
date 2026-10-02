# 隔离 Docker 双候选恢复验收

在仓库根目录执行。先运行 `docker compose build` 构建当前源码镜像；验收项目使用独立名称
`gateway-recovery-acceptance`、内部网络、临时数据库和合成凭证。不读取 `deploy/local`，不发布宿主端口，不访问真实供应商。

```powershell
docker compose -f deploy/acceptance/compose.yaml up -d
docker compose -f deploy/acceptance/compose.yaml logs verify
docker compose -f deploy/acceptance/compose.yaml ps -a
# verify 退出 0、Collector 完成接收后核对遥测
.venv/Scripts/python.exe deploy/acceptance/verify_telemetry.py

# 在相同隔离项目中，完成原四调用与遥测核对后再运行；只调用合成 Provider
docker compose -f deploy/acceptance/compose.yaml run --rm --no-deps verify-limits

# 同一全新隔离项目中，最后验证第二种上游协议及 Schema 特性预检；仅使用合成凭证 C
docker compose -f deploy/acceptance/compose.yaml run --rm --no-deps verify-protocols
```

`verify-protocols` 在已发布配置上新增独立的 `messages` Alias、`anthropic_messages/v1` Binding 和合成 Provider C，不修改 `general` 的候选或五次/分钟限额。开放对象 Schema 应在 Provider I/O 前返回 422；显式 `additionalProperties: false` 的 Schema 应发到 `/v1/messages`，并将合成响应和 5/3 Token 用量映射回统一协议。它只证明 Docker/API 上两个协议的路由与映射，**不是真实 Anthropic 调用证据**；运行前须先完成 `verify`、`verify_telemetry.py`、`verify-limits`。无第二协议凭证时不要将此结果表述为真实供应商验收。

`verify-limits` 要求前四次逻辑调用和 8 次上游请求已经由 `verify` 完成；它核对首次请求中的同候选重试及跨候选恢复保留相同意图，且首个同候选重试遵守合成 Provider 的一秒 `Retry-After`，然后发送第五次合法请求。第六次必须在 Gateway 返回 JSON 429 / `rate_limited`，没有 call_id，且两个合成 Provider 的请求记录不增加。先运行 `verify_telemetry.py`，因为它按原四次调用核对精确计数。该演示使用正常 Docker Gateway/HTTP/配置/限速路径，真实 Provider 调用为零；指数退避及全抖动的数值边界仍由策略测试精确验证。

`verify` 必须退出 0，并输出 `status=passed`：主候选返回 503，两次 Attempt 后切换第二供应商，第三次成功。
现在验收同步恢复、输出后截断、客户端断连和流式恢复四次调用，分别输出通过记录（mode 为 broken/disconnect/stream；同步记录无 mode）。验收容器通过 `--enable-streaming` 同时开启模型 API 和配置能力验证。
流式调用前发布另一个 Prompt 版本，再显式引用旧版本；验证三个 SSE 帧（增量、已结算终态、DONE）、稳定调用身份、实际模型和原 Prompt 版本。两次调用各有三个上游请求，同一调用内的意图摘要必须一致。
验证逻辑 Alias 不变、实际模型明确披露、三个请求的 Prompt/采样参数摘要相同、恢复摘要来自数据库以及上游私有字段未透传。
Gateway 使用正常启动、迁移、DNS/egress、HTTP Adapter、准入/路由、凭证、恢复、终态结算路径，没有 MockTransport 注入。

本项目的网络只允许容器内部通信；Provider 都是测试 HTTP 服务，不能用于质量评估或真实供应商可靠性结论。
认证失败、uncertain、预算耗尽、Retry-After、在途模板发布等分支由 `tests/test_gateway_recovery.py` 的独立 PostgreSQL/API 测试验证。
该矩阵同时覆盖同步和流式。Docker 断连夹具确认主供应商观察到连接关闭；输出后截断必须收到显式错误而非成功 DONE，两个故障都不得访问后备供应商。运行结束共 8 次上游请求（主供应商 6、后备 2）。数据库中的 failed/cancelled、唯一终态及取消原因还应独立核对，不能仅凭上游关闭证明结算完成。真实调用方 TCP 的 deadline 与流式停机排空另有专项。

核对结果后清理这个独立项目（明确删除的只有验收夹具）：

```powershell
docker compose -f deploy/acceptance/compose.yaml down -v
```

数据库使用 tmpfs，停止后不可恢复；命名 `state` 卷只存合成凭证和临时指纹材料。不要省略 `-f`，不要对默认开发项目运行 `down -v`。
重新验收前先清理此项目，避免在旧验收实例中重新生成材料。

## 完整观测链路

Adapter 子 Span 验收已加入同一检查器：每个 Provider Attempt 的 ID 必须恰好作为一个 `adapter_request` 的 parent ID；Adapter kind 为 Client，同步不携带首增量计时，三个流式场景仅实际输出的 Adapter 有首增量及后续观测时段；失败/断连 Adapter 必须为 Error。2026-09-13 四场景复验通过，调用分别为 `a98c1fff-c772-430a-941f-ae350238602f`、`b6e28ba9-134e-4855-9b5d-2f8a76c94dfa`、`bce2905a-38f9-47cb-b4d8-6357a20b00ea`、`0379f66b-f4af-4d7f-a39c-f748f85632ae`；Prometheus 数值断言同时通过。

现已包含 Prometheus 验证：Collector 在共享 loopback 的 9464 端口提供 `/metrics`，不发布宿主端口。宿主运行 `verify_telemetry.py` 会通过隔离 Gateway 容器读取该端点，要求 8 Attempts、2 retries、2 fallbacks、已知输入 20/输出 4、4 条未知费用记录，且无伪造金额；准入/队列上限分别为 200/256，延迟直方图存在，无调用身份标签。该检查只读，不发送额外模型请求。

2026-09-13 新指标接收复验通过，四个调用为 `e0f671d5-2f26-4916-980b-7296db1e7ced`、`242de538-3d0a-40da-a6f0-87de36daadff`、`664d6d16-68b8-469a-913b-44010a840f79`、`467be6af-2da4-436c-804f-cfce44a77909`。首次采集发现 Gauge 单位 1 被转换为 ratio，已将数量单位修正为 `{item}` 后重建、清空隔离夹具并完整复验通过；不是删除断言绕过差异。

prepare 仅对隔离 Bootstrap 开启本机 OTLP；Collector 0.160.0 共享 Gateway 网络命名空间，端口不发布到宿主机。Gateway 使用正常统一启动器和真实只读数据库摘要导出，不注入测试 Sink。Collector debug 仅接收本项目合成数据，不用于生产存储。

`verify_telemetry.py` 在宿主读取此项目 verify/collector 日志，匹配四个 call_id，要求摘要 available、终态/actual_model/Attempt 数一致、每次调用有唯一 HTTP span 和相应 Attempt spans、失败/取消根 span 为 Error、请求身份一致、流式首增量时间存在、固定指标存在，并检查合成凭证/正文未泄露。缺失记录会失败；若刚结束调用尚未导出，可在同一实例再次运行此只读检查，不重放模型请求。

成功恢复的最后 Attempt 为 10 输入 + 2 输出 = 12 tokens，但前两次失败消耗未知，因此全调用 Usage/Cost 必须保留未知。不能将最终 Attempt 的已知量冒充整次调用总量。

2026-09-13 完整链路通过：同步 `e9184f70-1660-49be-b9e5-c09ad1e34229`、失败流 `42d36c23-720f-411b-b075-59b726b1fa84`、断连 `70549a55-d8de-4e60-b747-2ea38d74a30b`、流式恢复 `4e424ddb-b75e-4190-b35d-3cbc5cfd9538`；独立 SQL 读得 completed/failed/cancelled/completed，与导出一致。8 次合成上游请求，真实 Provider 调用 0。缺失 HTTP span、错误模型、缺失指标、凭证泄露四种日志变异均被检查器拒绝。
