# Docker/API 文本与流式链路

本入口已组装 dev 模式的同步及 SSE 文本服务，不代表全部 v0.1 已完成。
生产模式、Tools、Structured Output、缓存/降级、幂等重放、状态/取消 API 和完整主动探针调度尚未接入。
`production`、未知启动模式、跨分支鉴权配置都会拒绝启动，不会退回 bypass。

## 首次启动

需要 Docker Compose 和 Python 3.12+。在仓库根目录运行：

```powershell
python -m pip install -e ".[test]"
python -m llm_gateway.infrastructure.development_init --provider-from-env-file .env.local
docker compose up -d --build
python deploy/smoke.py
```

`development_init` 是公开的宿主初始化命令，不连接数据库或 Provider。它从显式选择的开发环境源读取
`DEEPSEEK_API_KEY`（进程环境优先），创建 `deploy/local/` 中的数据库密码、连接文件、32 字节 Fingerprint Key
和版本化 Provider Secret。原环境文件不变。没有凭证时可省略 `--provider-from-env-file`，但生成的 Provider
记录明确为 revoked，不能完成真实模型调用。初始化拒绝覆盖任何已有 Secret。

该目录已排除出 Git 和 Docker 构建上下文，运行时只读挂载。请与数据库卷一起保存；不要在已有数据库上重新生成同版本
Fingerprint Key。Windows 若由沙箱用户创建文件，Docker Desktop 用户也需要这个目录的读取权限；只向该用户授权，
不要赋予 Everyone。Linux 开发 Compose 为读取宿主 mode-0600 Secret 使用容器 root，同时关闭 Gateway 的所有 capabilities、
启用 no-new-privileges 和只读根文件系统；这不是生产权限部署方案。

默认只启动 PostgreSQL、独立一次性 `gateway-migrate` 和 Gateway，不启动 Redis或模拟 Provider。
数据库不发布宿主端口。Migration Job 最多等待数据库 60 秒，按版本迁移并核验；失败会阻止 Gateway 启动。
Gateway 本身只核验 Schema，绝不自动迁移或发布配置。

## API 与显式发布

- 模型 API：`http://127.0.0.1:8000/v1/chat/completions`
- 管理 API：`http://127.0.0.1:8001/gateway/v1/config/...`
- 健康：`http://127.0.0.1:8001/healthz` 和 `/readyz`，模型端口没有健康或管理路由。

容器内分别监听 `0.0.0.0:8000/8001`，宿主默认仅绑定 `127.0.0.1`。可通过 `GATEWAY_MODEL_PORT`、
`GATEWAY_MANAGEMENT_PORT` 改宿主端口；不要将免鉴权 dev 服务发布到不可信网络。

空数据库启动后 `/healthz` 为 200、`/readyz` 为 503，配置恢复 API 可用。
`deploy/smoke.py` 仅通过上述 HTTP API 完成：检查空库 → validate → 创建 Candidate → 显式 publish → 等待 ready → 一次模型请求。
它不读取 Secret、不执行 SQL、不覆盖已有 Active，也不自动重试模型调用。每次执行可能计费。
已有 Active 时，仅重新调用模型可用：

```powershell
python deploy/smoke.py --invoke-only
```

## 已有配置时的人工验证（PowerShell）

已完成首次发布的环境无需再次初始化 Secret 或重复执行首次发布。先在仓库根目录检查服务：

```powershell
Set-Location E:\personal\llm-gateway
docker compose up -d
docker compose ps
(Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8001/readyz').StatusCode
```

期望就绪检查返回 200；浏览器打开该地址时显示空白页面正常。503 表示尚不可调用，先检查 Active 发布状态及 Gateway 日志。
下面的 HTTP 命令可在任意目录执行。只复制代码内容，不要复制 `PS ...>`、`>>`、Markdown 围栏或 `diff` 标记。

### 发送一次中文请求并明确按 UTF-8 解码

```powershell
$body = @{
    model = 'general'
    messages = @(
        @{ role = 'user'; content = '你好，请简短介绍一下自己。' }
    )
    max_tokens = 2048
} | ConvertTo-Json -Depth 5

$request = @{
    Uri = 'http://127.0.0.1:8000/v1/chat/completions'
    Method = 'Post'
    ContentType = 'application/json; charset=utf-8'
    Body = [System.Text.Encoding]::UTF8.GetBytes($body)
    UseBasicParsing = $true
    ErrorAction = 'Stop'
}
$response = Invoke-WebRequest @request
$json = [System.Text.Encoding]::UTF8.GetString($response.RawContentStream.ToArray())
$result = $json | ConvertFrom-Json
$result.choices[0].message.content
```

执行 `Invoke-WebRequest @request` 会产生一次可能计费的模型调用；之后解码、查看 `$json` 和读取 `$result` 不会再次调用模型。
`max_tokens=2048` 是此次人工验证的输出预算示例，不保证模型一定在该预算内完成推理和正文输出。

### 检查返回结果

```powershell
$response.StatusCode
$response.Headers['Content-Type']
$result.choices[0].finish_reason
$result.usage
$result.choices[0].message.content
# 如果仍有疑问，查看现有完整响应，无需重新发送请求：
$json
```

不要只检查 HTTP 200：还应确认最终正文非空、中文可读，并检查 `finish_reason`。

| 现象 | 检查与处理 |
| --- | --- |
| 中文显示为 `ä½ å¥½` 等乱码 | 使用上面的原始响应字节 → UTF-8 → JSON 解析。请求的 Content-Type 只指定请求编码，不能保证客户端正确解码响应；仅改字体或执行 `chcp 65001` 无法修复已解码错误的字符串。 |
| HTTP 200，但 `content` 为空且 `finish_reason=length` | 检查 `completion_tokens_details.reasoning_tokens`。本次曾出现 512 个输出 tokens 全部用于推理、正文尚未产生即达到上限的情况。可在已发布配置允许的上限内提高 `max_tokens`，再发送一次请求；这可能增加费用，且不保证一定有正文。当前 Gateway 未提供关闭 DeepSeek 思考模式的请求参数。 |
| `$json` 中有正文，但读取属性没有输出 | 属性必须写成 `$result.choices[0].message.content`。`conten` 少了最后的 `t`，不会读到正文；修正属性名即可，不必重新请求。 |

### 本次人工验收记录（2026-09-13 整理）

用户最终贴出的响应并经确认：HTTP 200，Content-Type 为 `application/json`，`model=deepseek-flash`，
`finish_reason=stop`，中文正文为：

> 你好！我是一个 AI 助手，可以帮你解答问题、写作、翻译、编程和整理信息。有什么我可以帮你的吗？

该响应 Usage 为输入 37、输出 238、合计 275 tokens，其中 reasoning tokens 为 210（是输出的子集，不应再加到总数中）。
此前遇到的中文解码、输出预算耗尽和 `content` 属性拼写问题已在上述流程中记录。
此记录证明本次 dev 同步文本链路和中文显示已跑通，不代表其他 v0.1 能力已经验收。

dev 模式忽略 Authorization Header，固定使用 `tenant_id=dev`、`subject=dev` 的 FR-927 Context；并发、限流、
配置/资源校验、Secret 有效性、TLS、出站 allowlist 和持久化检查仍执行。

## 示例配置的边界

`deploy/deepseek.bundle.json` 将逻辑 Alias `general` 指向 `deepseek-v4-flash`，只声明本 Gateway 已实现的文本能力。
其 8192 上下文和 4096 输出上限是保守的本地配置，不是 Provider 的最大能力。
示例只允许一次 Attempt，无缓存/降级，主动探针为 null；被动 Attempt/Circuit 采样仍启用。
当前入口对启用主动探针的 Provider 返回 health-unavailable，不会伪造探针成功。

模型名与价格依据 [DeepSeek 官方定价页](https://api-docs.deepseek.com/quick_start/pricing/) 于 2026-09-12 核对。
示例配置固定采用当日 off-peak USD 费率（每百万 tokens：未缓存输入 0.22、缓存输入 0.007、输出 0.66）；
Gateway 不自动选择峰谷费率。该价格表是本地估算基础，不是账单或精确对账结果，实际使用前请按当前费率和时段发布新版本。

## 停止、重启与核验

```powershell
docker compose ps
docker compose logs --tail 20 gateway gateway-migrate
docker compose run --rm gateway-migrate
docker compose restart gateway
docker compose stop
```

重复迁移为 no-op；重启保留配置、身份密钥和已完成调用，不重新发送旧请求。
停机先关闭准入并在至多 30 秒预算内排空/取消，再释放执行所有权、连接池和线程资源。
首次 Model 启动的关键依赖失败或执行所有权丢失后，需要修复并重启；管理 API 仍可恢复配置。
`/readyz` 只读取本地状态，不做数据库或 Provider I/O；后台配置观察和所有权检查负责更新状态。

`docker compose stop` 保留数据。不要使用 `down -v`，除非明确准备删除整个开发数据库。
修改示例配置不会自动覆盖 Active，必须创建并发布新的 Candidate。
CLI 也可直接运行 `gateway --bootstrap <本机JSON路径>`，其中所有 Secret 路径须改成宿主绝对路径，端口默认用回环绑定。

## Prompt 资产与固定版本调用（2026-09-13）

统一 dev Gateway 现在支持 Gateway 自己的 Prompt 管理和模型请求扩展，详见 [模板/API 契约](prompt-template-contract.md)。旧 `messages` 调用保持可用。Prompt 最初引入于 `0015_prompt_assets.sql`，当前代码要求 `0018_invocation_correlation.sql`；已有数据库应先构建镜像、停止模型服务，再显式迁移和启动，保留数据卷：

```powershell
docker compose build
docker compose stop gateway
docker compose run --rm gateway-migrate
docker compose up -d gateway
```

每步成功后再执行下一步；迁移失败不要跳过。健康检查确认 ready 后，执行可复现验收：

```powershell
# 创建一个测试资产、两个不可变版本，验证发布冲突和旧版本渲染；不调用模型
.venv\Scripts\python.exe deploy\prompt_smoke.py

# 相同管理验证，再发起一次真实固定版本调用（会产生少量费用）
.venv\Scripts\python.exe deploy\prompt_smoke.py --invoke
```

每次运行会新增测试资产，不覆盖已有配置或资产。脚本不读取凭证、不自动重试模型请求，返回资产/版本 UUID 便于复查。management API 的 render 只接受已发布过的固定版本；新的发布不会让旧版本调用改用新内容。

本轮 `--invoke` 已成功：call_id `3fb458eb-508d-47e6-bfc1-2e31ffc66cd4`，deepseek-flash，stop、非空正文，输入 37 / 输出 12 / 合计 49 tokens。数据库核对同一版本与 completed 终态。当前发布版本与调用版本不同，验证了固定旧版本选择。该结果不代表 SSE、完整 Trace 或 fallback 已完成。

## 同步恢复验收

另有独立的 [双 Provider Docker 验收夹具](../deploy/acceptance/README.md)，不覆盖真实 DeepSeek 配置、不读取真实凭证、不消费真实调用额度。它通过内部网络和合成供应商验证两次主候选失败后切换后备候选，保持 Prompt/参数不变。

默认 Gateway 现从 Bootstrap `provider_secret_files` 注册已知凭证引用，可显式挂载不同供应商的独立凭证。不要把示例的同一个 DeepSeek 凭证当成所有供应商共用凭证；某个材料版本失效后，引用它的后备候选也不会执行。

成功响应新增 `gateway.recovery`，已结算调用的响应头包含 `X-Gateway-Attempts`、`X-Gateway-Retries`、`X-Gateway-Fallback-Used`。见 [恢复契约](recovery-contract.md)。这些来自数据库事实，但尚不是完整 Trace 或 SSE 实现。

## 本机观测输出（开发部署）

默认 Bootstrap 已显式开启 `http://127.0.0.1:4318`，Compose 的 Collector 0.160.0 共享 Gateway 网络命名空间，只监听 loopback，无宿主端口或外部导出。构建当前镜像后使用 `docker compose up -d gateway collector` 同时更新这两个服务；单独重建 Gateway 后也要重建 Collector，避免共享旧容器网络。

```powershell
# 查询最近本机观测记录；Docker 权限等同于受限运维访问
docker compose logs --since 10m collector
# 按本次响应的 X-Request-Id 或调用 ID 定位
docker compose logs --since 10m collector | Select-String '替换为请求或调用ID'
```

输出包括 spans、白名单调用摘要及固定维度指标，不包含消息/Prompt 变量/凭证。日志仍有请求及业务关联身份，不应公开转发。Docker 日志按 10 MiB、最多 3 文件轮转；不是生产持久化查询后端，容器删除/轮转会丢记录。Collector 晚启动、故障或导出队列满也可能丢失遥测，模型请求不因此等待或失败。

关闭时将 Bootstrap 的 telemetry 设为 null，重建 Gateway 后停止 Collector；不要删除数据库卷。生产远程 Collector、认证及持久化后端尚未接线。2026-09-13 默认部署 readyz=200，准入前非法业务 ID 请求的 400、Error span 与日志身份一致，调用表仍为原 8 条，无 Provider 请求。

### Prometheus 兼容采集

Collector 现同时在共享容器 loopback 的 `127.0.0.1:9464/metrics` 暴露 Prometheus 文本，不发布宿主端口。可由同网络命名空间的受信采集器拉取，或用以下只读命令诊断：

```powershell
docker compose exec -T gateway python -c "import httpx; r=httpx.get('http://127.0.0.1:9464/metrics',trust_env=False); r.raise_for_status(); print(r.text)"
```

指标含请求 outcome/延迟直方图、观测集合的重试/fallback/已知 Token/费用可用性、准入与 Attempt 容量 Gauge。OTLP 点号规范化为下划线，累计 Counter 带 `_total`，延迟桶为 `gateway_request_duration_seconds_bucket`。容量是数量，不是 ratio。费用是趋势近似，不替代 Ledger；没有样本不代表金额为零。独立 Prometheus 服务与存储不由 Gateway 强制部署。

Agent 请求使用 `X-Gateway-Caller-Type: agent`，必须带 Task-Id/Turn-Id；独立操作声明 `X-Gateway-Operation-Scope: step` 并带 Step-Id。声明只用于关联完整性，不提供认证权限。此校验和新增指标已部署；缺 ID 请求拒绝且数据库调用数仍为 8。

## 流式入口使用

分钟请求限速：每个逻辑 `model` 默认 **5 次/滚动 60 秒**，同步和 SSE 共用；超限在流开始前返回 JSON 429。仅在模型定义 `model_aliases.<model>.requests_per_minute` 配置正整数，发布后生效，不能在请求体覆盖。配置与计数规则见[模型 RPM](model-rate-limit.md)。该新增能力需要运行包含本次改动的镜像；历史部署记录不代表自动更新。

默认 Compose 的 Gateway 命令已包含 `--enable-streaming`，同时开启 API 和配置能力校验。直接运行 `gateway` 时需显式传入该参数；不传仍关闭。部署前仍需构建当前镜像并优雅重启服务。

开启入口不等于现有 Binding 已支持流式。必须通过配置 API 创建并发布声明 `capabilities.streaming=true` 的候选版本；不要直接改数据库，也不要对未经验证的模型盲目声明能力。默认开发数据库现已通过 API 发布 revision 2，仅把 deepseek-text 的流式声明改为 true，并完成一次真实调用。用于初次同步 Smoke 的示例 bundle 仍为 false；从该示例新建的环境需显式发布流式能力，否则返回 422，不会静默改成同步调用。

已声明能力时，使用原有 messages 或固定 Prompt 请求并设置 `stream=true`，Accept 使用 `text/event-stream`（或省略）。成功的最终帧携带 Prompt、Usage 和 recovery，随后才有 DONE；输出后失败则收到错误帧并关闭，没有成功 DONE。尝试总数在流开始时尚未知，不能依赖响应头预先提供最终 recovery。详见[流式契约](streaming-contract.md)。

### Docker 启动后直接请求 HTTP SSE（PowerShell）

在仓库根目录的宿主机 PowerShell 中执行，不需要进入容器，也不需要在请求中填写 DeepSeek 密钥。以下针对默认本机 dev 部署；生产授权模式仍需有效的 Gateway 凭证。端口 8000 为模型接口，8001 为管理/健康接口，仅绑定宿主机 loopback。

1. 确认服务就绪：

```powershell
docker compose ps
curl.exe --noproxy "*" -i "http://127.0.0.1:8001/readyz"
```

Gateway 应为 healthy，readyz 应返回 200。新数据库需要先完成本文前面的配置发布流程；仅容器启动不代表模型配置可用。

2. 向模型接口发起 SSE 请求。此步骤会产生真实模型费用；示例使用英文以避开旧版 PowerShell 管道的中文编码差异。

```powershell
$body = '{"model":"general","stream":true,"max_completion_tokens":2048,"messages":[{"role":"user","content":"Count from 1 to 10."}]}'
$body | curl.exe --noproxy "*" -N -i "http://127.0.0.1:8000/v1/chat/completions" -H "Content-Type: application/json" -H "Accept: text/event-stream" --data-binary "@-"
```

必须使用 `curl.exe`，避免 Windows PowerShell 的 `curl` 别名；`-N` 关闭客户端输出缓冲，`-i` 显示响应头，`--noproxy "*"` 避免本机请求经过环境代理。SSE 仍是 `POST /v1/chat/completions`，只是请求体设置 `stream=true`；不是另一个 GET 接口，也不是 WebSocket。不要用普通 JSON 的 `choices[0].message.content` 方式解析整个响应，流式正文位于各 JSON 帧的 `choices[0].delta.content`。

3. 检查响应，而非只看 HTTP 200：

- 响应类型为 `text/event-stream`，可看到空行分隔的 `data: {...}` 帧，正文增量非空。分片数量和大小不固定，不保证逐字输出。
- 本示例正常完成应收到 `finish_reason: "stop"` 的终态帧，随后收到 `data: [DONE]`。保留响应头中的 `X-Gateway-Call-Id` 便于查询调用。
- 若出现 `error` 帧或异常截断，应按失败处理；输出提交后不能切换模型，失败流不发送成功 DONE。HTTP 200 和 curl 正常退出都不能单独证明调用成功。
- 若终态为 `length`，说明输出预算耗尽，不等于完成了示例任务；推理 Token 也可能占用输出预算，不应只检查总 Token 是否非零。

若返回 422 或提示不支持 streaming，先检查已发布 Binding 的流式声明；确认模型支持后，按下节显式发布。若返回非 SSE 错误响应，先按 HTTP 状态和错误码处理，不把它当作流帧。前端可使用 `fetch()` 发 POST 并读取响应流；原生 `EventSource` 只发 GET，不能直接调用此接口，跨源前端还需满足部署的访问策略。

### 自动验证 SSE

以下脚本同样从宿主机访问 Docker 的 8000/8001 端口，只通过 Gateway API 工作，不读取凭证。需要本地已安装项目依赖的 `.venv`；上面的 curl 请求不依赖 Python。

```powershell
# 只检查当前配置，不发布、不调用模型
.venv\Scripts\python.exe deploy\stream_smoke.py
# 输出 streaming=true 后，执行一次真实流式验证（可能产生费用）
.venv\Scripts\python.exe deploy\stream_smoke.py --invoke
```

只有检查显示 streaming=false 且已确认模型支持时，才用下面这条代替 `--invoke`；它会发布新配置并发起一次真实请求，不必两条都执行：

```powershell
.venv\Scripts\python.exe deploy\stream_smoke.py --publish-streaming --invoke
```

脚本成功输出 `"status": "passed"`、call_id、actual_model、非零 content_bytes、首增量/总耗时、usage 和 recovery。它验证响应类型、非空正文、帧身份、终态顺序及 stop/DONE；只显示最终摘要，不逐字打印内容。上面的 curl 与脚本各自会发起请求，按需要选用，不是必须连续执行。

发布保持其他配置不变，并以旧 Active 为 CAS 前提；客户端没有自动重试，实际 Attempt 数看最终 recovery。真实调用 28fea5f1-3f6f-462f-a964-7d2830fb53d0 已验证 completed、deepseek-flash、非空正文、stop/DONE，单次 Attempt，37 输入 + 15 输出 = 52 tokens（reasoning 13 是输出子集）。数据库费用为 partial/estimated，不能将缺失总费用补零。脚本输出的客户端首增量/总耗时不是服务端分阶段 Latency；当前服务端观测边界见[观测契约](observability-contract.md)。

成功请求不能替代故障验收。验证输出后截断、客户端断连及恢复边界时，使用[隔离 Docker 验收](../deploy/acceptance/README.md)，不调用真实模型。
