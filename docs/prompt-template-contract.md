# Prompt 模板领域契约

本页记录 ADR-0148 的基本实现契约。领域规则、PostgreSQL 存储、统一 dev Gateway 管理 API 和同步调用接入已实现；完整六项能力的验收仍以总清单为准。

## 模板与渲染

- 一个版本包含租户、资产 UUID、版本 UUID 和不可变的文本消息序列。消息角色沿用 Gateway 的 system/developer/user/assistant；模板不能改变角色。
- 语法仅支持 `{{name}}`；name 为 ASCII 字母或下划线开头，后续允许字母、数字、下划线，最长 64 字符。双花括号不支持表达式、空白修饰、属性访问、索引、循环或函数调用。单花括号作为普通文本，可直接写 JSON。
- 变量从占位符推导，最多 64 个。调用必须提供完全相同的变量名集合，值只能是 UTF-8 字符串；不隐式转换、忽略多余变量或补空值。
- 替换只执行一遍。变量值中的占位符、反斜杠或代码均作为普通文本，不进行递归渲染或执行。这是代码执行边界，不意味着可以消除语义上的 Prompt injection。
- 最多 100 条消息；单条模板、变量值、渲染消息上限 64 KiB；模板消息总量、变量值总量、渲染消息总量分别上限 256 KiB，按 UTF-8 字节计算。先计算展开大小，再分配结果，避免重复变量造成内存放大。
- 结果固定资产/版本身份与消息 tuple；调用方修改原变量字典不影响结果。模板正文、变量名集合和渲染正文不进入对象 repr；错误只包含封闭原因码。

这些是模板子系统的绝对上限，不取代入口 body 限制、当前配置的资源/安全检查或模型上下文限制。后续调用集成必须继续执行这些检查。

## 资产、版本与发布事务

- 租户身份只来自鉴权上下文，不能通过请求正文指定。read/write/publish/use 分别要求 `gateway.prompts.read`、`gateway.prompts.write`、`gateway.prompts.publish`、`gateway.prompts.use`；固定 dev 身份仍使用原来的显式 dev_bypass。过期上下文失败。未知资源和跨租户资源统一返回 404。
- 创建资产与首个版本在同一事务内提交，资产/版本 UUID 由服务器生成。增加版本不修改已有版本，不自动发布。模板正文仅存于受保护的 `prompt_version.messages`。
- 发布锁定租户下的资产行，比较 `expected_generation`，写不可变发布历史并更新发布指针，单次事务提交。初始 generation 为 0；发布后加一。并发相同期望序号只能有一个成功，其他返回 409。不采用 last-write-wins。
- `prompt_version`、`prompt_publication` 的 UPDATE/DELETE 在数据库触发器层拒绝。表间复合外键防止跨资产/租户关联。模板不能隐式发布模型配置。
- 渲染与模型调用必须给出具体资产/版本 UUID，且该版本至少显式发布过一次。新发布不会撤销历史已发布版本，也不会改变在途调用。没有 `latest` 动态选择或隐式自动发布。
- 管理写入不提供幂等命令重放。不要自动重试结果不确定的 create；publish 可读取 generation/版本检查结果。`Idempotency-Key`、`X-Gateway-Command-Id`、条件请求头不受支持并明确拒绝，不借用配置命令协议。

## 管理 API（默认端口 8001）

JSON 请求/响应，UTF-8；严格拒绝未知/重复字段。请求体上限 1 MiB，处理预算 5 秒；无缓存。UUID 使用规范的小写带连字符形式。错误只返回安全原因码，不回显输入、SQL 或校验器原始错误。

| 方法与路径 | 请求 / 返回 |
| --- | --- |
| POST `/gateway/v1/prompts` | `{"messages":[{"role":"user","content":"你好 {{name}}"}]}` → 201，asset_id/version_id/messages/variables |
| GET `/gateway/v1/prompts/{asset_id}` | 返回 asset_id/generation/published_version，未发布为 null |
| POST `/gateway/v1/prompts/{asset_id}/versions` | 同创建的 messages DTO → 201，新版本 |
| GET `/gateway/v1/prompts/{asset_id}/versions/{version_id}` | 返回不可变版本、模板消息和变量名 |
| POST `/gateway/v1/prompts/{asset_id}/publish` | `{"version_id":"UUID","expected_generation":0}` → 更新后的资产状态；过期期望值 409 |
| POST `/gateway/v1/prompts/{asset_id}/versions/{version_id}/render` | `{"variables":{"name":"世界"}}` → 版本身份和渲染消息；未发布版本 404 |

读取/渲染响应有意返回受权限保护的正文用于管理，但日志、Trace、错误、普通 Invocation Evidence 不包含这些正文。部署必须保护数据库和备份权限；dev_bypass 不是生产多租户认证。

## 模型调用（默认端口 8000）

`POST /v1/chat/completions` 的 `messages` 与 Gateway 扩展 `prompt` 必须二选一，不能同时提供，也不能显式为 null：

```json
{
  "model": "general",
  "prompt": {
    "asset_id": "替换为创建返回的 UUID",
    "version_id": "替换为已发布版本 UUID",
    "variables": {"name": "世界"}
  },
  "max_tokens": 2048
}
```

在鉴权、并发准入之后、指纹和持久化调用准入之前，从存储读取并渲染一次。该时间计入调用截止预算。渲染后继续执行当前配置的资源/安全/路由检查，消息总字节还受 max_request_bytes 限制。缺变量/非法模板/未发布/越权均在 Provider I/O 前失败。

所有 Attempt 接收相同的渲染消息，不再读取 Prompt 发布指针。Provider Adapter 不接收 Prompt DTO 或变量。响应 `gateway.prompt` 返回固定 asset_id/version_id；安全来源在同一准入事务写入 `invocation_prompt`，不存变量或正文。

Prompt 调用采用独立指纹 profile `gateway.request-fingerprint/prompt-text-v1` 与 `gateway.execution-fingerprint/prompt-text-v1`：包含固定版本来源及渲染内容的原有受密钥保护投影，未渲染选择禁止计算指纹。不同版本即使渲染相同也不同。原 messages 调用的 text-v1 指纹保持不变。

## 验证

`tests/test_prompts.py`、`test_prompt_management.py`、`test_prompt_invocation.py` 覆盖领域边界、事务回滚、并发发布、权限/租户隔离、HTTP 生命周期和指纹版本分离。`test_gateway_postgres.py` 通过管理 API 发布模板，再经模型 HTTP、真实 PostgreSQL 和 MockTransport Provider 验证渲染消息、版本证据与重启不重放；这不是付费 Provider 的实时证明。
