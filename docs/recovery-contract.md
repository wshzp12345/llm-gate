# 同步文本恢复与双供应商契约

逻辑 Alias 的候选列表由已发布配置锁定。上层请求不指定重试时的模型；Gateway 在相同权限、安全策略、Prompt、生成参数和截止预算内进行候选选择。供应商 endpoint、API 根路径、凭证、upstream_model 和兼容协议字段的解析留在 Adapter/边界层。

## 判断边界

| 故障 | 同候选重试 | 换候选 | 说明 |
| --- | --- | --- | --- |
| 可重试的 429/部分 5xx/连接建立失败 | 预算内允许 | 合规候选可用时允许 | 遵守 Retry-After 上限、退避、取消和单一截止时间 |
| 凭证不可用、供应商协议错误 | 不允许 | 仅其他仍合规候选 | 不能用同一已失效凭证继续；独立供应商应配置独立 Secret Reference |
| invalid_request | 不允许 | 不允许 | 原请求本身无效 |
| uncertain（例如发送后读取超时） | 不允许 | 不允许 | 防止不确定执行被自动重放 |
| Provider refusal | 不允许 | 不允许 | 拒绝是终态结果，不用 fallback 绕过 |

配置的最大总 Attempt 为 3、每候选为 2（可配更低）；这些是现有恢复策略约束，不是用户授权的账户调用次数额度。
启动器使用整数毫秒的 full-jitter 随机值。候选尚未获得执行资格不能消耗 Attempt；被跳过的候选需要保留原因证据。
当前同步 full-service 路径拒绝启用 reduced/cache，不能静默降级。SSE 已接通，并另有真实 TCP 与 Docker 恢复/故障验收；输出提交后禁止重试或切换模型，失败流不发送成功 DONE。具体见[流式契约](streaming-contract.md)，不能把同步的恢复成功当作流式边界证明。

## 能力相符不等于回答质量等价

候选按请求所需 streaming/tool/structured capability、输出上限、enabled/weight/service-level、出站安全、凭证、健康、容量等规则筛选。
测试中输出上限不足的候选未执行。Alias 中声明 full 表示部署者接受该模型满足该服务契约，不表示 Gateway 已用评测证明两个模型的知识、推理质量或安全表现相同。
不同模型需要质量等价保证时，应先在外部评测再纳入同一 Alias；此基础范围没有自动 Prompt Evals。两种兼容协议供应商已验证，不声称已实现所有专有供应商协议。

## 可观测响应

正常返回后，统一 Gateway 从已提交的 Attempt 记录读取恢复摘要；不会在持久化前凭内存计数宣称成功：

```json
{"gateway":{"requested_model":"general","resolved_model":"actual-secondary",
            "recovery":{"attempts":3,"retries":1,"fallback_used":true}}}
```

`attempts` 为实际启动并记录终态的次数，`retries` 为同一 Binding 的第二次 Attempt 数，`fallback_used` 表示至少两个不同 Binding 真正执行过。
首选候选在执行前被跳过、只执行一个后备候选时 attempts=1、fallback_used=false；这不是“重试”，跳过原因仍在路由证据中。
成功与已结算失败响应均携带 `X-Gateway-Attempts`、`X-Gateway-Retries`、`X-Gateway-Fallback-Used`；未准入请求不伪造次数。
此摘要不是完整 Trace。request/call/trace 的关联、spans 和阶段时延仍是独立待验收项目。

统一启动器从 Bootstrap `provider_secret_files` 的键生成已知 Secret Reference 清单。配置只能引用已经声明的材料来源；配置发布不会读取凭证值或自动注册任意路径。

## 可复现证据

- `tests/test_gateway_recovery.py`：13 种完整模型 API + Prompt 管理 API + PostgreSQL 场景；在首个 Provider 请求期间发布新模板，所有后续 Attempt 仍使用原模板/参数；校验恢复响应、实际模型、凭证失效不可绕过、不合规后备候选不能执行、refusal 不切换及持久化版本来源。
- `tests/test_recovery.py`、`test_attempt_execution.py`、`test_routing_eligibility.py`：预算、截止时间、取消、已提交保护及能力筛选的底层策略边界。
- [隔离 Docker 验收](../deploy/acceptance/README.md)：两台合成 HTTP Provider、真实出站传输和 PostgreSQL，通过正常 Gateway 启动链路验证重试后 fallback，不消费真实 Provider 配额。
