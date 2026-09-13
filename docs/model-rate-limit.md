# 按逻辑模型的分钟请求限速

每个请求 `model` 对应一个 `model_aliases` 定义。其 `requests_per_minute` 是正整数，省略时为 **5**，只允许在模型定义中配置；模型调用请求、Provider Model Binding、路由策略和环境变量均不能覆盖它。此项为模型请求 RPM，不取代既有 Provider QPS、租户/实例 API 速率和并发保护。

```yaml
model_aliases:
  general:
    requests_per_minute: 5
    # candidates、routing_policy、safety_policy 等原必需字段保持不变
```

以上只是字段位置示意，不是完整可发布 Bundle。修改时通过管理 API 导出当前版本，修改目标模型的 `requests_per_minute`，再走 validate → 创建 Candidate → CAS publish。其他字段保持原样，不直接修改数据库，不自动发布模型配置。`0`、负数、浮点数、布尔值、字符串和 null 均拒绝；不提供 0 表示无限额度的隐式约定。

## 计数规则

- 以模型 Alias 的精确资源 ID 为键，同一模型跨调用方共享额度；不同 Alias 各自计数，即使指向同一个上游 Binding。用户新建另一个 Alias 是配置权限行为，不是请求参数可以绕过的入口。
- 单实例进程内滚动 **60 秒**窗口，使用单调时钟；第六次有效请求在最近 60 秒已有五次时返回 HTTP **429** / `error.code=rate_limited`，不排队。时间戳恰满 60 秒即过期，不以自然分钟清零。
- 同步和 SSE 共用额度。已授权、通过当前快照的请求预检查后、创建调用记录前消耗一次；重试、fallback、SSE 分片不再扣减。进入后即便模型失败、数据库准入失败或客户端取消，也不退还已消耗额度。被 RPM 拒绝的请求不消耗额度，不延长窗口。
- 请求格式无效、未授权、无有效配置、无效模型等在该检查前拒绝，不消耗模型额度。RPM 拒绝发生在调用身份创建和 Provider I/O 前，不返回 call_id、不产生 Token 费用；原外层 API 限速仍可能计入本次访问。
- SSE 被 RPM 拒绝时返回普通 JSON 429，不先提交 200 SSE 响应，也不返回 DONE。调用方必须先检查 HTTP 状态再解析流。
- 后续配置发布使用新额度，但不清空同一模型的最近请求计数；降低额度后等待旧请求过期，提高额度可立即使用新增余量。已开始的调用不被中途取消，删除/重建同名模型在窗口内也不重置计数。
- 此实现沿用单实例边界，进程重启重置计数，不宣称持久化或多副本全局限流。闲置计数随后续检查清理，外层实例 API gate 约束一分钟内计数规模。只读配置/健康接口不消耗模型额度。

## 兼容旧配置

已有 `gateway.config/v1` 不可变快照缺少此字段，运行时仍执行默认 5 RPM。规范化序列化省略值为 5 的字段，因此显式 5 与省略等价，旧快照摘要不变；其他额度进入快照及其摘要。导出时看不到该字段表示 5，不表示不限速。修改配置必须经过显式发布。

## 验证

无模型费用的自动测试：

```powershell
.venv\Scripts\python.exe -m pytest -q tests/test_model_rate.py
# 需 GATEWAY_TEST_DATABASE_URL 指向独立、可丢弃的 PostgreSQL 测试库
.venv\Scripts\python.exe -m pytest -q tests/test_model_rate_postgres.py
```

单元测试覆盖窗口精确边界、并发原子性、独立模型、变更额度不重置、过期回收、配置校验和旧摘要兼容。数据库/API 测试覆盖默认五次与自定义两次、同步/SSE 共用计数、429 无调用记录/上游请求、请求不能覆盖额度，以及七次逻辑请求含一次内部重试仍只消耗七个额度。

本机 Docker 的服务使用对应源码镜像后，按[启动说明](docker-quickstart.md#流式入口使用)发起请求。真实测试的成功请求会产生模型费用；已有窗口内请求也计入次数，不能要求任意六次手工请求一定前五次成功。不要为了触发 429 自动重试大量真实调用。

## 本轮验收记录（2026-09-14）

- 非 PostgreSQL 全集：1586 passed；PostgreSQL 全集：258 passed，共 1844 项。数据库回归使用独立 tmpfs 测试库，仅有 13 条既有 Uvicorn 弃用警告。
- 新增 RPM 数据库/API 测试单独运行通过，含同步/SSE 混合、独立模型、自定义额度、内部重试和限速前置拒绝。`compileall -q src` 通过。
- Docker 构建未完成：`pip install` 下载 `psycopg-binary` 时发生 `files.pythonhosted.org` 读取超时，构建退出 1。未执行服务替换，运行中的旧容器尚不包含本次 RPM 实现；不能把旧服务健康当作新限速已部署。网络恢复后可重新 `docker compose build gateway`，成功后再 `docker compose up -d gateway collector` 并核对 readiness。
- 本轮临时 PostgreSQL 容器和 pytest 目录已清理，tmpfs 合成测试数据不可恢复但可重新生成。默认业务数据库未修改；核对时保留 9 条调用。无真实 Provider 调用。

## 本轮验收记录（2026-09-14）

- 非 PostgreSQL 全集：1586 passed；PostgreSQL 全集：258 passed，共 1844 项。数据库回归使用独立 tmpfs 测试库，仅有 13 条既有 Uvicorn 弃用警告。
- 新增 RPM 数据库/API 测试单独运行通过，含同步/SSE 混合、独立模型、自定义额度、内部重试和限速前置拒绝。`compileall -q src` 通过。
- 新能力的 Docker 镜像构建/部署尚在核对，不用历史 Docker 成功记录代替本次部署证明；无真实 Provider 调用。
