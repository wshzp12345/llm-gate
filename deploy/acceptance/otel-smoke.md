# OTLP 合成接收验收

从仓库根目录执行；需要 Docker 与已安装项目的虚拟环境。仅发送内存构造的未准入访问，不读凭证或数据库、不调用 Provider。独立端口 14318 必须空闲。

```powershell
docker run -d --name gateway-otlp-test-20260913 -p 127.0.0.1:14318:4318 --mount "type=bind,source=$((Get-Location).Path)/deploy/acceptance/otel-collector.yaml,target=/etc/otelcol/config.yaml,readonly" otel/opentelemetry-collector:0.160.0 --config=/etc/otelcol/config.yaml
.venv/Scripts/python.exe deploy/acceptance/otel_smoke.py
docker logs gateway-otlp-test-20260913
```

脚本必须退出 0，输出三个信号均接受。核对 Collector 解码输出中同一个 trace_id/request_id、父子 HTTP/authorization spans、关联日志及请求数为 1 的累计指标。HTTP 返回成功本身不证明存储可检索，因此必须同时检查接收输出。

此 debug Collector 仅用于合成数据，不能接收生产调用。核对容器镜像及只读挂载后，清理本次测试容器（也删除其临时日志）：

```powershell
docker inspect gateway-otlp-test-20260913 --format '{{.Config.Image}} {{json .Mounts}}'
docker rm -f gateway-otlp-test-20260913
```

2026-09-13 验证：Collector 0.160.0，镜像 digest `sha256:e495787f07dbe432ce763ebaf5bc3d113850e9eee2250ade7a3da6a882d0d69a`；接收 2 spans、1 log record、3 metrics/11 data points。Trace `09699128dd8747f599ae9875defa84ed`，request `224dae91-3b4b-4957-b5e0-5259c5e92438`。默认 Gateway、数据库及凭证未改动。
