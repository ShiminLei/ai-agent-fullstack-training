# 多协议 LLM 统一调用服务

本项目提供一个 OpenAI Chat Completions 风格的统一入口，并通过 Adapter 模式调用两种不同的上游协议：

- OpenAI Responses API：`POST /v1/responses`
- Anthropic Messages API：`POST /v1/messages`

客户端只需要使用统一的 `/v1/chat/completions`。Gateway 根据公开的 `model` 字段选择 Adapter，由 Adapter 处理鉴权、请求转换、响应解析和 SSE 事件转换。

## 架构

```text
Client
  │ POST /v1/chat/completions
  ▼
FastAPI ── Authentication ── Model Rate Limiter
  ▼
Gateway ── Prompt ── Retry ── Observability
  ▼
ModelRouter
  ├── ResponsesAdapter ──► /v1/responses
  └── AnthropicMessagesAdapter ──► /v1/messages
```

## 已实现功能

- 统一请求和统一响应
- 根据模型别名动态路由两种协议 Adapter
- 普通响应和 SSE 流式响应
- `response_format.json_schema` 结构化输出及本地二次校验
- Jinja2 Sandbox Prompt 模板、变量渲染和版本引用
- Token 分类统计：输入、输出、缓存读取和缓存创建
- 总延迟与首 Token 延迟（TTFT）
- 网络错误及指定 HTTP 状态码指数退避重试，最多三次
- 按公开模型别名隔离的令牌桶限流，超限返回 HTTP 429
- OpenAI 风格统一错误对象与 Gateway API Key 鉴权
- SQLite 持久化 Prompt 和调用记录

## 项目结构

```text
app/
├── adapters/
│   ├── base.py          # Adapter 统一契约和 SSE 解析
│   ├── responses.py     # Responses API 协议转换
│   └── anthropic.py     # Anthropic Messages API 协议转换
├── config.py            # YAML 配置与环境变量展开
├── errors.py            # 统一错误语义
├── gateway.py           # 调用编排、重试、流式和观测
├── main.py              # FastAPI 接口与组件组装
├── observability.py     # SQLite 调用记录
├── prompts.py           # Prompt 版本与模板渲染
├── rate_limit.py        # 按模型限流
├── router.py            # model 到 Adapter 的路由
├── schemas.py           # 统一领域模型
└── structured.py        # JSON Schema 校验
```

## 启动

需要 Python 3.10+，推荐 Python 3.12 和 `uv`。

```bash
cd homework/week01/project
cp .env.example .env
# 在 .env 中填写两个上游的地址和密钥
set -a; source .env; set +a
uv sync --extra dev
uv run uvicorn app.main:app --reload --port 8000
```

配置中的 `base_url` 不包含协议路径，Adapter 会分别追加 `/v1/responses` 和 `/v1/messages`。如果课程提供的 DeepSeek 服务地址或模型 ID 不同，请修改 `.env` 和 `gateway.yaml`。

也可以使用 Docker：

```bash
docker compose up --build
```

健康检查：

```bash
curl http://localhost:8000/healthz
```

## 普通调用

Responses API 模型：

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer dev-gateway-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-pro",
    "messages":[{"role":"user","content":"解释 Adapter 模式"}]
  }'
```

Anthropic Messages API 模型只需替换公开模型名：

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer dev-gateway-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-flash",
    "messages":[{"role":"user","content":"解释 Gateway 的职责"}]
  }'
```

## 流式输出

```bash
curl -N http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer dev-gateway-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-flash",
    "stream":true,
    "messages":[{"role":"user","content":"逐步解释模型路由"}]
  }'
```

两个 Adapter 会把各自不同的上游事件统一成 `chat.completion.chunk` SSE，最后发送 `data: [DONE]`。首个内容块产生时记录 TTFT。流式开始前可以重试；内容一旦发给客户端便不会重试，避免重复文本。

## 结构化输出

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer dev-gateway-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-pro",
    "messages":[{"role":"user","content":"提取：Ada，36岁"}],
    "response_format":{
      "type":"json_schema",
      "json_schema":{
        "name":"person",
        "strict":true,
        "schema":{
          "type":"object",
          "properties":{"name":{"type":"string"},"age":{"type":"integer"}},
          "required":["name","age"],
          "additionalProperties":false
        }
      }
    }
  }'
```

Responses Adapter 将约束转换为 `text.format`；Anthropic Adapter 将 Schema 写入系统指令。非流式结果还会在网关内使用 `jsonschema` 二次校验，不合规时返回 HTTP 422。

## Prompt 版本管理

创建第一版：

```bash
curl http://localhost:8000/v1/prompts \
  -H 'Authorization: Bearer dev-gateway-key' \
  -H 'Content-Type: application/json' \
  -d '{"id":"teacher","content":"你是 {{ topic }} 老师。","activate":true}'
```

使用模板及指定版本：

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer dev-gateway-key' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-flash",
    "messages":[{"role":"user","content":"从基础开始讲"}],
    "prompt":{"id":"teacher","version":1,"variables":{"topic":"Python"}}
  }'
```

对同一个 `id` 再次调用创建接口会生成下一版本。`activate=true` 会把新版本设为默认；请求省略 `version` 时读取激活版本。

## 可观测数据

```bash
curl 'http://localhost:8000/admin/usage?limit=20' \
  -H 'Authorization: Bearer dev-gateway-key'
```

记录包括请求 ID、公开模型、供应商、真实模型、状态、Token 分类、总延迟、TTFT、尝试次数、错误类型和 Prompt 版本。调用正文和 API Key 原文不会进入调用记录。

## 错误格式

```json
{
  "error": {
    "message": "Unknown model: missing",
    "type": "invalid_request_error",
    "code": "model_not_found"
  }
}
```

主要状态码：`401` 鉴权失败、`404` 模型或 Prompt 不存在、`422` 参数或结构化结果错误、`429` 模型限流、`502` 上游错误。

## 验证

测试使用 Mock Transport 模拟两个上游，不消耗真实 Token：

```bash
uv run --extra dev ./verify.sh
```

测试覆盖：双协议动态路由、SSE、结构化输出、Prompt 版本、Token 分类、总延迟、TTFT、三次重试、模型独立限流和鉴权。

## 生产化说明

当前限流器是单进程内存实现，适合课程作业和单副本部署。多副本应改为 Redis；高并发观测数据应迁移到 PostgreSQL 或 ClickHouse。真实密钥应由 Secrets Manager/KMS 管理，管理接口应限制在内网或增加管理员权限。
