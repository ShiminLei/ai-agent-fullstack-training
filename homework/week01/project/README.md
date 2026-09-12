# 多协议 LLM 统一调用服务

这是第一周作业的分步骤教学实现。服务对客户端提供统一的 OpenAI Chat Completions 风格接口，在内部通过 Adapter 模式调用两种不同协议：

- OpenAI Responses API：`POST /v1/responses`
- Anthropic Messages API：`POST /v1/messages`

## 作业要求

本项目按照课程第一周作业“LLM 统一模型调用服务”实现，要求如下：

1. 设计统一调用接口，通过 Adapter 模式封装两种不同 API 协议，并根据请求中的 `model` 动态路由。
2. 支持三项模型调用能力：
   - `stream=true` 时通过 SSE 流式返回；
   - 使用 `response_format` 约束合法 JSON；
   - 支持 Prompt 模板存储、变量替换和版本引用。
3. 记录每次调用的 Token 分类统计、总延迟和首 Token 延迟。
4. 提供统一错误、最多三次指数退避重试，以及按模型独立限流；超限返回 HTTP 429。
5. 交付完整源码、启动指南、curl 示例和覆盖全部功能的验证脚本。

验收时，两个模型都应能正常调用，并能够分别验证流式输出、结构化输出、模板引用、可观测数据、重试和限流。

## 实现进度

- [x] 统一领域模型和 Adapter 接口
- [x] Responses API Adapter
- [x] Anthropic Messages API Adapter
- [x] 模型路由和 Gateway
- [x] FastAPI 普通与流式接口
- [x] 结构化输出和 Prompt 版本
- [x] Token/延迟观测、重试和模型限流
- [x] 配置、Docker、验证脚本和使用文档

## 架构

```text
Client
  │ POST /v1/chat/completions
  ▼
FastAPI ── 鉴权 ── 按模型限流
  ▼
Gateway ── Prompt ── 重试 ── 观测记录
  ▼
ModelRouter
  ├── ResponsesAdapter ─────► /v1/responses
  └── AnthropicAdapter ─────► /v1/messages
```

Gateway 负责完整调用过程；Adapter 只负责协议翻译。多个使用相同协议的模型可以共享同一种 Adapter。

## 功能

- 根据公开 `model` 动态选择供应商、真实模型和 Adapter
- 普通调用和 SSE 流式调用
- `response_format.json_schema` 约束及非流式结果本地二次校验
- Jinja2 Sandbox Prompt 模板、变量替换、自动版本号和版本引用
- 输入、输出、缓存读取、缓存创建 Token 分类统计
- 总延迟和首 Token 延迟（TTFT）
- 网络错误及指定 HTTP 状态码指数退避重试，最多三次
- 每个公开模型独立的令牌桶限流，超限返回 HTTP 429
- 统一错误结构和 Gateway API Key 鉴权
- SQLite 保存 Prompt 版本和调用记录

## 目录

```text
app/
├── adapters/
│   ├── base.py
│   ├── responses.py
│   └── anthropic.py
├── config.py
├── errors.py
├── gateway.py
├── main.py
├── observability.py
├── prompts.py
├── rate_limit.py
├── router.py
├── schemas.py
└── structured.py
```

## 本地启动

要求 Python 3.10+，推荐 Python 3.12 和 `uv`。

```bash
cd homework/week01/project2
cp .env.example .env
```

在 `.env` 中填写课程提供的两个模型服务地址和密钥：

```dotenv
GATEWAY_API_KEY=replace-with-a-long-random-secret
RESPONSES_BASE_URL=https://your-responses-compatible-provider.example
RESPONSES_API_KEY=replace-me
ANTHROPIC_BASE_URL=https://your-anthropic-compatible-provider.example
ANTHROPIC_API_KEY=replace-me
```

安装并启动：

```bash
uv sync --extra dev
set -a; source .env; set +a
uv run uvicorn app.main:app --reload --port 8000
```

打开：

- OpenAPI 文档：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/healthz>

`base_url` 不包含 `/v1/responses` 或 `/v1/messages`，对应 Adapter 会自动追加协议路径。如果课程提供的模型 ID 不同，请修改 `gateway.yaml`。

## Docker 启动

```bash
docker compose up --build
```

## 普通调用

调用 Responses API 模型：

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer replace-with-a-long-random-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-pro",
    "messages":[{"role":"user","content":"解释 Adapter 模式"}]
  }'
```

调用 Anthropic Messages API 模型时，只需要更换公开模型名：

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer replace-with-a-long-random-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-flash",
    "messages":[{"role":"user","content":"解释 Gateway 的职责"}]
  }'
```

## 流式调用

```bash
curl -N http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer replace-with-a-long-random-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-flash",
    "stream":true,
    "messages":[{"role":"user","content":"逐步解释模型路由"}]
  }'
```

响应被统一为 `chat.completion.chunk` SSE，并以 `data: [DONE]` 结束。首个文本块到达时记录 TTFT。流式输出开始前允许重试；一旦已有内容发送便不再重试，避免重复文本。

## 结构化输出

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer replace-with-a-long-random-secret' \
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

Responses Adapter 将约束转换为 `text.format`；Anthropic Adapter 将约束写入系统指令。非流式结果会使用 `jsonschema` 再次校验，不合规时返回 HTTP 422。

## Prompt 版本

创建第一版：

```bash
curl http://localhost:8000/v1/prompts \
  -H 'Authorization: Bearer replace-with-a-long-random-secret' \
  -H 'Content-Type: application/json' \
  -d '{"id":"teacher","content":"你是 {{ topic }} 老师。","activate":true}'
```

再次使用相同 `id` 创建会自动得到下一个版本号。调用时可以使用激活版本，也可以明确指定旧版本：

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Authorization: Bearer replace-with-a-long-random-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "model":"deepseek-v4-flash",
    "messages":[{"role":"user","content":"从基础开始讲"}],
    "prompt":{"id":"teacher","version":1,"variables":{"topic":"Python"}}
  }'
```

## 可观测数据

```bash
curl 'http://localhost:8000/admin/usage?limit=20' \
  -H 'Authorization: Bearer replace-with-a-long-random-secret'
```

每条记录包括请求 ID、调用方密钥指纹、公开模型、供应商、真实模型、状态、Token 分类、总延迟、TTFT、尝试次数和使用的 Prompt 版本。不会保存对话正文和 API Key 原文。

## 统一错误

```json
{
  "error": {
    "message": "Unknown model: missing",
    "type": "invalid_request_error",
    "code": "model_not_found"
  }
}
```

主要状态码：

- `401`：Gateway API Key 无效
- `404`：模型或 Prompt 不存在
- `422`：请求、Prompt 渲染或结构化结果错误
- `429`：模型限流
- `502`：上游服务失败

## 一键验证

测试使用 Mock Transport 模拟两种上游协议，不需要真实密钥，也不消耗模型额度：

```bash
uv run --extra dev ./verify.sh
```

测试覆盖统一模型、两种 Adapter、普通调用、SSE、路由、Prompt 版本、结构化输出、Token 分类、总延迟、TTFT、三次重试、模型独立限流、鉴权和统一错误。

## 生产化说明

当前限流器是单进程内存实现，适合课程作业。多副本部署应使用 Redis 共享限流状态；高吞吐场景可将 SQLite 替换为 PostgreSQL 或 ClickHouse。真实密钥应放入 Secrets Manager/KMS，管理接口应部署在内网或增加管理员权限。
