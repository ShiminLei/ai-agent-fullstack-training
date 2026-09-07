# Streaming Agent Gateway

这是 Week 01「Streaming 流式输出」作业的实现。项目把一次模型调用包装成独立的 Run，并通过统一事件协议和 SSE 向客户端持续推送结果。

当前实现的核心能力：

- 创建独立 Run；
- 使用 DeepSeek 的 OpenAI 兼容流式接口；
- 将供应商 chunk 转换为统一的 `ModelStreamEvent`；
- 将模型事件转换并保存为带 `run_id`、`seq` 的 `RunEvent`；
- 通过 SSE 推送和重放事件；
- 使用 `Last-Event-ID` 从断点继续接收；
- 用户取消后停止后台任务并写入 `run.cancelled`；
- 异常统一转换成 `run.failed`；
- 保证一个 Run 只有一个终态。

## 架构

```text
DeepSeek API
    │ ChatCompletionChunk
    ▼
DeepSeekChatAdapter
    │ ModelStreamEvent
    ▼
Agent Loop
    │ RunEvent
    ▼
InMemoryRunStore
    │ SSE bytes
    ▼
HTTP Client / Browser
```

这里有两层不同的事件：

- `ModelStreamEvent`：Adapter 与 Agent Loop 之间的内部模型事件；
- `RunEvent`：系统正式保存、审计、重放并发送给客户端的事件。

## 目录结构

```text
1-2/
├── app/
│   ├── events.py       # RunEvent 协议与终态定义
│   ├── model.py        # StreamingModel、内部事件和 DeepSeek Adapter
│   ├── runner.py       # Agent Loop：消费模型流并推进 Run
│   ├── sse.py          # SSE 编码、历史重放和实时订阅
│   ├── store.py        # 内存 Run Store、序号、状态和取消信号
│   └── main.py         # FastAPI 应用与 HTTP 接口
├── static/
│   └── index.html      # 浏览器客户端（待实现）
├── tests/
│   ├── fakes.py        # 测试使用的假模型
│   ├── test_adapter.py
│   ├── test_events.py
│   ├── test_main.py
│   ├── test_model.py
│   ├── test_runner.py
│   ├── test_sse.py
│   └── test_store.py
├── requirements.txt
└── README.md
```

## Run 事件

每个 `RunEvent` 都包含：

```json
{
  "schema_version": "1",
  "run_id": "run_...",
  "seq": 0,
  "type": "run.started",
  "created_at": "2026-09-08T00:00:00Z",
  "data": {}
}
```

当前协议允许以下事件类型：

```text
run.started
text.delta
tool.started
tool.completed
run.retrying
run.completed
run.failed
run.cancelled
```

终态事件为：

```text
run.completed
run.failed
run.cancelled
```

一个 Run 写入终态后，Store 不允许再追加任何事件。

## 环境准备

以下命令均在仓库根目录执行。

安装依赖：

```bash
.venv/bin/python -m pip install -r homework/week01/1-2/requirements.txt
```

设置 DeepSeek API Key：

```bash
export DEEPSEEK_API_KEY="你的 API Key"
```

可选配置：

```bash
export DEEPSEEK_BASE_URL="https://api.deepseek.com"
export DEEPSEEK_MODEL="deepseek-v4-flash"
```

API Key 只从环境变量读取，不应写入代码或提交到 Git。

## 启动服务

从仓库根目录启动：

```bash
.venv/bin/python -m uvicorn app.main:app \
  --app-dir homework/week01/1-2 \
  --reload
```

默认地址：

```text
http://127.0.0.1:8000
```

FastAPI 自动接口文档：

```text
http://127.0.0.1:8000/docs
```

## HTTP 接口

### 创建 Run

```http
POST /v1/runs
Content-Type: application/json
```

请求：

```json
{
  "prompt": "请介绍一下 Python 异步编程"
}
```

命令行示例：

```bash
curl -X POST http://127.0.0.1:8000/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"打个招呼"}'
```

响应示例：

```json
{
  "run_id": "run_1234...",
  "status": "created"
}
```

创建接口只负责启动后台任务，不等待模型生成完整答案。

### 订阅事件

```http
GET /v1/runs/{run_id}/events
```

使用 `curl -N` 可以关闭客户端输出缓冲，直接观察流式事件：

```bash
curl -N http://127.0.0.1:8000/v1/runs/run_1234/events
```

SSE 帧示例：

```text
id: 1
event: text.delta
data: {"schema_version":"1","run_id":"run_1234","seq":1,...}

```

### 断线恢复

如果客户端已经收到 `seq=5`，重新连接时传入：

```bash
curl -N http://127.0.0.1:8000/v1/runs/run_1234/events \
  -H 'Last-Event-ID: 5'
```

服务端只返回 `seq > 5` 的事件，不会重复发送前面的事件。

### 查询 Run

```http
GET /v1/runs/{run_id}
```

响应示例：

```json
{
  "run_id": "run_1234",
  "status": "running",
  "next_seq": 3
}
```

### 取消 Run

```http
POST /v1/runs/{run_id}/cancel
```

```bash
curl -X POST http://127.0.0.1:8000/v1/runs/run_1234/cancel
```

取消接口会：

1. 设置 Run 的取消信号；
2. 写入唯一的 `run.cancelled` 终态事件；
3. 取消正在运行的后台异步任务；
4. 保存取消前已经生成的 `partial_text`。

## 运行测试

从仓库根目录执行：

```bash
.venv/bin/python -m pytest \
  --rootdir=homework/week01/1-2 \
  homework/week01/1-2/tests \
  -q
```

测试不访问真实 DeepSeek，也不消耗 API 额度。测试中的 `FakeStreamingModel` 和 `FakeOpenAIClient` 会模拟模型事件和 SDK chunk。

当前测试覆盖：

- RunEvent 校验和终态判断；
- SSE 编码、中文、换行和大数据；
- 假模型事件顺序；
- DeepSeek Adapter 的请求参数和 chunk 转换；
- Store 创建、查询、序号、终态和断线重放；
- Agent Loop 的完成、失败和取消路径；
- FastAPI 的创建、查询、订阅、恢复、取消和错误响应。

## 当前限制和后续工作

当前 Run Store 位于进程内存中，服务重启后数据会消失，也不支持多进程共享。

后续还需要完成：

- 浏览器页面 `static/index.html`；
- 将静态页面接入 FastAPI；
- SSE 空闲心跳；
- TTFT（首个文本增量延迟）统计；
- 数据库持久化；
- checkpoint 保存与恢复；
- 更完整的错误码、重试策略和 Trace 信息。
