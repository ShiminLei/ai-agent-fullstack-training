# Streaming Agent Gateway

这是 Week 01「Streaming 流式输出」作业。它把一次模型调用包装成独立的 Run，并通过统一事件协议与 SSE 向 Web/CLI 客户端持续发送结果。

## 课件中的作业要求

这次作业的总体目标，是把普通的同步模型调用升级成一个**可观察、可取消、可恢复、可审计**的 Streaming Agent Gateway。

### 四项能力升级

| 能力 | 课件要求 | 本项目中的实现 |
|---|---|---|
| 可观察 | 运行过程中的事件可以被追踪 | 每个 Run 保存连续的 `RunEvent`，并记录 Trace、TTFT、总耗时、Token 用量等信息 |
| 可取消 | 用户可以显式终止正在运行的任务 | Cancel API 将状态切换为 `cancelling`，向后台 Task 传播取消，真正停止后写入 `run.cancelled` |
| 可恢复 | 支持 Replay 和 Checkpoint | 客户端通过最后一个 `seq` 重放遗漏事件；服务端持久化事件和 Checkpoint |
| 可审计 | 具备 Trace 和统一失败协议 | 每个 Run 都有 `trace_id`；失败统一转换为稳定错误码、失败阶段、可重试性和恢复建议 |

### 应实现的八项系统能力

- [x] 创建独立 Run；
- [x] 流式调用模型；
- [x] 将供应商输出转换成统一 Harness Event，本项目中命名为 `RunEvent`；
- [x] 通过 SSE 推送事件；
- [x] 支持用户取消；
- [x] 支持断线恢复；
- [x] 保存 Checkpoint；
- [x] 统计 TTFT（Time To First Token，首个有效文本增量延迟）。

### 课件建议的实现顺序

1. 定义 `RunEvent`；
2. 实现 SSE 编解码；
3. 抽象 `StreamingModel`；
4. 接入模型 Adapter；
5. 实现 Run Store；
6. 实现 Agent Loop；
7. 创建订阅接口和取消接口；
8. 开发 Web/CLI 客户端；
9. 增加异常与边界测试；
10. 接入数据库并保存 Checkpoint。

### 验收底线

- **首个 delta 快速展示**：客户端收到第一个有效 `text.delta` 后立即展示，不等待完整回答；
- **不依赖模型 SDK**：Web/CLI 只识别统一 `RunEvent`，不接触 DeepSeek/OpenAI 的 SDK chunk；
- **Run 只有一个终态**：`run.completed`、`run.failed`、`run.cancelled` 三者只能出现一个；
- **取消真的停止**：关闭 SSE 连接不等于取消；必须调用 Cancel API，并终止后台模型消费；
- **断线不重跑**：重连时继续订阅原来的 `run_id`，从最后一个 `seq` 之后重放，不能重新创建 Run。

本项目还通过测试检查 SSE 拆包/粘包、UTF-8 分段、连续序号、重复事件、完成与取消竞争、模型中途失败、受限重试以及 Checkpoint 重载等边界情况。

## 已实现能力

- 独立 Run：创建接口立即返回 `202 Accepted`，模型在后台运行；
- 流式模型 Adapter：把 DeepSeek/OpenAI SDK chunk 转成内部 `ModelStreamEvent`；
- 统一事件：Store 集中分配连续 `seq`，并保证每个 Run 只有一个终态；
- SSE：支持 UTF-8、拆包、粘包、多行 data、心跳与事件重放；
- 真取消：`created/running -> cancelling -> cancelled`，取消会传播到后台 Task；
- 断线恢复：客户端携带最后一个 `seq`，只补收遗漏事件，不重新运行模型；
- 持久化与 Checkpoint：Run、事件、Trace 和 Checkpoint 保存到 SQLite，重启后可继续查询及重放；
- 可观测性：记录 `trace_id`、TTFT、总耗时、Token 用量、重试/重连次数、取消延迟、Checkpoint 版本和终态；
- 失败协议：对外返回稳定错误码、失败阶段、可重试性和恢复建议，不泄露原始异常；
- Web 与 CLI：两端都按 `seq` 去重，断线时恢复原 Run。

## 数据流

```text
DeepSeek API
    │ SDK ChatCompletionChunk
    ▼
DeepSeekChatAdapter
    │ ModelStreamEvent（内部模型事件）
    ▼
Agent Loop
    │ RunEvent（统一业务事件）
    ▼
SQLiteRunStore
    │ SSE bytes
    ▼
Web / CLI
```

这里有两种不同层级的事件：

- `ModelStreamEvent` 只存在于 Adapter 与 Agent Loop 之间，用来屏蔽不同模型供应商的格式；
- `RunEvent` 是系统正式保存、审计、重放并发送给客户端的事件。

## 目录结构

```text
1-2/
├── app/
│   ├── events.py       # RunEvent 协议和终态判断
│   ├── model.py        # StreamingModel、内部事件和 DeepSeek Adapter
│   ├── runner.py       # Agent Loop、重试、取消、失败协议、Trace、Checkpoint
│   ├── sse.py          # SSE 编解码、心跳、历史重放和实时订阅
│   ├── store.py        # 内存状态机、序号、取消信号、Trace、Checkpoint
│   ├── persistence.py  # SQLite 持久化 Store
│   └── main.py         # FastAPI 应用和 HTTP 接口
├── static/index.html   # 浏览器客户端
├── tests/              # 单元及 API 测试
├── cli.py              # 命令行客户端
├── requirements.txt
└── README.md
```

## Run 生命周期与事件

```text
created -> running -> completed
                   -> failed
created/running -> cancelling -> cancelled
```

合法的 `RunEvent.type`：

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

当前模型循环只执行文本生成；`tool.started/tool.completed` 已在统一协议中预留，接入 Tool Runtime 后使用。Store 一旦写入 `completed/failed/cancelled`，就拒绝任何后续事件。

## 安装与启动

以下命令都可以直接在仓库根目录执行，无需 `cd`：

```bash
.venv/bin/python -m pip install -r homework/week01/1-2/requirements.txt
export DEEPSEEK_API_KEY="你的 API Key"
.venv/bin/python -m uvicorn app.main:app \
  --app-dir homework/week01/1-2 \
  --reload
```

可选环境变量：

```bash
export DEEPSEEK_BASE_URL="https://api.deepseek.com"
export DEEPSEEK_MODEL="deepseek-v4-flash"
export RUN_DATABASE_PATH="homework/week01/1-2/data/runs.db"
```

API Key 只从环境变量读取，不要写入代码或提交到 Git。

启动后可访问：

- Web 客户端：`http://127.0.0.1:8000/`
- FastAPI 文档：`http://127.0.0.1:8000/docs`

## HTTP 接口

创建 Run：

```http
POST /v1/runs
Content-Type: application/json

{"prompt":"请介绍 Python 异步编程"}
```

创建成功返回 `202` 和新的 `run_id`。订阅事件：

```bash
curl -N http://127.0.0.1:8000/v1/runs/{run_id}/events
```

浏览器刷新时不能主动设置 `Last-Event-ID` 请求头，所以接口同时支持查询参数：

```text
GET /v1/runs/{run_id}/events?after_seq=5
```

服务端只发送 `seq > 5` 的事件；这是恢复传输，不会重新调用模型。

查询、取消与审计接口：

```text
GET  /v1/runs/{run_id}
POST /v1/runs/{run_id}/cancel
GET  /v1/runs/{run_id}/trace
GET  /v1/runs/{run_id}/checkpoint
```

`run.failed` 只暴露稳定字段：`code`、`stage`、`retryable`、`hint` 和 `partial_text`。原始异常消息不会发送给客户端。

## CLI 客户端

服务运行后，在仓库根目录执行：

```bash
.venv/bin/python homework/week01/1-2/cli.py "打个招呼"
```

CLI 遇到传输断开会使用最后一个 `seq` 重订阅同一 Run；不会重新创建任务。

## 测试

```bash
.venv/bin/python -m pytest \
  --rootdir=homework/week01/1-2 \
  homework/week01/1-2/tests \
  -q
```

测试不访问真实 DeepSeek，也不消耗 API 额度。覆盖事件校验、Adapter 转换、SSE 拆包/粘包、连续序号、去重重放、心跳、取消传播、完成/取消竞争、失败恢复、受限重试、TTFT、Trace、SQLite 和 Checkpoint 恢复。

## 边界说明

- 当前作业只有一次文本模型步骤，没有真正的 Tool Runtime；遇到 `finish_reason=tool_calls` 会明确失败，不会错误地标记完成。
- 已有文本增量后发生模型断线时禁止自动重跑，以避免重复或拼接出错误答案；客户端仍可重放已经落库的事件。
- Checkpoint 支持重启后恢复已保存事实、查询和 SSE 重放。由于供应商流本身没有可移植的续传游标，本实现不会假装从模型响应的任意字节处继续生成。
