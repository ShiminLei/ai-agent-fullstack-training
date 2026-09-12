# cancel_order 作业：一步一步理解

这个作业实现一条完整但最小的安全工具调用链：

```text
模型提出 Tool Call
  -> Runtime 校验参数
  -> Runtime 检查可信身份和权限
  -> Runtime 检查高风险操作是否已确认
  -> Handler 执行一次取消操作
  -> Runtime 校验输出并生成 Tool Result
  -> 写入带 trace_id 与 tool_call_id 的审计记录
```

## 第 1 步：定义三份 Schema

打开 `schemas.py`。

- `CancelOrderInput` 只有 `order_id` 和 `reason`，并用 `extra="forbid"` 禁止额外字段。
- `CancelOrderOutput` 固定返回 `order_id`、`status="cancelled"` 和 `operation_id`。
- `ToolError` 让各种失败拥有统一、可判断的结构。

Input Schema 保护“模型到 Runtime”的边界；Output Schema 保护“业务系统到 Agent”的边界；Error Schema 保护“失败到恢复逻辑”的边界。

## 第 2 步：划分可信与不可信数据

打开 `contracts.py`。

- `ToolCall.arguments_json` 是模型生成的，不可信。
- `ExecutionContext` 中的用户、租户、权限、审批和 `trace_id` 由 Harness 注入，可信。
- `ToolDefinition.to_model_tool()` 只向模型公开名称、描述和 Input Schema。

因此模型不能声称“我是管理员”或“我已经审批”。即使它把这些内容塞进参数，也会被 Input Schema 拒绝。

## 第 3 步：装配 cancel_order

打开 `cancel_order_tool.py`。

工具策略是：

- 权限：`order:write`
- 风险：`high`
- 幂等性：`False`
- 自动重试次数：`0`

Handler 使用 `ctx.user_id`，而不是模型参数中的用户身份。这样模型不能取消其他用户的订单。

## 第 4 步：按固定顺序治理

打开 `runtime.py`，阅读 `ToolRuntime.execute()`：

1. 查找工具；
2. 校验 Input Schema；
3. 检查权限；
4. 检查审批；
5. 执行 Handler；
6. 校验 Output Schema；
7. 生成保留原始 `tool_call_id` 的 Tool Result；
8. 写审计记录。

前三类检查失败时，`handler_invoked=False`，业务函数不会被调用。

## 第 5 步：理解为什么超时不能重试

Runtime 的超时只说明“调用方没有按时收到结果”，不代表取消动作一定没发生。如果自动调用第二次，可能造成重复写入或状态冲突。

因此非幂等工具会被 Runtime 强制限制为一次尝试。超时结果是 `TIMEOUT`、`retryable=false`，交给上层查询最终状态或请用户处理。

## 第 6 步：用测试证明，而不只靠解释

在本目录运行：

```bash
python test_cancel_order.py
```

重点阅读 `test_cancel_order.py` 中三类“调用次数必须为零”的测试：非法额外参数、没有权限、没有审批。

再运行演示：

```bash
python demo.py
```

演示依次展示公开 Schema、未确认拒绝、成功取消、超时不重试和审计记录。

## 作业问题回答

### Q1：Function Calling 为什么是候选动作，而不是函数执行？

模型只能生成工具名称和参数。参数可能错误、越权或缺少审批，所以它表达的是“建议做什么”；只有 Runtime 校验通过后，Handler 才能产生真实副作用。

### Q2：工具描述、Input Schema 和权限检查分别解决什么问题？

- 描述帮助模型选择正确工具；
- Input Schema 限制模型能提交哪些业务参数及其格式；
- 权限检查根据可信身份判断这次调用是否允许执行。

三者分别处理“选什么”“参数是否合法”“有没有权做”，不能互相替代。

### Q3：为什么 Tool Result 必须保留 tool_call_id？

同一轮可能有多个同名调用，而且结果可能乱序返回。`tool_call_id` 把结果精确关联到原始调用，也便于错误恢复和审计。

### Q4：为什么权限信息不能从模型参数中读取？

模型输出属于不可信输入。如果允许模型填写角色、用户或审批状态，它就可以通过生成 `role="admin"` 或 `approved=true` 自行越权。权限必须来自登录态或服务身份形成的 `ExecutionContext`。

### Q5：为什么只有幂等工具的临时错误适合 Runtime 重试？

幂等操作执行一次和多次的业务效果相同。取消订单是写操作；超时时上游可能已经取消成功，盲目重试可能重复产生副作用，所以本工具禁止 Runtime 自动重试。

### Q6：插件机制怎样避免把治理逻辑写进每个工具？

工具插件只提供元数据、Schema 和 Handler，再统一注册到 Runtime。参数校验、授权、审批、超时、重试、结果封装和审计都由 Runtime 集中处理，因此新增工具不需要复制整套治理代码。
