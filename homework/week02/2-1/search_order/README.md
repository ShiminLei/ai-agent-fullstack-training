# search_orders 最小调用作业

本目录完成课件 4.2 的四步最小调用，并用测试覆盖 4.3 的四类协议错误。订单数据全部为模拟数据，不访问真实商城。

## 文件顺序

1. `search_orders.py`：Schema、ToolDefinition、模拟 Handler、两个 Tool Call 与最小执行函数。
2. `demo.py`：严格按照作业四步打印运行结果。
3. `test_search_orders.py`：可执行验收测试。
4. `protocol_review.md`：四类协议错误的影响和对应测试。

## 运行

先安装依赖：

```bash
python -m pip install -r requirements.txt
```

运行四步演示：

```bash
python demo.py
```

运行验收测试：

```bash
python test_search_orders.py
```

## 最小协议闭环

```text
定义名称、描述和 Input Schema
→ 投影模型可见 Tool JSON
→ 构造两个不同 ID 的 Tool Call
→ 校验参数并执行模拟 Handler
→ 生成保留原 tool_call_id 的 Tool Result
```

这个练习不实现 Registry、Snapshot、完整权限系统或重试框架。
