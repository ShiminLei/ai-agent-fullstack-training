# 4.3 协议审查验收

## 问题 1：让模型填写 user_id、role 或 approved

错误示例：

```python
class BadInput(BaseModel):
    status: str
    user_id: str
    role: str
    approved: bool
```

影响的是可信边界。模型输出不可信，允许它填写这些字段等同于允许调用者声明自己的身份、角色和审批状态。正确实现的 Input Schema 只包含查询条件；身份和权限应由登录态或 Harness 注入。

对应测试：`test_identity_role_and_approval_are_forbidden_model_arguments`。

## 问题 2：把完整 ToolDefinition 或 Handler 发给模型

影响模型可见能力与执行边界。模型只需要名称、描述和 Input Schema；Handler、权限和内部依赖属于应用内部信息。暴露它们会造成实现细节泄漏，也会模糊“模型提议、Runtime 执行”的边界。

正确实现通过 `to_model_tool()` 做显式投影。

对应测试：`test_model_tool_does_not_leak_internal_fields`。

## 问题 3：直接相信参数 JSON

错误示例：

```python
args = json.loads(call.arguments_json)
result = await handler(args)
```

影响协议和可信边界。普通 JSON 解析只能证明语法有效，不能验证必填字段、枚举、范围和额外字段。正确实现先调用 `SearchOrdersInput.model_validate_json()`，验证通过后才能进入 Handler。

对应测试：`test_arguments_are_validated_before_handler`，它同时断言非法参数下 Handler 调用次数为零。

## 问题 4：Tool Result 没有原始 tool_call_id

影响结果关联。两个同名调用可能并行执行并乱序完成；没有原始 ID，Agent 无法判断每份结果属于哪次调用。

正确实现始终使用：

```python
ToolResult(tool_call_id=call.id, content=...)
```

对应测试：`test_results_keep_original_call_ids` 和 `test_tool_result_requires_original_call_id_field`。
