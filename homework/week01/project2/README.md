# 多协议 LLM Gateway：教学实现

这个目录用于从零、分步骤实现第一周作业。每一步只增加一层能力，并通过测试确认当前抽象可以工作。

## 当前进度

- [x] 第一步：统一领域模型和 Adapter 接口
- [x] 第二步：Responses API Adapter
- [x] 第三步：Anthropic Messages API Adapter
- [x] 第四步：模型路由和 Gateway
- [ ] 第五步：FastAPI 普通与流式接口
- [ ] 第六步：结构化输出和 Prompt 版本
- [ ] 第七步：观测、重试和模型限流
- [ ] 第八步：完整验收脚本和使用文档

## 当前验证

```bash
uv run --extra dev pytest -q
uv run --extra dev ruff check .
```
