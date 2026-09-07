from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from openai import AsyncOpenAI


ModelEventType = Literal[
    # 模型新生成的一小段文字，不保证是完整句子或完整单词。
    "text.delta",
    # 模型停止生成，并报告 stop、length 等结束原因。
    "model.finished",
    # 本次请求消耗的输入、输出和总 Token 数。
    "model.usage",
]


@dataclass(frozen=True, slots=True)
class ModelStreamEvent:
    """Adapter 输出的统一内部事件，屏蔽不同模型供应商的数据格式。"""

    # 事件种类只能是 ModelEventType 中定义的三种字符串。
    type: ModelEventType
    # 每类事件的具体数据，例如 {"delta": "你"}。
    data: dict[str, Any]


class StreamingModel(Protocol):
    """Agent Loop 对流式模型的最小接口要求。"""

    def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        # Protocol 只描述接口，不在这里提供具体实现。
        # 任何拥有同样 stream() 方法的对象都可被当作 StreamingModel 使用。
        ...


class DeepSeekChatAdapter:
    """把 DeepSeek/OpenAI SDK 的 chunk 转换成 ModelStreamEvent。"""

    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        # client 由外部创建并注入，Adapter 不负责读取或保存 API Key。
        self.client = client
        # 保存模型名，便于测试时替换，也避免在 stream() 中写死。
        self.model = model

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        """调用 DeepSeek 流式接口，并逐个产出统一的模型事件。"""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            # 要求供应商边生成边返回，而不是等待完整答案。
            stream=True,
            max_tokens=1024,
            # 要求最后的 chunk 额外携带 Token 用量。
            stream_options={"include_usage": True},
            # 关闭 DeepSeek 思考模式，本作业只处理最终回答文本。
            extra_body={"thinking": {"type": "disabled"}},
        )

        # AsyncOpenAI 已经处理网络 bytes、上游 SSE 和 JSON；这里拿到的是
        # 一个完整可解析的 ChatCompletionChunk，而不是原始网络字节块。
        async for chunk in response:
            # usage chunk 可能没有 choices，因此取下标前必须先判断。
            choice = chunk.choices[0] if chunk.choices else None

            # 将供应商的文本增量转换成统一的 text.delta 事件。
            if choice and choice.delta.content:
                yield ModelStreamEvent(
                    type="text.delta",
                    data={"delta": choice.delta.content},
                )

            # finish_reason 表示模型停止生成；它不是网络流的分隔符。
            if choice and choice.finish_reason:
                yield ModelStreamEvent(
                    type="model.finished",
                    data={"finish_reason": choice.finish_reason},
                )

            # include_usage=True 时，最后的 chunk 通常会携带用量信息。
            if chunk.usage is not None:
                yield ModelStreamEvent(
                    type="model.usage",
                    data={
                        "input_tokens": chunk.usage.prompt_tokens,
                        "output_tokens": chunk.usage.completion_tokens,
                        "total_tokens": chunk.usage.total_tokens,
                    },
                )
