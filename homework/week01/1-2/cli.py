import argparse
import asyncio
import json

import httpx

from app.sse import SSEDecoder


TERMINAL_TYPES = {"run.completed", "run.failed", "run.cancelled"}


async def consume_run(base_url: str, prompt: str) -> None:
    """创建 Run，并按 seq 去重消费 SSE；网络断开后只恢复传输。"""

    timeout = httpx.Timeout(30.0, read=None)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        response = await client.post("/v1/runs", json={"prompt": prompt})
        response.raise_for_status()
        run_id = response.json()["run_id"]
        print(f"run_id={run_id}")

        last_seq = -1
        try:
            while True:
                decoder = SSEDecoder()
                # 第一次订阅不带游标；只有断线后才声明已经消费到的 seq。
                params = {"after_seq": last_seq} if last_seq >= 0 else None
                async with client.stream(
                    "GET",
                    f"/v1/runs/{run_id}/events",
                    params=params,
                ) as stream:
                    stream.raise_for_status()
                    async for chunk in stream.aiter_bytes():
                        for frame in decoder.feed_bytes(chunk):
                            payload = json.loads(frame.data)
                            seq = int(payload["seq"])
                            # 重连可能重放最后一条事件，客户端必须去重。
                            if seq <= last_seq:
                                continue
                            last_seq = seq
                            event_type = payload["type"]
                            if event_type == "text.delta":
                                print(payload["data"]["delta"], end="", flush=True)
                            elif event_type == "run.retrying":
                                print("\n[模型连接重试中]", flush=True)
                            elif event_type in TERMINAL_TYPES:
                                print(f"\n[{event_type}]", flush=True)
                                return
                # 连接意外结束时继续循环，只重订阅，不重新创建 Run。
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            await client.post(f"/v1/runs/{run_id}/cancel")
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming Agent Gateway CLI")
    parser.add_argument("prompt", help="发送给 Agent 的任务")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    try:
        asyncio.run(consume_run(args.base_url, args.prompt))
    except KeyboardInterrupt:
        print("\n已停止本地 CLI；若 Run 仍在运行，请调用 cancel 接口。")


if __name__ == "__main__":
    main()
