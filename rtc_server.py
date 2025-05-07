import asyncio
import aiohttp
from aiohttp import web
import json
from typing import Dict, Any, Optional


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws: web.WebSocketResponse = web.WebSocketResponse()
    await ws.prepare(request)

    # Assign peer ID (simple incrementing ID)
    peers: Dict[int, web.WebSocketResponse] = request.app["peers"]
    peer_id: int = len(peers)
    peers[peer_id] = ws
    print(f"Peer {peer_id} connected")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data: Dict[str, Any] = json.loads(msg.data)
                    target_id: Optional[int] = data.get("to")

                    if target_id in peers:
                        await peers[target_id].send_json(
                            {
                                "from": peer_id,
                                "type": data["type"],
                                "data": data["data"],
                            }
                        )
                    else:
                        await ws.send_json({"error": f"Peer {target_id} not found"})
                except (json.JSONDecodeError, KeyError) as e:
                    await ws.send_json({"error": f"Invalid message format: {e}"})

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                break

    finally:
        peers.pop(peer_id, None)
        print(f"Peer {peer_id} disconnected")

    return ws


async def init_app() -> web.Application:
    app: web.Application = web.Application()
    app["peers"] = {}
    app.router.add_get("/ws", websocket_handler)
    return app


if __name__ == "__main__":
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    app: web.Application = loop.run_until_complete(init_app())
    web.run_app(app, port=8080)
