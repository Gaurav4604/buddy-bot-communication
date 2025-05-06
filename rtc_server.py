import asyncio
import aiohttp
from aiohttp import web
import json


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Assign peer ID (simple incrementing ID)
    peer_id = len(request.app["peers"])
    request.app["peers"][peer_id] = ws
    print(f"Peer {peer_id} connected")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                target_id = data.get("to")
                if target_id in request.app["peers"]:
                    await request.app["peers"][target_id].send_json(
                        {"from": peer_id, "type": data["type"], "data": data["data"]}
                    )
                else:
                    await ws.send_json({"error": f"Peer {target_id} not found"})
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                break
    finally:
        request.app["peers"].pop(peer_id)
        print(f"Peer {peer_id} disconnected")

    return ws


async def init_app():
    app = web.Application()
    app["peers"] = {}
    app.router.add_get("/ws", websocket_handler)
    return app


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    web.run_app(app, port=8080)
