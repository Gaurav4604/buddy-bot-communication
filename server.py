import asyncio, json, uuid
from aiohttp import web, WSMsgType

clients = {}  # client_id -> WebSocketResponse
peer_channels = {}  # client_id -> list of channels


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    client_id = str(uuid.uuid4())
    clients[client_id] = ws
    peer_channels[client_id] = []
    print(f"[SERVER] Client connected: {client_id}")

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            t = data.get("type")

            if t == "register":
                # record channels and notify all others of new peer
                peer_channels[client_id] = data["channels"]
                for oid, ows in clients.items():
                    if oid != client_id and not ows.closed:
                        await ows.send_json({"type": "peer", "peer_id": client_id})
                # also tell the newcomer about existing peers
                for oid in clients:
                    if oid != client_id:
                        await ws.send_json({"type": "peer", "peer_id": oid})

            elif t in ("offer", "answer", "candidate"):
                target = data["target_id"]
                if target in clients and not clients[target].closed:
                    payload = {"type": t}
                    # unify field name
                    payload[t] = data[t] if t != "candidate" else data["candidate"]
                    payload["source_id"] = client_id
                    await clients[target].send_json(payload)

            elif t == "message":
                ch = data["channel"]
                for pid, chans in peer_channels.items():
                    if pid != client_id and ch in chans and not clients[pid].closed:
                        await clients[pid].send_json(
                            {
                                "type": "message",
                                "channel": ch,
                                "message": data["message"],
                                "source_id": client_id,
                            }
                        )

    finally:
        print(f"[SERVER] Client disconnected: {client_id}")
        del clients[client_id], peer_channels[client_id]
        for ows in clients.values():
            if not ows.closed:
                await ows.send_json({"type": "peer_disconnect", "peer_id": client_id})
        await ws.close()

    return ws


app = web.Application()
app.router.add_get("/ws", websocket_handler)
app.router.add_get("/", lambda r: web.Response(text="Signaling server running"))

if __name__ == "__main__":
    web.run_app(app, port=7000)
