import socketio
from aiohttp import web

# Create an asynchronous Socket.IO server using aiohttp.
sio = socketio.AsyncServer(async_mode="aiohttp")
app = web.Application()
sio.attach(app)


async def index(request):
    return web.Response(text="Socket.IO Server is Running", content_type="text/html")


# Add the route using the app's router.
app.router.add_get("/", index)


# ----- /control Namespace -----
@sio.event(namespace="/control")
async def connect(sid, environ):
    print(f"[CONTROL] Client connected: {sid}")


@sio.event(namespace="/control")
async def disconnect(sid):
    print(f"[CONTROL] Client disconnected: {sid}")


@sio.on("message", namespace="/control")
async def control_message(sid, data):
    print(f"[CONTROL] Received from {sid}: {data}")
    # Broadcast the message to all other clients in /control.
    await sio.emit("message", data, skip_sid=sid, namespace="/control")


# ----- /data Namespace -----
@sio.event(namespace="/data")
async def connect(sid, environ):
    print(f"[DATA] Client connected: {sid}")


@sio.event(namespace="/data")
async def disconnect(sid):
    print(f"[DATA] Client disconnected: {sid}")


@sio.on("message", namespace="/data")
async def data_message(sid, data):
    print(f"[DATA] Received from {sid}: {data}")
    # Broadcast the message to all other clients in /data.
    await sio.emit("message", data, skip_sid=sid, namespace="/data")


if __name__ == "__main__":
    web.run_app(app, port=7000)
