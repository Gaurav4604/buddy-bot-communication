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


# ----- /vision-channel-1 Namespace -----
@sio.event(namespace="/vision-channel-1")
async def connect(sid, environ):
    print(f"[VISION-CHANNEL-1] Client connected: {sid}")


@sio.event(namespace="/vision-channel-1")
async def disconnect(sid):
    print(f"[VISION-CHANNEL-1] Client disconnected: {sid}")


@sio.on("message", namespace="/vision-channel-1")
async def data_channel_1_message(sid, data):
    print(f"[VISION-CHANNEL-1] Received from {sid}: {data}")
    await sio.emit("message", data, skip_sid=sid, namespace="/vision-channel-1")


# ----- /vision-channel-2 Namespace -----
@sio.event(namespace="/vision-channel-2")
async def connect(sid, environ):
    print(f"[VISION-CHANNEL-2] Client connected: {sid}")


@sio.event(namespace="/vision-channel-2")
async def disconnect(sid):
    print(f"[VISION-CHANNEL-2] Client disconnected: {sid}")


@sio.on("message", namespace="/vision-channel-2")
async def data_channel_2_message(sid, data):
    print(f"[VISION-CHANNEL-2] Received from {sid}: {data}")
    await sio.emit("message", data, skip_sid=sid, namespace="/vision-channel-2")


if __name__ == "__main__":
    web.run_app(app, port=7000)
