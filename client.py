import asyncio
import json
from socketio import AsyncClient


class Node:
    def __init__(self, url: str):
        self.url = url
        self.sio = AsyncClient()

    async def connect(self, namespaces: list):
        # Connect to the server for the specified namespaces.
        await self.sio.connect(self.url, namespaces=namespaces)
        print(f"Connected to {self.url} on namespaces {', '.join(namespaces)}")

    async def publish(self, namespace: str, data: dict):
        # Publish a message on the specified namespace using event "message".
        await self.sio.emit("message", json.dumps(data), namespace=namespace)

    async def subscribe(self, namespace: str, callback):
        # Register the event handler for the "message" event on the specified namespace.
        self.sio.on("message", callback, namespace=namespace)

    async def disconnect(self):
        await self.sio.disconnect()


async def main():
    server_url = "http://localhost:7000"

    # Create two node instances.
    node1 = Node(server_url)
    node2 = Node(server_url)

    channels = ["/control", "/vision-channel-1", "/vision-channel-2", "/data"]

    # Connect both nodes to the server.
    await asyncio.gather(node1.connect(channels), node2.connect(channels))

    # ---- Setup for Node1 ----
    # Node1: Publish on /control and subscribe on /data.
    async def node1_publish_control():
        while True:
            data = {"command": "start", "value": 10}
            await node1.publish("/control", data)
            print(f"Node1 published on /control: {data}")
            await asyncio.sleep(1)

    def node1_on_data(message):
        print(f"Node1 received on /data: {message}")

    await node1.subscribe("/data", node1_on_data)

    # ---- Setup for Node2 ----
    # Node2: Subscribe on /control and publish on /data.
    def node2_on_control(message):
        print(f"Node2 received on /control: {message}")

    await node2.subscribe("/control", node2_on_control)

    async def node2_publish_data():
        while True:
            data = {
                "data": "sensor reading",
                "timestamp": asyncio.get_event_loop().time(),
            }
            await node2.publish("/data", data)
            print(f"Node2 published on /data: {data}")
            await asyncio.sleep(2)

    # Run publisher tasks concurrently.
    await asyncio.gather(node1_publish_control(), node2_publish_data())


if __name__ == "__main__":
    asyncio.run(main())
