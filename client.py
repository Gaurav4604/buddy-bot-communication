import asyncio
import json
import uuid
from aiortc import (
    RTCPeerConnection,
    RTCDataChannel,
    RTCConfiguration,
    RTCIceServer,
    RTCSessionDescription,
)
from aiohttp import ClientSession, WSMsgType


class Node:
    def __init__(self, url: str):
        self.url = url
        self.client_id = str(uuid.uuid4())
        self.ws = None
        self.session = None
        self.pcs = {}  # peer_id -> RTCPeerConnection
        self.channels = {}  # peer_id -> {channel_name: RTCDataChannel}
        self.callbacks = {}  # channel_name -> callback
        self.channels_list = []

    async def connect(self, channels: list):
        self.channels_list = channels
        self.session = ClientSession()
        self.ws = await self.session.ws_connect(
            f"{self.url}/ws"
        )  # aiohttp WS client :contentReference[oaicite:2]{index=2}
        # Tell server who we are and which channels we want
        await self.ws.send_json(
            {"type": "register", "channels": channels}
        )  # new register handshake
        asyncio.create_task(self._handle_ws_messages())
        print(f"Connected to {self.url} with client ID {self.client_id}")

    async def _handle_ws_messages(self):
        async for msg in self.ws:
            if msg.type == WSMsgType.TEXT:
                data = json.loads(msg.data)
                t = data["type"]
                if t == "peer":
                    # new peer joined → start offer
                    await self._create_peer_connection(data["peer_id"])
                elif t == "offer":
                    await self._handle_offer(data["source_id"], data["offer"])
                elif t == "answer":
                    await self._handle_answer(data["source_id"], data["answer"])
                elif t == "candidate":
                    await self._handle_candidate(data["source_id"], data["candidate"])
                elif t == "message":
                    # data‑channel payload
                    ch = data["channel"]
                    if ch in self.callbacks:
                        self.callbacks[ch](data["message"])
                elif t == "peer_disconnect":
                    pid = data["peer_id"]
                    if pid in self.pcs:
                        await self.pcs[pid].close()
                        del self.pcs[pid], self.channels[pid]

    async def _setup_ice(self, pc, peer_id):
        @pc.on("icecandidate")
        async def on_icecandidate(evt):
            if evt.candidate:
                await self.ws.send_json(
                    {
                        "type": "candidate",
                        "candidate": {
                            "candidate": evt.candidate.candidate,
                            "sdpMid": evt.candidate.sdpMid,
                            "sdpMLineIndex": evt.candidate.sdpMLineIndex,
                        },
                        "target_id": peer_id,
                    }
                )

    def _setup_channel(self, dc: RTCDataChannel, peer_id: str, label: str):
        buffer = []

        @dc.on("open")
        def on_open():
            print(
                f"Data channel {label} opened with peer {peer_id}"
            )  # now safe to send :contentReference[oaicite:3]{index=3}
            # start continuous publishing if this is a publisher channel
            if label == "/control":
                asyncio.create_task(self._control_loop(peer_id, dc))
            elif label == "/data":
                asyncio.create_task(self._data_loop(peer_id, dc))
            # flush any early-buffered messages
            for m in buffer:
                dc.send(m)

        @dc.on("message")
        def on_message(msg):
            if label in self.callbacks:
                self.callbacks[label](msg)

        # override send to buffer until open :contentReference[oaicite:4]{index=4}
        orig_send = dc.send

        def send_override(msg):
            if dc.readyState == "open":
                orig_send(msg)
            else:
                buffer.append(msg)

        dc.send = send_override

        self.channels[peer_id][label] = dc

    async def _new_pc(self, peer_id: str, make_channels: bool):
        config = RTCConfiguration(
            iceServers=[RTCIceServer(urls="stun:stun.l.google.com:19302")]
        )
        pc = RTCPeerConnection(configuration=config)
        self.pcs[peer_id] = pc
        self.channels[peer_id] = {}
        await self._setup_ice(pc, peer_id)

        if make_channels:
            for label in self.channels_list:
                dc = pc.createDataChannel(
                    label
                )  # ensure SCTP m=application :contentReference[oaicite:5]{index=5}
                self._setup_channel(dc, peer_id, label)
        else:

            @pc.on("datachannel")
            def on_datachannel(evt):
                self._setup_channel(evt.channel, peer_id, evt.channel.label)

        return pc

    async def _create_peer_connection(self, peer_id):
        pc = await self._new_pc(peer_id, make_channels=True)
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await self.ws.send_json(
            {
                "type": "offer",
                "offer": {"type": offer.type, "sdp": offer.sdp},
                "target_id": peer_id,
            }
        )

    async def _handle_offer(self, peer_id, offer):
        pc = await self._new_pc(peer_id, make_channels=False)
        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=offer["sdp"], type=offer["type"])
        )
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)
        await self.ws.send_json(
            {
                "type": "answer",
                "answer": {"type": answer.type, "sdp": answer.sdp},
                "target_id": peer_id,
            }
        )

    async def _handle_answer(self, peer_id, answer):
        await self.pcs[peer_id].setRemoteDescription(
            RTCSessionDescription(sdp=answer["sdp"], type=answer["type"])
        )

    async def _handle_candidate(self, peer_id, candidate):
        await self.pcs[peer_id].addIceCandidate(
            {
                "candidate": candidate["candidate"],
                "sdpMid": candidate["sdpMid"],
                "sdpMLineIndex": candidate["sdpMLineIndex"],
            }
        )

    async def _control_loop(self, peer_id, dc):
        while True:
            msg = json.dumps({"command": "start", "value": 10})
            dc.send(msg)
            await asyncio.sleep(1)

    async def _data_loop(self, peer_id, dc):
        while True:
            payload = {
                "data": "sensor reading",
                "timestamp": asyncio.get_event_loop().time(),
            }
            dc.send(json.dumps(payload))
            await asyncio.sleep(2)

    async def subscribe(self, channel: str, callback):
        self.callbacks[channel] = callback

    async def disconnect(self):
        for pc in self.pcs.values():
            await pc.close()
        await self.ws.close()
        await self.session.close()


async def main():
    server = "http://localhost:7000"
    node1 = Node(server)
    node2 = Node(server)
    channels = ["/control", "/data"]

    # both register and announce channels
    await asyncio.gather(node1.connect(channels), node2.connect(channels))

    # set up subscriptions
    await node1.subscribe("/data", lambda m: print("Node1 got /data:", m))
    await node2.subscribe("/control", lambda m: print("Node2 got /control:", m))

    # keep running
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
