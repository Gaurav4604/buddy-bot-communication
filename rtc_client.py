import asyncio
import json
from typing import Optional

import aiohttp
from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCIceCandidate,
    RTCDataChannel,
)
from aiortc.rtcconfiguration import RTCConfiguration, RTCIceServer


async def send_messages(channel: RTCDataChannel) -> None:
    while True:
        try:
            message: str = await asyncio.get_event_loop().run_in_executor(
                None, input, "Enter message (or 'quit' to exit): "
            )
            if message.lower() == "quit":
                channel.close()
                break
            channel.send(message)
            print(f"Sent: {message}")
        except Exception as e:
            print(f"Error sending message: {e}")
            break


async def run_client(peer_id: int, target_id: int) -> None:
    config: RTCConfiguration = RTCConfiguration(
        iceServers=[
            RTCIceServer(urls="stun:stun.l.google.com:19302"),
        ]
    )
    pc: RTCPeerConnection = RTCPeerConnection(configuration=config)
    print(f"Peer {peer_id} created RTCPeerConnection")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect("ws://localhost:8080/ws") as ws:
                print(f"Peer {peer_id} connected to WebSocket server")

                # Offerer creates the data channel
                channel: RTCDataChannel = pc.createDataChannel("chat")
                print(f"Peer {peer_id} created DataChannel")

                @channel.on("open")
                def on_open() -> None:
                    print(f"Peer {peer_id}: Data channel opened!")
                    asyncio.ensure_future(send_messages(channel))

                @channel.on("message")
                def on_message(message: str) -> None:
                    print(f"Peer {peer_id} Received: {message}")

                @channel.on("close")
                def on_close() -> None:
                    print(f"Peer {peer_id}: Data channel closed")

                @pc.on("icecandidate")
                async def on_icecandidate(event: Optional[object]) -> None:
                    if event and hasattr(event, "candidate") and event.candidate:
                        print(
                            f"Peer {peer_id} sending ICE candidate: {event.candidate.sdp}"
                        )
                        await ws.send_json(
                            {
                                "to": target_id,
                                "type": "candidate",
                                "data": {
                                    "candidate": event.candidate.sdp,
                                    "sdpMid": event.candidate.sdp_mid,
                                    "sdpMLineIndex": event.candidate.sdp_m_line_index,
                                },
                            }
                        )

                @pc.on("iceconnectionstatechange")
                async def on_iceconnectionstatechange() -> None:
                    print(
                        f"Peer {peer_id} ICE connection state: {pc.iceConnectionState}"
                    )
                    if pc.iceConnectionState == "failed":
                        print(f"Peer {peer_id}: ICE negotiation failed")
                        await pc.close()

                @pc.on("datachannel")
                def on_datachannel(incoming_channel: RTCDataChannel) -> None:
                    print(f"Peer {peer_id} received DataChannel")

                    @incoming_channel.on("open")
                    def on_open() -> None:
                        print(f"Peer {peer_id}: Data channel opened!")
                        asyncio.ensure_future(send_messages(incoming_channel))

                    @incoming_channel.on("message")
                    def on_message(message: str) -> None:
                        print(f"Peer {peer_id} Received: {message}")

                    @incoming_channel.on("close")
                    def on_close() -> None:
                        print(f"Peer {peer_id}: Data channel closed")

                async def handle_signaling() -> None:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data: dict = json.loads(msg.data)
                            print(f"Peer {peer_id} received signaling message: {data}")
                            if "from" in data and data["from"] == target_id:
                                try:
                                    if data["type"] == "offer":
                                        print(f"Peer {peer_id} processing offer")
                                        offer: RTCSessionDescription = (
                                            RTCSessionDescription(
                                                sdp=data["data"]["sdp"],
                                                type=data["data"]["type"],
                                            )
                                        )
                                        await pc.setRemoteDescription(offer)
                                        await pc.setLocalDescription(
                                            await pc.createAnswer()
                                        )
                                        await ws.send_json(
                                            {
                                                "to": target_id,
                                                "type": "answer",
                                                "data": {
                                                    "sdp": pc.localDescription.sdp,
                                                    "type": pc.localDescription.type,
                                                },
                                            }
                                        )
                                        print(f"Peer {peer_id} sent answer")
                                    elif data["type"] == "answer":
                                        print(f"Peer {peer_id} processing answer")
                                        answer: RTCSessionDescription = (
                                            RTCSessionDescription(
                                                sdp=data["data"]["sdp"],
                                                type=data["data"]["type"],
                                            )
                                        )
                                        await pc.setRemoteDescription(answer)
                                    elif data["type"] == "candidate":
                                        print(
                                            f"Peer {peer_id} processing ICE candidate"
                                        )
                                        candidate: RTCIceCandidate = RTCIceCandidate(
                                            sdp=data["data"]["candidate"],
                                            sdpMid=data["data"]["sdpMid"],
                                            sdpMLineIndex=data["data"]["sdpMLineIndex"],
                                        )
                                        await pc.addIceCandidate(candidate)
                                except Exception as e:
                                    print(
                                        f"Peer {peer_id} error processing signaling message: {e}"
                                    )
                            elif "error" in data:
                                print(
                                    f"Peer {peer_id} signaling error: {data['error']}"
                                )
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"Peer {peer_id} WebSocket error: {msg.data}")
                            break

                if peer_id == 0:
                    try:
                        await pc.setLocalDescription(await pc.createOffer())
                        print(f"Peer {peer_id} sending offer")
                        await ws.send_json(
                            {
                                "to": target_id,
                                "type": "offer",
                                "data": {
                                    "sdp": pc.localDescription.sdp,
                                    "type": pc.localDescription.type,
                                },
                            }
                        )
                    except Exception as e:
                        print(f"Peer {peer_id} error creating offer: {e}")
                        await pc.close()
                        return

                try:
                    await asyncio.wait_for(handle_signaling(), timeout=30)
                except asyncio.TimeoutError:
                    print(f"Peer {peer_id}: Signaling timed out")
                    await pc.close()
                finally:
                    await pc.close()
                    print(f"Peer {peer_id}: PeerConnection closed")

    except Exception as e:
        print(f"Error {e}")


async def main() -> None:
    try:
        peer_id: int = int(input("Enter your peer ID (0 or 1): "))
        target_id: int = int(input("Enter target peer ID (0 or 1): "))
        if peer_id not in [0, 1] or target_id not in [0, 1] or peer_id == target_id:
            print("Invalid peer IDs. Use 0 or 1, and ensure they differ.")
            return
        await run_client(peer_id, target_id)
    except ValueError:
        print("Peer IDs must be integers (0 or 1).")
    except Exception as e:
        print(f"Error starting client: {e}")


if __name__ == "__main__":
    asyncio.run(main())
