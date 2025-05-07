import asyncio
import json
from typing import Dict, List, Any, Set, Optional

import aiohttp
from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCIceCandidate,
    RTCDataChannel,
)
from aiortc.rtcconfiguration import RTCConfiguration, RTCIceServer


class Node:
    def __init__(
        self, peer_id: int, signaling_url: str = "ws://localhost:8080/ws"
    ) -> None:
        self.peer_id = peer_id
        self.signaling_url = signaling_url
        self._pcs: Dict[int, RTCPeerConnection] = {}
        self._outgoing_channels: Dict[int, RTCDataChannel] = {}
        self._incoming_channels: Dict[int, RTCDataChannel] = {}
        self._running = True
        self._input_task: Optional[asyncio.Task] = None

    async def _create_peer_connection(
        self, other_id: int, ws: aiohttp.ClientWebSocketResponse
    ) -> RTCPeerConnection:
        """Create and configure a new RTCPeerConnection for communication with other_id."""
        config = RTCConfiguration([RTCIceServer(urls="stun:stun.l.google.com:19302")])
        pc = RTCPeerConnection(configuration=config)
        self._pcs[other_id] = pc

        # Create an OUTGOING data channel for this peer
        outgoing_channel = pc.createDataChannel("chat")
        self._outgoing_channels[other_id] = outgoing_channel

        @outgoing_channel.on("open")
        def on_open() -> None:
            print(f"[{self.peer_id}] Outgoing DataChannel to {other_id} opened")

        @outgoing_channel.on("message")
        def on_message(message: str) -> None:
            # This receives messages on the channel *we* created
            print(
                f"[{self.peer_id}] Received on outgoing channel from {other_id}: {message}"
            )

        # Handle INCOMING data channels created by the remote peer
        @pc.on("datachannel")
        def on_datachannel(incoming_channel: RTCDataChannel) -> None:
            self._incoming_channels[other_id] = incoming_channel
            print(f"[{self.peer_id}] Incoming DataChannel from {other_id} received")

            @incoming_channel.on("open")
            def _() -> None:
                print(f"[{self.peer_id}] Incoming DataChannel from {other_id} opened")

            @incoming_channel.on("message")
            def _msg(m: str) -> None:
                print(f"[{self.peer_id}] Received from {other_id}: {m}")

        @pc.on("icecandidate")
        async def on_icecandidate(event: Any) -> None:
            if event.candidate:
                candidate_data = {
                    "candidate": event.candidate.sdp,
                    "sdpMid": event.candidate.sdp_mid,
                    "sdpMLineIndex": event.candidate.sdp_m_line_index,
                }
                if not ws.closed:
                    try:
                        await ws.send_json(
                            {
                                "from": self.peer_id,
                                "to": other_id,
                                "type": "candidate",
                                "data": candidate_data,
                            }
                        )
                    except Exception:
                        pass  # Error sending candidate, connection might be closing

        @pc.on("connectionstatechange")
        async def on_connectionstatechange() -> None:
            print(
                f"[{self.peer_id}] Connection state with {other_id}: {pc.connectionState}"
            )
            if pc.connectionState == "failed":
                print(
                    f"[{self.peer_id}] Connection with {other_id} failed. Attempting ICE restart."
                )
                try:
                    await pc.restartIce()
                    if self.peer_id < other_id:
                        print(
                            f"[{self.peer_id}] Re-sending offer to {other_id} after ICE restart."
                        )
                        offer = await pc.createOffer()
                        await pc.setLocalDescription(offer)
                        if not ws.closed:
                            await ws.send_json(
                                {
                                    "from": self.peer_id,
                                    "to": other_id,
                                    "type": "offer",
                                    "data": {
                                        "sdp": pc.localDescription.sdp,
                                        "type": pc.localDescription.type,
                                    },
                                }
                            )

                except Exception as e:
                    print(
                        f"[{self.peer_id}] Error during ICE restart with {other_id}: {e}"
                    )

        return pc

    async def _handle_user_input(self) -> None:
        """Reads user input and broadcasts to all open data channels."""
        print(f"[{self.peer_id}] Enter messages to broadcast. Type 'quit' to exit.")
        while self._running:
            try:
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, input, f"[{self.peer_id}] Broadcast: "
                )

                if msg.strip().lower() == "quit":
                    self._running = False
                    break

                if not msg.strip():
                    continue

                sent_count = 0
                for target_id, channel in list(
                    self._outgoing_channels.items()
                ):  # Iterate over a copy
                    if channel.readyState == "open":
                        try:
                            channel.send(msg)
                            sent_count += 1
                        except Exception as e:
                            print(
                                f"[{self.peer_id}] Error sending message to {target_id}: {e}"
                            )
                            # Consider removing the channel/PC if sending consistently fails

                if sent_count == 0:
                    print(
                        f"[{self.peer_id}] No open data channels to broadcast message."
                    )

            except Exception as e:
                print(f"[{self.peer_id}] Error getting user input or broadcasting: {e}")
                await asyncio.sleep(0.1)  # Prevent tight loop

    async def connect_to(self, peer_ids: List[int]) -> None:
        """Connect this node to specified peer_ids via signaling and WebRTC."""
        async with aiohttp.ClientSession() as session:
            ws = None
            try:
                ws = await session.ws_connect(self.signaling_url)
                print(f"[{self.peer_id}] Registered with signaling server")

                await ws.send_json(
                    {
                        "current_peer_id": self.peer_id,
                        "listen_for": peer_ids,
                        "data": "connected",
                    }
                )

                offers_sent: Set[int] = set()

                for pid in peer_ids:
                    if pid == self.peer_id:
                        print(f"[{self.peer_id}] Skipping connection to self ({pid})")
                        continue
                    pc = await self._create_peer_connection(pid, ws)

                    if self.peer_id < pid:
                        offers_sent.add(pid)
                        offer = await pc.createOffer()
                        await pc.setLocalDescription(offer)
                        if not ws.closed:
                            try:
                                await ws.send_json(
                                    {
                                        "from": self.peer_id,
                                        "to": pid,
                                        "type": "offer",
                                        "data": {
                                            "sdp": pc.localDescription.sdp,
                                            "type": pc.localDescription.type,
                                        },
                                    }
                                )
                                print(f"[{self.peer_id}] → offer → [{pid}]")
                            except Exception:
                                pass  # Error sending offer, peer might be gone

                self._input_task = asyncio.create_task(self._handle_user_input())

                while self._running and not ws.closed:
                    try:
                        msg = await ws.receive(timeout=1.0)

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload: Dict[str, Any] = json.loads(msg.data)
                            except json.JSONDecodeError:
                                print(
                                    f"[{self.peer_id}] Error decoding JSON: {msg.data}"
                                )
                                continue

                            frm = payload.get("from")
                            to = payload.get("to")
                            typ = payload.get("type")
                            data = payload.get("data")

                            if to is not None and to != self.peer_id:
                                continue

                            if (
                                frm is None
                                or typ is None
                                or (data is None and typ != "disconnect")
                            ):
                                print(
                                    f"[{self.peer_id}] Warning: Incomplete message: {payload}"
                                )
                                continue

                            pc = self._pcs.get(frm)
                            if not pc and frm in peer_ids:
                                print(
                                    f"[{self.peer_id}] Creating PC for new peer {frm}"
                                )
                                pc = await self._create_peer_connection(frm, ws)
                            elif not pc and typ != "disconnect":
                                continue  # Ignore messages from unknown peers
                            elif not pc and typ == "disconnect":
                                print(f"[{self.peer_id}] Peer {frm} disconnected.")
                                if frm in self._outgoing_channels:
                                    del self._outgoing_channels[frm]
                                if frm in self._incoming_channels:
                                    del self._incoming_channels[frm]
                                continue

                            if typ == "offer":
                                if self.peer_id > frm:
                                    print(f"[{self.peer_id}] ← offer ← [{frm}]")
                                    offer = RTCSessionDescription(
                                        sdp=data["sdp"], type=data["type"]
                                    )
                                    await pc.setRemoteDescription(offer)
                                    answer = await pc.createAnswer()
                                    await pc.setLocalDescription(answer)
                                    if not ws.closed:
                                        try:
                                            await ws.send_json(
                                                {
                                                    "from": self.peer_id,
                                                    "to": frm,
                                                    "type": "answer",
                                                    "data": {
                                                        "sdp": pc.localDescription.sdp,
                                                        "type": pc.localDescription.type,
                                                    },
                                                }
                                            )
                                            print(
                                                f"[{self.peer_id}] → answer → [{frm}]"
                                            )
                                        except Exception:
                                            pass  # Error sending answer
                                else:
                                    print(
                                        f"[{self.peer_id}] Warning: Unexpected offer from {frm}"
                                    )

                            elif typ == "answer":
                                if self.peer_id < frm:
                                    print(f"[{self.peer_id}] ← answer ← [{frm}]")
                                    answer = RTCSessionDescription(
                                        sdp=data["sdp"], type=data["type"]
                                    )
                                    await pc.setRemoteDescription(answer)
                                else:
                                    print(
                                        f"[{self.peer_id}] Warning: Unexpected answer from {frm}"
                                    )

                            elif typ == "candidate":
                                try:
                                    candidate = RTCIceCandidate(
                                        sdp=data["candidate"],
                                        sdpMid=data.get("sdpMid"),
                                        sdpMLineIndex=data.get("sdpMLineIndex"),
                                    )
                                    await pc.addIceCandidate(candidate)
                                except Exception as e:
                                    print(
                                        f"[{self.peer_id}] Error adding ICE candidate from {frm}: {e}"
                                    )

                            elif typ == "peer_info" or typ == "connection_opportunity":
                                print(f"[{self.peer_id}] Received {typ} from {frm}")
                                if (
                                    self.peer_id < frm
                                    and frm in peer_ids
                                    and frm not in offers_sent
                                ):
                                    offers_sent.add(frm)
                                    try:
                                        offer = await pc.createOffer()
                                        await pc.setLocalDescription(offer)
                                        if not ws.closed:
                                            await ws.send_json(
                                                {
                                                    "from": self.peer_id,
                                                    "to": frm,
                                                    "type": "offer",
                                                    "data": {
                                                        "sdp": pc.localDescription.sdp,
                                                        "type": pc.localDescription.type,
                                                    },
                                                }
                                            )
                                            print(
                                                f"[{self.peer_id}] → offer → [{frm}] (after {typ})"
                                            )
                                    except Exception:
                                        pass  # Error sending offer

                            elif typ == "request_offer":
                                print(
                                    f"[{self.peer_id}] Received request_offer from {frm}"
                                )
                                if (
                                    self.peer_id < frm
                                    and frm in peer_ids
                                    and frm in offers_sent
                                ):
                                    try:
                                        offer = await pc.createOffer()
                                        await pc.setLocalDescription(offer)
                                        if not ws.closed:
                                            await ws.send_json(
                                                {
                                                    "from": self.peer_id,
                                                    "to": frm,
                                                    "type": "offer",
                                                    "data": {
                                                        "sdp": pc.localDescription.sdp,
                                                        "type": pc.localDescription.type,
                                                    },
                                                }
                                            )
                                            print(
                                                f"[{self.peer_id}] → offer → [{frm}] (re-sending on request)"
                                            )
                                    except Exception:
                                        pass  # Error re-sending offer

                            elif typ == "disconnect":
                                print(f"[{self.peer_id}] Peer {frm} disconnected.")
                                if frm in self._pcs:
                                    await self._pcs[frm].close()
                                    del self._pcs[frm]
                                if frm in self._outgoing_channels:
                                    del self._outgoing_channels[frm]
                                if frm in self._incoming_channels:
                                    del self._incoming_channels[frm]

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"[{self.peer_id}] WebSocket error: {msg.data}")
                            self._running = False
                            break

                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            print(f"[{self.peer_id}] WebSocket connection closed.")
                            self._running = False
                            break

                        elif msg.type == aiohttp.WSMsgType.PING:
                            if not ws.closed:
                                await ws.pong()

                        elif msg.type == aiohttp.WSMsgType.PONG:
                            pass

                        else:
                            print(
                                f"[{self.peer_id}] Received unknown message type: {msg.type}"
                            )

                    except asyncio.TimeoutError:
                        continue
                    except aiohttp.WSServerHandshakeError as e:
                        print(f"[{self.peer_id}] WebSocket handshake error: {e}")
                        self._running = False
                        break
                    except Exception as e:
                        print(f"[{self.peer_id}] Error processing message: {e}")
                        await asyncio.sleep(0.1)

            except aiohttp.ClientConnectorError as e:
                print(f"[{self.peer_id}] Connection error to signaling server: {e}")
            except Exception as e:
                print(f"[{self.peer_id}] An unexpected error occurred: {e}")

            finally:
                print(f"[{self.peer_id}] Shutting down...")

                if self._input_task and not self._input_task.done():
                    self._input_task.cancel()
                    try:
                        await self._input_task
                    except asyncio.CancelledError:
                        pass  # Expected cancellation
                    except Exception as e:
                        print(f"[{self.peer_id}] Error cancelling input task: {e}")

                for pid, pc in list(self._pcs.items()):
                    if not pc.closed:
                        print(f"[{self.peer_id}] Closing connection with {pid}...")
                        await pc.close()
                self._pcs.clear()
                self._outgoing_channels.clear()
                self._incoming_channels.clear()

                if ws and not ws.closed:
                    await ws.close()
                    print(f"[{self.peer_id}] Signaling WebSocket closed.")


if __name__ == "__main__":

    async def main():
        peer_id = int(input("Enter your peer ID: "))
        listen_for = list(
            map(int, input("Enter target peer IDs (space-separated): ").split())
        )
        node = Node(peer_id)
        try:
            await node.connect_to(listen_for)
        except KeyboardInterrupt:
            print("Shutting down from KeyboardInterrupt...")
        finally:
            pass  # Cleanup is handled in connect_to's finally block

    asyncio.run(main())
