import asyncio
import json
from typing import Dict, List, Any, Set, Optional, Callable
import functools


import aiohttp
from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCIceCandidate,
    RTCDataChannel,
)
from aiortc.rtcconfiguration import RTCConfiguration, RTCIceServer

# Constants for data channel types (repeated for clarity in this standalone sample)
CHANNEL_TYPE_CHAT = "chat"
CHANNEL_TYPE_VIDEO_FORMAT = "video-stream-{}"


class Node:
    def __init__(
        self,
        peer_id: int,
        signaling_url: str = "ws://localhost:8080/ws",
        num_video_streams: int = 0,  # Number of video streams this node will handle (send/receive)
    ) -> None:
        self.peer_id = peer_id
        self.signaling_url = signaling_url
        self.num_video_streams = num_video_streams

        self._pcs: Dict[int, RTCPeerConnection] = {}

        # Data channels for chat (keyed by target_id)
        self._chat_outgoing_channels: Dict[int, RTCDataChannel] = {}
        self._chat_incoming_channels: Dict[int, RTCDataChannel] = {}

        # Data channels for video (keyed by target_id, then by stream_index)
        self._video_outgoing_channels: Dict[int, Dict[int, RTCDataChannel]] = {}
        self._video_incoming_channels: Dict[int, Dict[int, RTCDataChannel]] = {}

        self._running = True
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

        # Public handlers for incoming messages - can be overridden or assigned
        self.on_chat_message_received: Callable[[int, str], None] = (
            self._default_chat_handler
        )
        # Video handler now receives stream_index as well
        self.on_video_stream_data_received: Callable[[int, int, bytes], None] = (
            self._default_video_handler
        )

    async def _create_peer_connection(self, other_id: int) -> RTCPeerConnection:
        """Create and configure a new RTCPeerConnection for communication with other_id."""
        config = RTCConfiguration([RTCIceServer(urls="stun:stun.l.google.com:19302")])
        pc = RTCPeerConnection(configuration=config)
        self._pcs[other_id] = pc

        # Initialize channel dictionaries for this peer
        self._video_outgoing_channels[other_id] = {}
        self._video_incoming_channels[other_id] = {}

        # Create a data channel for chat
        chat_channel = pc.createDataChannel(CHANNEL_TYPE_CHAT)
        self._chat_outgoing_channels[other_id] = chat_channel

        @chat_channel.on("open")
        def on_chat_open() -> None:
            print(f"[{self.peer_id}] Outgoing Chat DataChannel to {other_id} opened")

        @chat_channel.on("message")
        def on_chat_message(message: str) -> None:
            self.on_chat_message_received(other_id, message)

        # Create data channels for each video stream this node handles
        for stream_index in range(self.num_video_streams):
            channel_label = CHANNEL_TYPE_VIDEO_FORMAT.format(stream_index)
            video_channel = pc.createDataChannel(channel_label)
            self._video_outgoing_channels[other_id][stream_index] = video_channel

            # Message handler for outgoing video channels (usually not used for receiving)
            @video_channel.on("message")
            def on_video_message(message: bytes) -> None:
                pass

        # Handle INCOMING data channels created by the remote peer
        @pc.on("datachannel")
        def on_datachannel(incoming_channel: RTCDataChannel) -> None:
            channel_label = incoming_channel.label
            print(
                f"[{self.peer_id}] Incoming DataChannel '{channel_label}' from {other_id} received"
            )

            if channel_label == CHANNEL_TYPE_CHAT:
                self._chat_incoming_channels[other_id] = incoming_channel

                @incoming_channel.on("open")
                def _chat_open() -> None:
                    print(
                        f"[{self.peer_id}] Incoming Chat DataChannel from {other_id} opened"
                    )

                @incoming_channel.on("message")
                def _chat_msg(m: str) -> None:
                    self.on_chat_message_received(other_id, m)

            elif channel_label.startswith("video-stream-"):
                try:
                    stream_index = int(channel_label.split("-")[-1])
                    if (
                        stream_index < self.num_video_streams
                    ):  # Only handle if we expect this stream index
                        self._video_incoming_channels[other_id][
                            stream_index
                        ] = incoming_channel

                        @incoming_channel.on("open")
                        def _video_open() -> None:
                            print(
                                f"[{self.peer_id}] Incoming Video DataChannel '{channel_label}' from {other_id} opened"
                            )

                        # Use partial to pass stream_index to the message handler
                        on_video_msg_partial = functools.partial(
                            self.on_video_stream_data_received, other_id, stream_index
                        )
                        incoming_channel.on("message")(on_video_msg_partial)
                    else:
                        print(
                            f"[{self.peer_id}] Received video channel with unexpected stream index: {channel_label}"
                        )

                except (ValueError, IndexError):
                    print(
                        f"[{self.peer_id}] Received video channel with invalid label format: {channel_label}"
                    )

            else:
                print(
                    f"[{self.peer_id}] Received unknown data channel type: {channel_label}"
                )

        @pc.on("icecandidate")
        async def on_icecandidate(event: Any) -> None:
            if event.candidate and self._ws and not self._ws.closed:
                candidate_data = {
                    "candidate": event.candidate.sdp,
                    "sdpMid": event.candidate.sdp_mid,
                    "sdpMLineIndex": event.candidate.sdp_m_line_index,
                }
                try:
                    await self._ws.send_json(
                        {
                            "from": self.peer_id,
                            "to": other_id,
                            "type": "candidate",
                            "data": candidate_data,
                        }
                    )
                except Exception:
                    pass

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
                    if self.peer_id < other_id and self._ws and not self._ws.closed:
                        print(
                            f"[{self.peer_id}] Re-sending offer to {other_id} after ICE restart."
                        )
                        offer = await pc.createOffer()
                        await pc.setLocalDescription(offer)
                        await self._ws.send_json(
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
            elif pc.connectionState == "closed":
                print(f"[{self.peer_id}] PeerConnection with {other_id} closed.")
                # Clean up local references when PC closes
                if other_id in self._pcs:
                    del self._pcs[other_id]
                if other_id in self._chat_outgoing_channels:
                    del self._chat_outgoing_channels[other_id]
                if other_id in self._chat_incoming_channels:
                    del self._chat_incoming_channels[other_id]
                if other_id in self._video_outgoing_channels:
                    del self._video_outgoing_channels[other_id]
                if other_id in self._video_incoming_channels:
                    del self._video_incoming_channels[other_id]

        return pc

    async def send_chat_message(self, message: str) -> None:
        """Sends a text message to all open chat data channels."""
        if not message.strip():
            return

        sent_count = 0
        for target_id, channel in list(self._chat_outgoing_channels.items()):
            if channel.readyState == "open":
                try:
                    channel.send(message)
                    sent_count += 1
                except Exception:
                    pass  # Suppress frequent errors

        if sent_count == 0 and self._chat_outgoing_channels:
            print(f"[{self.peer_id}] No open chat data channels to send message.")

    async def send_video_frame(self, stream_index: int, frame_data: bytes) -> None:
        """Sends a video frame's raw bytes for a specific stream to all open video channels."""
        if stream_index < 0 or stream_index >= self.num_video_streams:
            # This should ideally not happen if external logic is correct
            # print(f"[{self.peer_id}] Warning: Attempted to send video frame for out-of-bounds stream index {stream_index}.")
            return

        # Send this frame data over the specific outgoing channel for this stream
        # to all connected peers that have that channel open.
        sent_count = 0
        for target_id, channels_by_stream in list(
            self._video_outgoing_channels.items()
        ):
            if stream_index in channels_by_stream:
                channel = channels_by_stream[stream_index]
                if channel.readyState == "open":
                    try:
                        # Data channels have a send buffer. If it's full, send() might block
                        # or raise an error depending on implementation/settings.
                        channel.send(frame_data)
                        sent_count += 1
                    except Exception:
                        pass  # Suppress frequent errors

        # print(f"[{self.peer_id}] Sent video frame for stream {stream_index} to {sent_count} peers.") # Too chatty

    # --- Default Handlers for Incoming Messages ---
    def _default_chat_handler(self, peer_id: int, message: str) -> None:
        """Default handler for received chat messages."""
        print(f"[{self.peer_id}] Received chat from {peer_id}: {message}")

    def _default_video_handler(
        self, peer_id: int, stream_index: int, frame_data: bytes
    ) -> None:
        """
        Default handler for received video stream data.
        Does nothing by default. External logic should assign a handler.
        """
        pass

    async def connect_to(self, peer_ids: List[int]) -> None:
        """Connect this node to specified peer_ids via signaling and WebRTC."""
        async with aiohttp.ClientSession() as session:
            self._ws = None
            try:
                self._ws = await session.ws_connect(self.signaling_url)
                print(f"[{self.peer_id}] Registered with signaling server")

                await self._ws.send_json(
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
                    pc = await self._create_peer_connection(pid)

                    if self.peer_id < pid:
                        offers_sent.add(pid)
                        offer = await pc.createOffer()
                        await pc.setLocalDescription(offer)
                        if self._ws and not self._ws.closed:
                            try:
                                await self._ws.send_json(
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
                                pass

                # Node's main loop now just processes signaling messages
                while self._running and self._ws and not self._ws.closed:
                    try:
                        msg = await self._ws.receive(timeout=1.0)

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
                                pc = await self._create_peer_connection(frm)
                            elif not pc and typ != "disconnect":
                                continue
                            elif typ == "disconnect":
                                print(f"[{self.peer_id}] Peer {frm} disconnected.")
                                if frm in self._pcs:
                                    await self._pcs[frm].close()
                                continue

                            # Handle different signaling message types
                            if typ == "offer":
                                if self.peer_id > frm:
                                    print(f"[{self.peer_id}] ← offer ← [{frm}]")
                                    offer = RTCSessionDescription(
                                        sdp=data["sdp"], type=data["type"]
                                    )
                                    await pc.setRemoteDescription(offer)
                                    answer = await pc.createAnswer()
                                    await pc.setLocalDescription(
                                        answer
                                    )  # Corrected from setAnswer
                                    if self._ws and not self._ws.closed:
                                        try:
                                            await self._ws.send_json(
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
                                            pass
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
                                        if self._ws and not self._ws.closed:
                                            await self._ws.send_json(
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
                                        pass

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
                                        if self._ws and not self._ws.closed:
                                            await self._ws.send_json(
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
                                        pass

                            elif typ == "disconnect":
                                print(f"[{self.peer_id}] Peer {frm} disconnected.")
                                # Cleanup handled in connectionstatechange

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
                            if self._ws and not self._ws.closed:
                                await self._ws.pong()

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

                # Close all peer connections
                for pid, pc in list(self._pcs.items()):
                    if not pc.closed:
                        print(f"[{self.peer_id}] Closing connection with {pid}...")
                        await pc.close()
                self._pcs.clear()
                self._chat_outgoing_channels.clear()
                self._chat_incoming_channels.clear()
                self._video_outgoing_channels.clear()
                self._video_incoming_channels.clear()

                # Ensure the WebSocket connection is closed
                if self._ws and not self._ws.closed:
                    await self._ws.close()
                    print(f"[{self.peer_id}] Signaling WebSocket closed.")
