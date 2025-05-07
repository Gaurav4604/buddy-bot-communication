import asyncio
import json
import logging
from typing import Dict, Any, Optional, List, Set

import aiohttp
from aiohttp import web

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Server:
    def __init__(self) -> None:
        self.app: web.Application = web.Application()
        # peers_info maps peer_id -> { data: Any, listen_for: List[int], ws: WebSocketResponse }
        self.app["peers_info"] = {}
        # Track active websocket connections for cleanup
        self.app["active_connections"] = set()
        self.app.router.add_get("/ws", self.websocket_handler)
        self.app.on_shutdown.append(self.on_shutdown)

    async def on_shutdown(self, app: web.Application) -> None:
        """Close all websocket connections on server shutdown"""
        for ws in app["active_connections"]:
            await ws.close(code=1001, message="Server shutdown")

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        """Handle WebSocket connections and messages"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Track this connection
        self.app["active_connections"].add(ws)
        peer_id = None

        try:
            logger.info("New WebSocket connection established")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        payload: Dict[str, Any] = json.loads(msg.data)
                        logger.info(f"Received: {payload}")

                        # Check if this is a signaling message (offer/answer/candidate)
                        if "to" in payload and "type" in payload:
                            await self._handle_signaling_message(payload)
                            continue

                        # Otherwise, it's a registration/presence message
                        current_peer_id: Optional[int] = payload.get("current_peer_id")
                        if current_peer_id is None:
                            continue

                        peer_id = current_peer_id  # Store for cleanup
                        listen_for: List[int] = payload.get("listen_for", [])
                        data: Any = payload.get("data")

                        await self._handle_registration(
                            ws, current_peer_id, listen_for, data
                        )

                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON: {msg.data}")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {ws.exception()}")

        finally:
            # Cleanup on disconnect
            self.app["active_connections"].discard(ws)
            if peer_id is not None:
                if peer_id in self.app["peers_info"]:
                    logger.info(f"Peer {peer_id} disconnected")
                    del self.app["peers_info"][peer_id]

                    # Notify other peers that this one has disconnected
                    for other_id, info in self.app["peers_info"].items():
                        try:
                            await info["ws"].send_json(
                                {
                                    "from": peer_id,
                                    "type": "disconnect",
                                    "data": "disconnected",
                                }
                            )
                        except Exception:
                            pass

        return ws

    async def _handle_signaling_message(self, payload: Dict[str, Any]) -> None:
        """Handle and relay WebRTC signaling messages (offer/answer/candidate)"""
        to_id: int = payload.get("to")
        from_id: int = payload.get("from")
        msg_type: str = payload.get("type")

        if to_id is None or from_id is None:
            logger.warning("Missing to/from fields in signaling message")
            return

        target_info = self.app["peers_info"].get(to_id)
        if target_info and "ws" in target_info:
            try:
                await target_info["ws"].send_json(payload)
                logger.info(f"Relayed {payload['type']} from {from_id} to {to_id}")

                # For offers that failed before, store them so we can retry when the peer connects
                if msg_type == "offer" and not target_info.get("ws"):
                    from_info = self.app["peers_info"].get(from_id)
                    if from_info:
                        if "pending_offers" not in from_info:
                            from_info["pending_offers"] = {}
                        # Store the offer to retry later
                        from_info["pending_offers"][to_id] = payload
                        logger.info(
                            f"Stored pending offer from {from_id} to {to_id} for later retry"
                        )

            except Exception as e:
                logger.error(f"Error relaying message: {str(e)}")
        else:
            logger.warning(f"Target peer {to_id} not found or not connected")

            # If this is an offer and the target peer isn't connected, store it for later
            if msg_type == "offer":
                from_info = self.app["peers_info"].get(from_id)
                if from_info:
                    if "pending_offers" not in from_info:
                        from_info["pending_offers"] = {}
                    # Store the offer to retry later
                    from_info["pending_offers"][to_id] = payload
                    logger.info(
                        f"Stored pending offer from {from_id} to {to_id} for later retry"
                    )

    async def _handle_registration(
        self, ws: web.WebSocketResponse, peer_id: int, listen_for: List[int], data: Any
    ) -> None:
        """Handle peer registration and connection setup"""
        # Store or update this peer's info
        self.app["peers_info"][peer_id] = {
            "data": data,
            "listen_for": set(listen_for),  # Use set for more efficient lookups
            "ws": ws,
        }

        logger.info(f"Peer {peer_id} registered, listening for {listen_for}")

        # 1. Notify all existing peers about this new peer
        for other_id, info in list(self.app["peers_info"].items()):
            if other_id == peer_id:
                continue

            other_ws = info.get("ws")
            if not other_ws or other_ws.closed:  # Check if ws is still valid
                logger.warning(
                    f"Skipping notification for peer {other_id} as WebSocket is closed."
                )
                continue

            # Notify other peers about this new one if they're interested (i.e., they listen for this new peer)
            if peer_id in info["listen_for"]:
                try:
                    await other_ws.send_json(
                        {
                            "from": peer_id,
                            "type": "peer_info",
                            "data": data,
                        }
                    )
                    logger.info(f"Sent peer {peer_id} info to {other_id}")
                    # Also notify peers listening for this one that they have an opportunity to connect
                    await other_ws.send_json(
                        {
                            "from": peer_id,
                            "type": "connection_opportunity",
                            "data": data,
                        }
                    )
                    logger.info(
                        f"Notified peer {other_id} of connection opportunity with {peer_id}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error sending peer info/opportunity to {other_id}: {str(e)}"
                    )

        # 2. Notify this new peer about all existing peers it's interested in
        for other_id in listen_for:
            info = self.app["peers_info"].get(other_id)
            # Only send info if the other peer exists and is connected
            if info and "ws" in info and info["ws"] and not info["ws"].closed:
                try:
                    await ws.send_json(
                        {
                            "from": other_id,
                            "type": "peer_info",
                            "data": info.get("data"),
                        }
                    )
                    logger.info(f"Sent peer {other_id} info to {peer_id}")
                except Exception as e:
                    logger.error(f"Error sending peer info to {peer_id}: {str(e)}")

        # 3. Check for and deliver any pending offers for this newly connected peer
        # Pending offers are stored in the sender's info. We need to iterate through all peers
        # and check if they have pending offers *for* peer_id.
        pending_offers_delivered = []
        for sender_id, sender_info in list(self.app["peers_info"].items()):
            if (
                sender_info.get("pending_offers")
                and peer_id in sender_info["pending_offers"]
            ):
                offer_message = sender_info["pending_offers"][peer_id]
                try:
                    await ws.send_json(offer_message)
                    logger.info(
                        f"Delivered pending offer from {sender_id} to newly connected {peer_id}"
                    )
                    # Mark for removal after loop
                    pending_offers_delivered.append((sender_id, peer_id))
                except Exception as e:
                    logger.error(
                        f"Error delivering pending offer from {sender_id} to {peer_id}: {str(e)}"
                    )

        # Remove delivered pending offers
        for sender_id, target_id in pending_offers_delivered:
            if (
                sender_id in self.app["peers_info"]
                and self.app["peers_info"][sender_id].get("pending_offers")
                and target_id in self.app["peers_info"][sender_id]["pending_offers"]
            ):
                del self.app["peers_info"][sender_id]["pending_offers"][target_id]
                if not self.app["peers_info"][sender_id]["pending_offers"]:
                    del self.app["peers_info"][sender_id][
                        "pending_offers"
                    ]  # Clean up if empty
                logger.info(
                    f"Removed delivered pending offer from {sender_id} to {target_id}"
                )

    def start_app(self, host="0.0.0.0", port=8080) -> None:
        """Start the web server"""
        logger.info(f"Starting server on {host}:{port}")
        web.run_app(self.app, host=host, port=port, access_log=logger)


if __name__ == "__main__":
    server = Server()
    server.start_app()
