from rtc_client import Node
from typing import Dict, List, Any, Set, Optional, Callable, Tuple
import cv2  # Import OpenCV
import numpy as np  # Import numpy for image data
import pyrealsense2 as rs
import concurrent.futures  # Import for ThreadPoolExecutor
import threading  # Import for managing display loop in a separate thread
import asyncio
import argparse


class VideoDisplayManager:
    """Manages decoding and displaying video frames from multiple peers and streams."""

    def __init__(self, my_peer_id: int):
        self.my_peer_id = my_peer_id
        # Store frames keyed by (peer_id, stream_index)
        self._received_video_frames: Dict[Tuple[int, int], np.ndarray] = {}
        self._display_thread: Optional[threading.Thread] = None
        self._running = True
        self._lock = threading.Lock()  # Lock for accessing _received_video_frames
        self._windows_created: Set[Tuple[int, int]] = (
            set()
        )  # Keep track of created windows per (peer, stream)

    def handle_video_frame(
        self, peer_id: int, stream_index: int, frame_data: bytes
    ) -> None:
        """Decodes a video frame and prepares it for display."""
        try:
            np_arr = np.frombuffer(frame_data, np.uint8)
            frame = cv2.imdecode(
                np_arr, cv2.IMREAD_COLOR
            )  # Assuming BGR or Grayscale for display
            if frame is not None:
                with self._lock:
                    self._received_video_frames[(peer_id, stream_index)] = frame
                self._start_display_thread_if_needed()
            # else: Error decoding is not printed here to keep this handler clean,
            #       can be added for debugging if needed.
        except Exception as e:
            print(
                f"[{self.my_peer_id}] Error processing video frame for stream {stream_index} from {peer_id}: {e}"
            )

    def _start_display_thread_if_needed(self):
        """Starts the display thread if it's not already running."""
        if self._display_thread is None or not self._display_thread.is_alive():
            self._running = True
            self._display_thread = threading.Thread(
                target=self._display_loop, daemon=True
            )
            self._display_thread.start()
            print(f"[{self.my_peer_id}] Video display thread started.")

    def _display_loop(self):
        """Blocking loop for displaying received video frames using OpenCV."""
        print(f"[{self.my_peer_id}] Starting blocking video display loop.")

        while self._running:
            # Get a list of (peer_id, stream_index) tuples that have frames
            frames_to_display = list(self._received_video_frames.keys())

            if not frames_to_display and not self._windows_created:
                cv2.waitKey(1)
                import time

                time.sleep(0.01)
                continue

            peers_streams_with_closed_windows = []
            for peer_id, stream_index in frames_to_display:
                try:
                    with self._lock:
                        frame = self._received_video_frames.get((peer_id, stream_index))

                    if frame is not None:
                        peer_stream_window_name = f"Peer {self.my_peer_id} - Video from {peer_id} (Stream {stream_index})"
                        if (peer_id, stream_index) not in self._windows_created:
                            cv2.namedWindow(peer_stream_window_name, cv2.WINDOW_NORMAL)
                            self._windows_created.add((peer_id, stream_index))

                        cv2.imshow(peer_stream_window_name, frame)

                        if (
                            cv2.getWindowProperty(
                                peer_stream_window_name, cv2.WND_PROP_VISIBLE
                            )
                            < 1
                        ):
                            print(
                                f"[{self.my_peer_id}] Window for peer {peer_id} stream {stream_index} closed manually."
                            )
                            peers_streams_with_closed_windows.append(
                                (peer_id, stream_index)
                            )

                except Exception as e:
                    print(
                        f"[{self.my_peer_id}] Error displaying video frame for stream {stream_index} from {peer_id}: {e}"
                    )
                    peers_streams_with_closed_windows.append((peer_id, stream_index))

            # Clean up resources for peers/streams whose windows were closed
            for peer_id, stream_index in peers_streams_with_closed_windows:
                with self._lock:
                    if (peer_id, stream_index) in self._received_video_frames:
                        del self._received_video_frames[(peer_id, stream_index)]
                # Note: Closing the PC is handled in the Node's connectionstatechange
                # when the peer disconnects entirely, not per stream window close.
                if (peer_id, stream_index) in self._windows_created:
                    cv2.destroyWindow(
                        f"Peer {self.my_peer_id} - Video from {peer_id} (Stream {stream_index})"
                    )
                    self._windows_created.remove((peer_id, stream_index))

            # Process OpenCV events and check for quit key
            key = cv2.waitKey(1) & 0xFF
            if key == ord("q"):
                print(
                    f"[{self.my_peer_id}] 'q' pressed in OpenCV window, signaling shutdown."
                )
                self._running = False  # Signal shutdown

            # If there are no more windows but _running is still True and we had frames, assume manual close
            if (
                self._running
                and not self._windows_created
                and self._received_video_frames
            ):
                print(
                    f"[{self.my_peer_id}] No video windows visible, assuming manual close and signaling shutdown."
                )
                self._running = False

            import time

            time.sleep(0.01)

        print(f"[{self.my_peer_id}] Blocking video display loop stopping.")
        cv2.destroyAllWindows()  # Ensure all windows are closed when the loop finishes
        print(f"[{self.my_peer_id}] OpenCV windows destroyed.")

    def stop(self):
        """Signals the display thread to stop and cleans up."""
        self._running = False
        if self._display_thread and self._display_thread.is_alive():
            cv2.waitKey(1)  # Attempt to unblock waitKey
            try:
                self._display_thread.join(timeout=1.0)
            except threading.TimeoutError:
                print(
                    f"[{self.my_peer_id}] Warning: Display thread did not join within timeout."
                )

        cv2.destroyAllWindows()  # Ensure windows are closed


# --- External Functions for Handling Input and Video Capture (using pyrealsense2) ---


async def handle_user_input(node):
    """Reads user input and sends chat messages via the node."""
    print(f"[{node.peer_id}] Enter messages to broadcast. Type 'quit' to exit.")
    loop = asyncio.get_event_loop()
    while node._running:  # Use the node's running flag to control loop
        try:
            # Run the blocking input() call in a separate thread.
            # This will block the executor thread until input is received.
            msg = await loop.run_in_executor(
                None, input, f"[{node.peer_id}] Chat Broadcast: "
            )

            if msg.strip().lower() == "quit":
                # Signal the node to stop, which will cascade shutdown
                node._running = False
                break

            if not msg.strip():
                continue

            await node.send_chat_message(msg)

        except asyncio.CancelledError:
            print(f"[{node.peer_id}] Input task cancelled.")
            break  # Exit loop on cancellation
        except Exception as e:
            print(f"[{node.peer_id}] Error getting user input or sending chat: {e}")
            await asyncio.sleep(0.1)
            # Loop will continue unless node._running is False


async def run_t265_and_send(node, executor: concurrent.futures.ThreadPoolExecutor):
    """
    Captures frames from T265 left and right fisheye cameras and sends them as JPEGs using `node.send_video_frame`.
    Pose data is ignored.
    Stream mapping:
        - Left fisheye = stream index 0
        - Right fisheye = stream index 1
    Runs blocking operations in the provided executor.
    """
    print(f"[{node.peer_id}] Starting T265 pipeline.")
    pipeline = rs.pipeline()
    config = rs.config()

    # Configure the fisheye streams
    # Note: T265 fisheye streams are typically 640x480 @ 30 FPS, Y8 format
    # Ensure these settings are supported by your device if you encounter issues.
    try:
        config.enable_stream(
            rs.stream.fisheye, 1
        )  # Left camera, stream index 1 in Realsense
        config.enable_stream(
            rs.stream.fisheye, 2
        )  # Right camera, stream index 2 in Realsense
        # config.enable_stream(rs.stream.pose) # Enable pose if needed, but won't be sent as video
        print(f"[{node.peer_id}] T265 streams configured (Fisheye 1 & 2).")
    except Exception as e:
        print(f"[{node.peer_id}] Error configuring T265 streams: {e}")
        print(
            f"[{node.peer_id}] Ensure your T265 is connected and supports 640x480 @ 30 FPS Y8 on streams 1 and 2."
        )
        return  # Exit task if configuration fails

    try:
        # Start the pipeline (can be blocking)
        await asyncio.get_event_loop().run_in_executor(executor, pipeline.start, config)
        print(f"[{node.peer_id}] T265 pipeline started.")

        # Warm-up frames (can be blocking)
        print(f"[{node.peer_id}] Waiting for T265 warm-up frames...")
        for _ in range(30):
            await asyncio.get_event_loop().run_in_executor(
                executor, pipeline.wait_for_frames
            )
        print(f"[{node.peer_id}] T265 warm-up complete.")

        while node._running:  # Use the node's running flag to control loop
            try:
                # Wait for a coherent pair of frames (blocking)
                frames = await asyncio.get_event_loop().run_in_executor(
                    executor, pipeline.wait_for_frames
                )

                left_frame = frames.get_fisheye_frame(1)
                right_frame = frames.get_fisheye_frame(2)
                # pose_frame = frames.get_pose_frame() # Get pose if enabled

                if not left_frame or not right_frame:
                    continue  # Skip if frames are not available

                # Convert frame data to numpy arrays
                left_image = np.asanyarray(left_frame.get_data())
                right_image = np.asanyarray(right_frame.get_data())

                # Encode images to JPEG bytes (can be CPU intensive, but not blocking)
                encode_param = [
                    int(cv2.IMWRITE_JPEG_QUALITY),
                    40,
                ]  # Adjust quality (0-100) to balance size/quality/latency
                _, left_encoded_buffer = cv2.imencode(".jpg", left_image, encode_param)
                _, right_encoded_buffer = cv2.imencode(
                    ".jpg", right_image, encode_param
                )

                # Send the encoded frames via the node (async operation)
                # Map Realsense stream index 1 to Node stream index 0
                await node.send_video_frame(0, left_encoded_buffer.tobytes())
                # Map Realsense stream index 2 to Node stream index 1
                await node.send_video_frame(1, right_encoded_buffer.tobytes())

                # No explicit sleep needed, wait_for_frames controls rate

            except asyncio.CancelledError:
                print(f"[{node.peer_id}] T265 Streaming loop cancelled.")
                break  # Exit loop on cancellation
            except Exception as e:
                print(f"[{node.peer_id}] Error in T265 streaming loop: {e}")
                await asyncio.sleep(0.1)  # Sleep briefly on error
                if not node._running:
                    break

    except Exception as e:
        print(f"[{node.peer_id}] Error starting or running T265 pipeline: {e}")

    finally:
        print(f"[{node.peer_id}] Stopping T265 pipeline.")
        # Stop the pipeline (can be blocking)
        try:
            await asyncio.get_event_loop().run_in_executor(executor, pipeline.stop)
            print(f"[{node.peer_id}] T265 Pipeline stopped.")
        except Exception as e:
            print(f"[{node.peer_id}] Error stopping T265 pipeline: {e}")


async def main():
    print(
        "\n--- Running Sample Usage with External Control (T265 Fisheye Streaming) ---"
    )

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="T265 Fisheye Streaming Node")
    parser.add_argument(
        "--node_id", type=int, required=True, help="Unique peer ID for this node"
    )
    parser.add_argument(
        "--listen_to",
        type=int,
        nargs="*",
        default=[],
        help="Target peer IDs to listen to (space-separated)",
    )
    parser.add_argument(
        "--stream",
        choices=["yes", "no"],
        default="no",
        help="Stream video from this node (yes/no, default: no)",
    )
    args = parser.parse_args()

    peer_id = args.node_id
    listen_for = args.listen_to
    is_streaming = args.stream == "yes"

    # T265 always sends two video streams (left/right), no pose streaming via network
    # If streaming, we handle 2 outgoing streams. If not streaming but listening,
    # assume we might receive 2 streams. If neither, 0.
    num_video_streams_to_handle = 2 if is_streaming else (2 if listen_for else 0)

    # Instantiate the Node (assuming Node class is defined elsewhere or imported)
    # from rtc_client_node import Node # Uncomment if Node is in a separate file
    try:
        node = Node(peer_id, num_video_streams=num_video_streams_to_handle)
    except NameError:
        print(
            "Error: Node class not found. Please ensure the Node class definition is available (e.g., imported)."
        )
        return

    video_display_manager = VideoDisplayManager(peer_id)

    def custom_chat_handler(pid: int, msg: str) -> None:
        print(f"[ChatHandler {node.peer_id}] Chat from {pid}: {msg}")

    def custom_video_handler(pid: int, stream_index: int, frame_data: bytes) -> None:
        video_display_manager.handle_video_frame(pid, stream_index, frame_data)

    node.on_chat_message_received = custom_chat_handler
    node.on_video_stream_data_received = custom_video_handler

    # Create a ThreadPoolExecutor for blocking operations (like RealSense capture)
    # Adjust max_workers based on your needs (e.g., 1 for input, 1 for Realsense pipeline)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    # Launch user input and optional T265 streaming task
    input_task = asyncio.create_task(handle_user_input(node))
    realsense_pipeline_task = (
        asyncio.create_task(run_t265_and_send(node, executor)) if is_streaming else None
    )

    try:
        tasks_to_run = [node.connect_to(listen_for), input_task]
        if realsense_pipeline_task:
            tasks_to_run.append(realsense_pipeline_task)

        # Use return_exceptions=True to ensure all tasks are waited on even if one raises an exception
        await asyncio.gather(*tasks_to_run, return_exceptions=True)

    except KeyboardInterrupt:
        print("Sample shutting down from KeyboardInterrupt...")
    finally:
        print("Main loop finished. Starting cleanup...")
        # Signal the node to stop (this will also stop the input task loop check)
        node._running = False

        # Cancel the external tasks
        input_task.cancel()
        if realsense_pipeline_task:
            realsense_pipeline_task.cancel()

        # Wait for external tasks to finish gracefully after cancellation
        try:
            # Gather tasks, handling the case where realsense_pipeline_task is None
            await asyncio.gather(
                input_task,
                *([realsense_pipeline_task] if realsense_pipeline_task else []),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass  # Expected during cancellation
        except Exception as e:
            print(f"Error waiting for tasks to finish: {e}")

        # Signal the video display manager to stop its thread
        video_display_manager.stop()

        # Shutdown the executor
        print("Shutting down executor.")
        executor.shutdown(wait=True)
        print("Executor shut down.")

        # The node's cleanup is handled within its connect_to method's finally block
        print("Cleanup complete.")


if __name__ == "__main__":
    asyncio.run(main())
