import pyrealsense2 as rs
import numpy as np
import cv2

# Initialize the pipeline
pipeline = rs.pipeline()
config = rs.config()

# Enable fisheye and pose streams
config.enable_stream(rs.stream.any, 1, 848, 800, rs.format.any, 30)  # Left camera
config.enable_stream(rs.stream.any, 2, 848, 800, rs.format.any, 30)  # Right camera
config.enable_stream(rs.stream.pose)  # Pose data

# Start streaming
pipeline.start(config)

try:
    while True:
        # Wait for a coherent pair of frames: fisheye and pose
        frames = pipeline.wait_for_frames()
        left = frames.get_fisheye_frame(1)
        right = frames.get_fisheye_frame(2)
        pose = frames.get_pose_frame()

        if not left or not right or not pose:
            continue

        # Convert images to numpy arrays
        left_image = np.asanyarray(left.get_data())
        right_image = np.asanyarray(right.get_data())

        # Retrieve pose data
        data = pose.get_pose_data()
        position = data.translation
        velocity = data.velocity
        acceleration = data.acceleration

        # Prepare pose information text
        pose_info = [
            f"Position: x={position.x:.2f}, y={position.y:.2f}, z={position.z:.2f}",
            f"Velocity: x={velocity.x:.2f}, y={velocity.y:.2f}, z={velocity.z:.2f}",
            f"Acceleration: x={acceleration.x:.2f}, y={acceleration.y:.2f}, z={acceleration.z:.2f}",
        ]

        # Stack left and right images horizontally
        combined_image = np.hstack((left_image, right_image))

        # Convert to BGR for OpenCV
        combined_image = cv2.cvtColor(combined_image, cv2.COLOR_GRAY2BGR)

        # Overlay pose information on the image
        for idx, text in enumerate(pose_info):
            cv2.putText(
                combined_image,
                text,
                (10, 30 + idx * 30),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (0, 255, 0),
                2,
            )

        # Display the image
        cv2.imshow("T265 Fisheye and Pose", combined_image)

        # Exit on 'q' key press
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break

finally:
    # Stop streaming
    pipeline.stop()
    cv2.destroyAllWindows()
