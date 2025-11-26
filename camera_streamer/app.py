import cv2
import redis
import base64
import time
import os
import threading
from datetime import datetime

# Redis connection
redis_host = os.getenv("REDIS_HOST", "redis_server")
r = redis.StrictRedis(host=redis_host, port=6379, db=0)

# List of cameras with metadata
CAMERAS = [
    {
        "url": "videos/sample1.mp4",
        "interval": 0.5,
        "plant_id": "plantA",
        "site_id": "site1",
        "camera_code": "CAM01"
    },
    {
        "url": "videos/sample2.mkv",
        "interval": 0.5,
        "plant_id": "plantA",
        "site_id": "site2",
        "camera_code": "CAM02"
    },
    # Add more cameras here
]

def encode_frame(frame):
    """Convert frame to base64 bytes for Redis."""
    _, buffer = cv2.imencode(".jpg", frame)
    return base64.b64encode(buffer)

def camera_worker(cam_cfg):
    """Thread to read camera frames and push to Redis."""
    cam_id = cam_cfg["camera_code"]  # for logging
    print(f"🎥 Camera thread started: {cam_id}")

    cap = cv2.VideoCapture(cam_cfg["url"])
    if not cap.isOpened():
        print(f"❌ Cannot open {cam_id}")
        return

    while True:
        ok, frame = cap.read()
        if not ok:
            print(f"⚠️ {cam_id}: stream ended or cannot read... restarting...")
            cap.release()
            time.sleep(2)
            cap = cv2.VideoCapture(cam_cfg["url"])
            continue

        encoded = encode_frame(frame)

        # Add current timestamp in ISO format
        timestamp = datetime.utcnow().isoformat()  # UTC time

        # Push frame + metadata to Redis
        r.xadd("camera_stream", {
            "plant_id": cam_cfg["plant_id"],
            "site_id": cam_cfg["site_id"],
            "camera_code": cam_cfg["camera_code"],
            "timestamp": timestamp,
            "frame": encoded
        })

        print(f"📤 Sent frame: {cam_cfg['plant_id']}-{cam_cfg['site_id']}-{cam_cfg['camera_code']}")
        time.sleep(cam_cfg["interval"])

print("🚀 Camera streamer started")

# Start a thread for each camera
threads = []
for cam_cfg in CAMERAS:
    t = threading.Thread(
        target=camera_worker,
        args=(cam_cfg,),
        daemon=True
    )
    t.start()
    threads.append(t)

# Keep main thread alive
while True:
    time.sleep(1)
