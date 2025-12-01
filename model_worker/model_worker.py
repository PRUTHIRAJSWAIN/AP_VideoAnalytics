import os
import time
import json
import redis
import base64
import traceback

from ultralytics import YOLO
import psycopg2
from psycopg2.extras import Json

# ---------------------------------------------------------
# REDIS CONNECTION
# ---------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis_server")
r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

print("🚀 Model Worker Started (YOLO Only)")

# ---------------------------------------------------------
# GPU CHECK
# ---------------------------------------------------------
USE_GPU = False
try:
    import torch
    if torch.cuda.is_available():
        USE_GPU = True
        print("💠 GPU detected! Using CUDA.")
    else:
        print("⬛ CPU mode")
except Exception:
    print("❌ PyTorch missing — CPU mode only.")


# ---------------------------------------------------------
# LOAD YOLO MODEL
# ---------------------------------------------------------
device = 0 if USE_GPU else "cpu"
print(f"📦 Loading YOLO model on {device}...")
model = YOLO("yolov8n.pt")
model.to(device)
print("✨ YOLO loaded.")


# ---------------------------------------------------------
# POSTGRES CONNECTION
# ---------------------------------------------------------
pg_conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=os.getenv("PG_PORT", "5432"),
    dbname=os.getenv("PG_DB", "yolo_db"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "postgres")
)
pg_conn.autocommit = True
pg_cursor = pg_conn.cursor()
print("🐘 Connected to PostgreSQL")


# ---------------------------------------------------------
# YOLO Inference
# ---------------------------------------------------------
def run_inference_yolo(image_paths):

    # send list of paths — Ultralytics handles batching internally
    results_raw = model(image_paths)

    results = []
    for i, res in enumerate(results_raw):
        detections = []
        for box in res.boxes:
            detections.append({
                "cls": int(box.cls[0]),
                "conf": float(box.conf[0]),
                "xyxy": box.xyxy[0].tolist()
            })

        results.append({
            "image_path": image_paths[i],
            "detections": detections
        })

    return results


# ---------------------------------------------------------
# WORKER LOOP
# ---------------------------------------------------------
def process_batches():
    last_id = "0-0"

    while True:
        try:
            # Read batch from Redis stream
            messages = r.xread({"model_queue:yolo_model": last_id}, block=5000, count=1)
            if not messages:
                continue

            _, entries = messages[0]
            msg_id, fields = entries[0]
            last_id = msg_id

            batch_json = fields[b"batch"].decode()
            batch = json.loads(batch_json)

            image_paths = [item["frame_path"] for item in batch]

            print(f"📥 Batch received: {len(image_paths)} images")

            # Run YOLO
            results = run_inference_yolo(image_paths)

            # Save to PostgreSQL
            for item in results:
                pg_cursor.execute("""
                    INSERT INTO yolo_results (image_path, detections)
                    VALUES (%s, %s)
                """, (item["image_path"], Json(item["detections"])))

            print(f"💾 Saved {len(results)} results")

            # Delete message from Redis
            r.xdel("model_queue:yolo_model", msg_id)

        except Exception as e:
            print("❌ Worker error:", e)
            traceback.print_exc()
            time.sleep(1)


process_batches()
