import os
import time
import json
import redis
import base64
import traceback

from PIL import Image
import numpy as np

# ---------------------------------------------------------
# REDIS CONNECTION
# ---------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis_server")
r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

print("🚀 Model Worker Started (Auto GPU/CPU Mode)")

# ---------------------------------------------------------
# AUTO DETECT MODE (GPU/TRITON or CPU)
# ---------------------------------------------------------

USE_GPU = False
USE_TRITON = False

try:
    import torch

    if torch.cuda.is_available():
        USE_GPU = True
        print("💠 GPU detected! CUDA available.")
    else:
        print("⬛ No GPU found (CUDA unavailable).")

except Exception:
    print("❌ PyTorch missing, cannot detect GPU.")

# Try Triton client
try:
    import tritonclient.http as httpclient
    USE_TRITON = True
    print("📡 Triton client available.")
except Exception:
    print("⚠️ Triton client not available; fallback to CPU YOLO.")

# ---------------------------------------------------------
# LOAD MODEL ACCORDING TO MODE
# ---------------------------------------------------------

if USE_TRITON:
    try:
        triton_client = httpclient.InferenceServerClient(url="triton_server:8000")
        print("🔗 Connected to Triton server.")
    except Exception:
        print("❌ Triton server not reachable. Falling back to CPU.")
        USE_TRITON = False

if not USE_TRITON:
    # Load Ultralytics YOLO model
    from ultralytics import YOLO
    try:
        print("📦 Loading YOLO model on CPU...")
        model = YOLO("yolov8n.pt")   # put your model here
        print("✨ CPU YOLO loaded.")
    except Exception as e:
        print("❌ Error loading CPU model:", e)
        raise

# ---------------------------------------------------------
# HELPER: Run Inference
# ---------------------------------------------------------
def run_inference_cpu(image_paths):
    """Run inference using YOLO CPU."""
    results = []

    batch = [np.array(Image.open(p)) for p in image_paths]
    output = model(batch)

    for i, res in enumerate(output):
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


def run_inference_triton(image_paths):
    """Run inference using Triton."""
    results = []

    # TODO: Depends on your Triton model input format.
    # Placeholder implementation:
    for p in image_paths:
        results.append({
            "image_path": p,
            "detections": [{"cls": 0, "conf": 0.99, "xyxy": [10, 10, 100, 100]}]
        })

    return results

# ---------------------------------------------------------
# MAIN LOOP – Read Batches From Redis
# ---------------------------------------------------------

def process_batches():
    last_id = "0-0"

    while True:
        try:
            messages = r.xread({"model_queue:yolo_model": last_id}, block=5000, count=1)
            if not messages:
                continue

            _, entries = messages[0]
            msg_id, fields = entries[0]
            last_id = msg_id

            batch_json = fields[b"batch"].decode()
            batch = json.loads(batch_json)

            image_paths = [item["frame_path"] for item in batch]

            print(f"📥 Received batch with {len(image_paths)} images")

            # -------------------------
            # Run inference
            # -------------------------
            if USE_TRITON:
                results = run_inference_triton(image_paths)
            else:
                results = run_inference_cpu(image_paths)

            # --------------------------
            # Push results to next queue
            # --------------------------
            r.xadd("analysis_queue", {"results": json.dumps(results)})

            print(f"📤 Sent inference results ({len(results)} images)")

            # delete processed batch
            r.xdel("model_queue:yolo_model", msg_id)

        except Exception as e:
            print("❌ ERROR in worker:", e)
            traceback.print_exc()
            time.sleep(1)


process_batches()
