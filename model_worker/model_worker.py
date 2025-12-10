import os
import time
import json
import redis
import base64
import traceback
import threading
from datetime import datetime
import cv2
from abc import ABC, abstractmethod

# Optional imports for specific backends
try:
    from ultralytics import YOLO
    import torch
except ImportError:
    YOLO = None
    torch = None
    print("YOLO not installed or failed to import")

import psycopg2
from psycopg2.extras import Json



# ---------------------------------------------------------
# GLOBAL CONFIG
# ---------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis_server")
PG_HOST = os.getenv("PG_HOST", "postgres_server")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "frames_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

SHARED_DIR = os.getenv("SHARED_DIR", "/shared")
MODELS_DIR = os.path.join(SHARED_DIR, "models")
MODELS_CONFIG_PATH = os.path.join(SHARED_DIR, "configs", "models_config.json")

# ---------------------------------------------------------
# DATABASE & REDIS HELPERS
# ---------------------------------------------------------
def get_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    conn.autocommit = True
    return conn

def get_redis_connection():
    return redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

# ---------------------------------------------------------
# ABSTRACT BACKEND
# ---------------------------------------------------------
class ModelBackend(ABC):
    def __init__(self, model_name, config):
        self.model_name = model_name
        self.config = config
        self.device = "cpu"
        
    @abstractmethod
    def load(self):
        """Load the model into memory. Raise Exception if file missing."""
        pass

    @abstractmethod
    def infer(self, image_paths):
        """Run inference on a list of image paths. Returns list of dicts."""
        pass

# ---------------------------------------------------------
# YOLO BACKEND
# ---------------------------------------------------------
class YOLOBackend(ModelBackend):
    def __init__(self, model_name, config):
        super().__init__(model_name, config)
        self.model = None
        # Always look inside /shared/models/
        weights_name = config.get("weights_file", f"{model_name}.pt")
        self.weights_file = os.path.join(MODELS_DIR, weights_name)

        # Check for GPU
        if torch and torch.cuda.is_available():
            self.device = 0
            print(f"[{self.model_name}] 💠 GPU detected! Using CUDA.")
        else:
            print(f"[{self.model_name}] ⬛ CPU mode")

    def load(self):
        if YOLO is None:
            # raise ImportError("Ultralytics not installed")
            print("Ultralytics not installed")

        # Check if weights file exists locally
        if not os.path.exists(self.weights_file):
            raise FileNotFoundError(
                f"⚠️ Weights file missing locally (no download allowed): {self.weights_file}"
            )
        print(f"[{self.model_name}] 📦 Loading YOLO model: {self.weights_file} on {self.device}...")
        self.model = YOLO(self.weights_file)
        self.model.to(self.device)
        print(f"[{self.model_name}] ✨ YOLO loaded.")

    def infer(self, image_paths):
        # Ultralytics handles batching internally
        results_raw = self.model(image_paths, verbose=False)
        
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
                "detections": detections,
                "original_result": res
            })
        return results

# ---------------------------------------------------------
# WORKER THREAD
# ---------------------------------------------------------
def worker_loop(backend):
    """Function to run in a separate thread for each model."""
    r = get_redis_connection()
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    stream_name = f"model_queue:{backend.model_name}"
    last_id = "0-0"
    
    print(f"[{backend.model_name}] 🚀 Worker thread started. Listening to {stream_name}")

    while True:
        try:
            # Read batch from Redis stream
            messages = r.xread({stream_name: last_id}, block=5000, count=1)
            if not messages:
                continue

            _, entries = messages[0]
            msg_id, fields = entries[0]
            last_id = msg_id

            batch_json = fields[b"batch"].decode()
            batch = json.loads(batch_json)

            image_paths = [item["frame_path"] for item in batch]
            print(f"[{backend.model_name}] 📥 Batch received: {len(image_paths)} images")

            # Run Inference
            results = backend.infer(image_paths)


            # Save to PostgreSQL and disk
            for i, item in enumerate(results):
                batch_item = batch[i]
                rule_ids = batch_item.get("rule_ids", [])

                # -----------------------------------------------------
                # SAVE ANNOTATED IMAGE IF DETECTIONS EXIST
                # -----------------------------------------------------
                if len(item["detections"]) > 0:
                    try:
                        plant = batch_item.get("plant")
                        site = batch_item.get("site")
                        camera = batch_item.get("camera")
                        timestamp_str = batch_item.get("timestamp")

                        if plant and site and camera and timestamp_str:
                            ts = datetime.fromisoformat(timestamp_str)
                            folder_time = ts.strftime("%Y_%m_%d_%H")
                            
                            # /shared/detected_frames/plant/site/camera/2023_...
                            save_dir = os.path.join(SHARED_DIR, "detected_frames", plant, site, camera, folder_time)
                            os.makedirs(save_dir, exist_ok=True)

                            original_filename = os.path.basename(item["image_path"])
                            
                            # Fix overlap: append model name
                            name_part, ext_part = os.path.splitext(original_filename)
                            new_filename = f"{name_part}_{backend.model_name}{ext_part}"
                            
                            save_path = os.path.join(save_dir, new_filename)

                            # Plot (returns BGR numpy array compatible with cv2)
                            plotted_img = item["original_result"].plot()
                            cv2.imwrite(save_path, plotted_img)
                            print(f"[{backend.model_name}] 📸 Saved detection: {save_path} (Rules: {rule_ids})")
                    except Exception as e:
                        print(f"[{backend.model_name}] ⚠️ Failed to save annotation: {e}")
                
                # -----------------------------------------------------
                # DB SAVE
                # -----------------------------------------------------
                frame_db_id = batch_item.get("frame_db_id")
                model_id = backend.config.get("model_id")
                
                if frame_db_id is not None and model_id is not None:
                    pg_cursor.execute("""
                        INSERT INTO model_detections (frame_id, model_id, detection)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (frame_id, model_id) 
                        DO UPDATE SET detection = EXCLUDED.detection, timestamp = NOW();
                    """, (frame_db_id, model_id, Json(item["detections"])))
                else:
                    # Fallback to old table or log warning
                    print(f"[{backend.model_name}] ⚠️ custom save skipped (missing IDs). frame_id={frame_db_id} model_id={model_id}")
                    pg_cursor.execute("""
                        INSERT INTO detection_results (image_path, detections)
                        VALUES (%s, %s)
                    """, (item["image_path"], Json(item["detections"])))

            print(f"[{backend.model_name}] 💾 Saved {len(results)} results")

            # Delete message from Redis
            r.xdel(stream_name, msg_id)

        except Exception as e:
            print(f"[{backend.model_name}] ❌ Error:", e)
            traceback.print_exc()
            time.sleep(1)

# ---------------------------------------------------------
# MAIN LOADER
# ---------------------------------------------------------
def load_config():
    try:
        with open(MODELS_CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Could not read {MODELS_CONFIG_PATH}: {e}")
        return {}

def main():
    print("🚀 Model Worker Manager Started")
    if YOLO is None:
        # raise ImportError("Ultralytics not installed")  
        print("Ultralytics not installed")
    # Wait for Redis/DB to be ready
    time.sleep(5) 
    
    config = load_config()
    threads = []

    for model_name, model_cfg in config.items():
        print(f"🔧 Initializing backend for: {model_name}")
        
        # FACTORY LOGIC (Expand here for Triton, etc.)
        # For now, we assume everything is YOLO unless specified otherwise
        backend_type = model_cfg.get("type", "yolo")
        
        try:
            if backend_type == "yolo":
                backend = YOLOBackend(model_name, model_cfg)
            else:
                print(f"⚠️ Unknown backend type '{backend_type}' for {model_name}. Skipping.")
                continue               
            # Load model (this might fail if file is missing)
            backend.load()
            
            # Start Thread
            t = threading.Thread(target=worker_loop, args=(backend,), daemon=True)
            t.start()
            threads.append(t)
            
        except Exception as e:
            print(f"⚠️ FAILED to start worker for {model_name}: {e}")
            print(f"   -> This model will NOT process frames.")
            continue

    if not threads:
        print("❌ No workers started. Exiting...")
        return

    # Keep main thread alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
