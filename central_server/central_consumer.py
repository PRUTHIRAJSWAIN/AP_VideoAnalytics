import redis
import base64
import os
import time
import json
import threading
from datetime import datetime

# -------------------------------------------------------
# 1. Redis Connection
# -------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis_server")
r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

SHARED_DIR = os.getenv("SHARED_DIR", "/shared")  # mount point inside containers
SAVE_DIR = os.path.join(SHARED_DIR, "frames")    # /shared/frames
os.makedirs(SAVE_DIR, exist_ok=True)

print("🚀 Central consumer with batching started")


# -------------------------------------------------------
# 2. Load config files (model settings + camera routing)
# -------------------------------------------------------
def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except:
        print(f"⚠️ Could not read {path}, using empty")
        return {}
    
MODELS_CONFIG_PATH = os.path.join(SHARED_DIR, "configs", "models_configs.json")
ROUTING_CONFIG_PATH = os.path.join(SHARED_DIR, "configs", "routing_config.json")

config_models = load_json(MODELS_CONFIG_PATH)
config_routing = load_json(ROUTING_CONFIG_PATH)


# Track last modification time → for hot reload
models_mtime = os.path.getmtime(MODELS_CONFIG_PATH)
routing_mtime = os.path.getmtime(ROUTING_CONFIG_PATH)


def hot_reload_configs():
    """Reload configuration JSON files when changed."""
    global config_models, config_routing, models_mtime, routing_mtime

    while True:
        try:
            # Check models config
            new_m_mtime = os.path.getmtime(MODELS_CONFIG_PATH)
            if new_m_mtime != models_mtime:
                config_models = load_json(MODELS_CONFIG_PATH)
                models_mtime = new_m_mtime
                print(f"🔄 Reloaded { MODELS_CONFIG_PATH }")

            # Check routing config
            new_r_mtime = os.path.getmtime(ROUTING_CONFIG_PATH)
            if new_r_mtime != routing_mtime:
                config_routing = load_json(ROUTING_CONFIG_PATH)
                routing_mtime = new_r_mtime
                print(f"🔄 Reloaded { ROUTING_CONFIG_PATH }")

        except Exception as e:
            print("⚠️ Hot reload error:", e)

        time.sleep(1)


# Start hot-reload thread
threading.Thread(target=hot_reload_configs, daemon=True).start()


# -------------------------------------------------------
# 3. Batch storage structure (in memory)
# -------------------------------------------------------
batches = {}  # {"model_name": [{"path":..., "timestamp":...}, ...]}
batch_lock = threading.Lock()  # thread safety


# Track when a batch started
batch_start_time = {}  # {"model_name": timestamp}


# -------------------------------------------------------
# 4. Helper functions
# -------------------------------------------------------

def get_model_for_camera(plant, site, camera):
    """Return the model assigned to (plant, site, camera)."""
    try:
        return config_routing[plant][site][camera]
    except:
        return None


def dispatch_batch(model_name):
    """Send completed batch to Redis and clear it."""

    with batch_lock:
        batch = batches.get(model_name, [])

        if not batch:
            return  # nothing to send

        # Redis stream for model → model batches
        stream_name = f"model_queue:{model_name}"

        # Convert batch list to JSON
        payload = json.dumps(batch)

        # Push to Redis
        r.xadd(stream_name, {"batch": payload})

        print(f"📤 Sent batch → {model_name} | size={len(batch)}")

        # Clear batch
        batches[model_name] = []
        batch_start_time[model_name] = time.time()


def add_to_batch(model_name, item):
    """Add image info to model batch."""
    with batch_lock:

        if model_name not in batches:
            batches[model_name] = []
            batch_start_time[model_name] = time.time()

        batches[model_name].append(item)


def batch_monitor():
    """Dispatch batches if max_wait_time exceeded."""
    while True:
        with batch_lock:
            for model_name, batch_items in batches.items():

                if not batch_items:
                    continue

                max_wait = config_models.get(model_name, {}).get("max_wait_time", 2)
                started = batch_start_time.get(model_name, time.time())

                if time.time() - started >= max_wait:
                    print(f"⏱ Timeout reached → dispatching {model_name}")
                    dispatch_batch(model_name)

        time.sleep(0.2)


threading.Thread(target=batch_monitor, daemon=True).start()


# -------------------------------------------------------
# 5. MAIN LOOP – Read frames → Save → Route → Batch
# -------------------------------------------------------
last_id = "0-0"

while True:
    try:
        messages = r.xread({"camera_stream": last_id}, block=5000, count=1)
        if not messages:
            continue

        _, entries = messages[0]
        msg_id, fields = entries[0]
        last_id = msg_id

        # Decode metadata
        plant = fields[b"plant_id"].decode()
        site = fields[b"site_id"].decode()
        camera = fields[b"camera_code"].decode()
        timestamp = fields[b"timestamp"].decode()

        # Which model will process this camera?
        model = get_model_for_camera(plant, site, camera)
        if model is None:
            print(f"⚠️ No model assigned for {plant}/{site}/{camera}")
            continue

        # Convert time to folder format
        ts = datetime.fromisoformat(timestamp)
        folder_time = ts.strftime("%Y_%m_%d_%H")

        # Save image to disk
        frame_bytes = base64.b64decode(fields[b"frame"])

        save_path = os.path.join(SAVE_DIR, plant, site, camera, folder_time)
        os.makedirs(save_path, exist_ok=True)
        file_path = os.path.join(save_path, f"{msg_id}.jpg")

        with open(file_path, "wb") as f:
            f.write(frame_bytes)

        print(f"✔ Saved {file_path}")

        # Remove from Redis to save RAM
        r.xdel("camera_stream", msg_id)

        # Add to batch
        add_to_batch(model, {
            "frame_path": file_path,
            "plant": plant,
            "site": site,
            "camera": camera,
            "timestamp": timestamp
        })

        # Check if batch full → send immediately
        current_batch = batches.get(model, [])
        batch_size = config_models.get(model, {}).get("batch_size", 4)

        if len(current_batch) >= batch_size:
            print(f"📦 Max batch size reached → {model}")
            dispatch_batch(model)

    except Exception as e:
        print("❌ ERROR:", e)
        time.sleep(1)
