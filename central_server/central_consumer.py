import redis
import base64
import os
import time
import json
import threading
from datetime import datetime
import psycopg2  
from psycopg2.extras import execute_values 

# -------------------------------------------------------
# 1. Redis Connection and Postgres Connection
# -------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis_server")
r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)


PG_HOST = os.getenv("PG_HOST", "postgres_server")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
PG_DB   = os.getenv("PG_DB",   "frames_db")

pg_conn = psycopg2.connect(
    host=PG_HOST,
    user=PG_USER,
    password=PG_PASS,
    dbname=PG_DB
)
pg_conn.autocommit = True
pg_cur = pg_conn.cursor()

SHARED_DIR = os.getenv("SHARED_DIR", "/shared")  # mount point inside containers
SAVE_DIR = os.path.join(SHARED_DIR, "frames")    # /shared/frames
os.makedirs(SAVE_DIR, exist_ok=True)

print("🚀 Central consumer with batching started")


# -------------------------------------------------------
# 2. Load Config Files Safely
# -------------------------------------------------------
def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except:
        print(f"⚠️ Could not read {path}, using empty")
        return {}

MODELS_CONFIG_PATH = os.path.join(SHARED_DIR, "configs", "models_config.json")
ROUTING_CONFIG_PATH = os.path.join(SHARED_DIR, "configs", "routing_config.json")

config_models = load_json(MODELS_CONFIG_PATH)
config_routing = load_json(ROUTING_CONFIG_PATH)

# Track modification timestamps
models_mtime = os.path.getmtime(MODELS_CONFIG_PATH) if os.path.exists(MODELS_CONFIG_PATH) else 0
routing_mtime = os.path.getmtime(ROUTING_CONFIG_PATH) if os.path.exists(ROUTING_CONFIG_PATH) else 0


def hot_reload_configs():
    """Reload JSON files when they change on disk."""
    global config_models, config_routing, models_mtime, routing_mtime

    while True:
        try:
            if os.path.exists(MODELS_CONFIG_PATH):
                new_m_mtime = os.path.getmtime(MODELS_CONFIG_PATH)
                if new_m_mtime != models_mtime:
                    config_models = load_json(MODELS_CONFIG_PATH)
                    models_mtime = new_m_mtime
                    print(f"🔄 Reloaded {MODELS_CONFIG_PATH}")

            if os.path.exists(ROUTING_CONFIG_PATH):
                new_r_mtime = os.path.getmtime(ROUTING_CONFIG_PATH)
                if new_r_mtime != routing_mtime:
                    config_routing = load_json(ROUTING_CONFIG_PATH)
                    routing_mtime = new_r_mtime
                    print(f"🔄 Reloaded {ROUTING_CONFIG_PATH}")

        except Exception as e:
            print("⚠️ Hot reload error:", e)

        time.sleep(1)


threading.Thread(target=hot_reload_configs, daemon=True).start()


# -------------------------------------------------------
# 3. Batch Structures
# -------------------------------------------------------
batches = {}              # {"model_name": [...]}
batch_lock = threading.Lock()
batch_start_time = {}     # {"model_name": timestamp}


# -------------------------------------------------------
# 4. Helpers
# -------------------------------------------------------
def get_model_for_camera(plant, site, camera):
    """Return model assigned for a camera."""
    try:
        return config_routing[plant][site][camera]['model']
    except:
        return None


def dispatch_batch(model_name):
    """Push completed batch to Redis stream."""
    with batch_lock:
        batch = batches.get(model_name, [])
        if not batch:
            return

        payload = json.dumps(batch)
        stream_name = f"model_queue:{model_name}"

        r.xadd(stream_name, {"batch": payload})
        print(f"📤 Sent batch → {model_name} | size={len(batch)}")

        batches[model_name] = []
        batch_start_time[model_name] = time.time()


def add_to_batch(model_name, item):
    with batch_lock:
        if model_name not in batches:
            batches[model_name] = []
            batch_start_time[model_name] = time.time()

        batches[model_name].append(item)


def batch_monitor():
    """Periodically dispatch timed-out batches."""
    while True:
        with batch_lock:
            timed_out = []
            for model_name, batch_items in batches.items():
                if not batch_items:
                    continue

                max_wait = config_models.get(model_name, {}).get("max_wait_time", 2)
                started = batch_start_time.get(model_name, time.time())

                if time.time() - started >= max_wait:
                    timed_out.append(model_name)


        for model_name in timed_out:
            print(f"⏱ Timeout → dispatching {model_name}")
            dispatch_batch(model_name)

        time.sleep(5)


threading.Thread(target=batch_monitor, daemon=True).start()


# -------------------------------------------------------
# 5. MAIN LOOP
# -------------------------------------------------------
last_id = "$"   # Read ONLY new messages (important!)

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

        # Determine model
        model = get_model_for_camera(plant, site, camera)
        if model is None:
            print(f"⚠️ No model for {plant}/{site}/{camera}")
            continue

        # Convert timestamp → folder name
        ts = datetime.fromisoformat(timestamp)
        folder_time = ts.strftime("%Y_%m_%d_%H")

        # Save decoded image
        frame_bytes = base64.b64decode(fields[b"frame"])

        save_path = os.path.join(SAVE_DIR, plant, site, camera, folder_time)
        os.makedirs(save_path, exist_ok=True)

        file_path = os.path.join(save_path, f"{msg_id.decode()}.jpg")

        with open(file_path, "wb") as f:
            f.write(frame_bytes)

        print(f"✔ Saved {file_path}")

        # -------------------------------------------------------
        # 🟢 ADD: Insert into Postgres BEFORE batching
        # -------------------------------------------------------
        pg_cur.execute(
            """
            INSERT INTO frame_repository (frame_id, plant, site, camera, timestamp, file_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (msg_id.decode(), plant, site, camera, timestamp, file_path)
        )
        db_id = pg_cur.fetchone()[0]
        # -------------------------------------------------------

        # DO NOT DELETE STREAM MESSAGE — EVER.
        # If you want cleaning, use XTRIM at system level.

        # Add to batch
        add_to_batch(model, {
            "frame_path": file_path,
            "plant": plant,
            "site": site,
            "camera": camera,
            "timestamp": timestamp,
            "frame_db_id": db_id
        })

        # Batch full? dispatch immediately
        current_batch = batches.get(model, [])
        batch_size = config_models.get(model, {}).get("batch_size", 4)

        if len(current_batch) >= batch_size:
            print(f"📦 Max batch size → {model}")
            dispatch_batch(model)

    except Exception as e:
        print("❌ ERROR:", e)
        time.sleep(1)
