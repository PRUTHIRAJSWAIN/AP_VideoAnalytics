CREATE TABLE IF NOT EXISTS frame_repository (
    id SERIAL PRIMARY KEY,
    frame_id TEXT,
    plant TEXT,
    site TEXT,
    camera TEXT,
    timestamp TEXT,
    file_path TEXT,
    version INT DEFAULT 1
);
CREATE TABLE IF NOT EXISTS model_detections (
    frame_id INT REFERENCES frame_repository(id),
    model_id INT,
    detection JSONB,
    timestamp TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(frame_id, model_id)
);
