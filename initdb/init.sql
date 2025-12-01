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


