CREATE TABLE IF NOT EXISTS raw_user_events (
    user_id INT NOT NULL,
    webtoon_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    session_id TEXT NOT NULL PRIMARY KEY,
    timestamp TIMESTAMP,
    local_timestamp TIMESTAMP,
    event_type TEXT NOT NULL,
    country TEXT,
    platform TEXT,
    device TEXT,
    browser TEXT,
    network_type TEXT,
    scroll_ratio DOUBLE PRECISION,
    scroll_event_count INT,
    dwell_time_ms BIGINT
);