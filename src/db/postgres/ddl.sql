CREATE TABLE IF NOT EXISTS raw_user_events (
    event_id UUID PRIMARY KEY,
    user_id INT NOT NULL,
    webtoon_id TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    utimestamptz TIMESTAMP NOT NULL,
    local_timestamptz TIMESTAMP NOT NULL,
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