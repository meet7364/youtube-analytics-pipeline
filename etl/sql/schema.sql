-- Database Schema for YouTube Analytics Pipeline

-- Channels table: Stores static/slowly changing channel metadata
CREATE TABLE IF NOT EXISTS channels (
    channel_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    custom_url VARCHAR(100),
    published_at TIMESTAMP WITH TIME ZONE,
    thumbnail_url VARCHAR(255),
    country VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Videos table: Stores static/slowly changing video metadata
CREATE TABLE IF NOT EXISTS videos (
    video_id VARCHAR(50) PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL REFERENCES channels(channel_id),
    title VARCHAR(500) NOT NULL, -- Titles can sometimes be long
    description TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    thumbnail_url VARCHAR(255),
    tags TEXT[],
    category_id VARCHAR(10),
    duration VARCHAR(20), -- ISO 8601 duration format
    definition VARCHAR(10), -- hd, sd
    caption VARCHAR(10), -- true, false
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Channel Daily Metrics: Time-series data for channel performance
CREATE TABLE IF NOT EXISTS channel_daily_metrics (
    id SERIAL PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL REFERENCES channels(channel_id),
    date DATE NOT NULL,
    view_count BIGINT,
    subscriber_count BIGINT,
    video_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, date) -- Ensure one record per channel per day
);

-- Video Daily Metrics: Time-series data for video performance
CREATE TABLE IF NOT EXISTS video_daily_metrics (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL REFERENCES videos(video_id),
    date DATE NOT NULL,
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(video_id, date) -- Ensure one record per video per day
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_videos_channel_id ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_channel_metrics_date ON channel_daily_metrics(date);
CREATE INDEX IF NOT EXISTS idx_video_metrics_date ON video_daily_metrics(date);
