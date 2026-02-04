-- Database Schema for YouTube Analytics Pipeline (Refactored)

-- 1. Channels Table
CREATE TABLE IF NOT EXISTS youtube_channels (
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

-- 2. Videos Table
CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id VARCHAR(50) PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL REFERENCES youtube_channels(channel_id),
    title VARCHAR(500) NOT NULL,
    description TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    thumbnail_url VARCHAR(255),
    tags TEXT[],
    category_id VARCHAR(10),
    duration VARCHAR(20),
    definition VARCHAR(10),
    caption VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Unified Metrics Table (Channels & Videos)
CREATE TABLE IF NOT EXISTS youtube_metrics (
    id SERIAL PRIMARY KEY,
    entity_id VARCHAR(50) NOT NULL, -- channel_id or video_id
    entity_type VARCHAR(20) NOT NULL, -- 'channel' or 'video'
    date DATE NOT NULL,
    view_count BIGINT DEFAULT 0,
    subscriber_count BIGINT DEFAULT 0, -- Channel only
    video_count INTEGER DEFAULT 0,     -- Channel only
    like_count BIGINT DEFAULT 0,       -- Video only
    comment_count BIGINT DEFAULT 0,    -- Video only
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_id, date)
);

-- 4. Comments Table
CREATE TABLE IF NOT EXISTS youtube_comments (
    comment_id VARCHAR(100) PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL REFERENCES youtube_videos(video_id),
    author_name VARCHAR(255),
    text TEXT,
    like_count INTEGER DEFAULT 0,
    published_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_videos_channel_id ON youtube_videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_metrics_date ON youtube_metrics(date);
CREATE INDEX IF NOT EXISTS idx_metrics_entity ON youtube_metrics(entity_id);
CREATE INDEX IF NOT EXISTS idx_comments_video_id ON youtube_comments(video_id);

-- 5. Analytics Views
CREATE SCHEMA IF NOT EXISTS analytics;

-- View 1: Channel Summary (Latest snapshot + specific metrics)
CREATE OR REPLACE VIEW analytics.channel_summary AS
SELECT 
    c.channel_id,
    c.title,
    c.country,
    m.view_count,
    m.subscriber_count,
    m.video_count,
    m.date as last_updated
FROM youtube_channels c
JOIN youtube_metrics m ON c.channel_id = m.entity_id 
WHERE m.entity_type = 'channel' 
AND m.date = (SELECT MAX(date) FROM youtube_metrics WHERE entity_id = c.channel_id);

-- View 2: Video Performance (Top videos by views)
CREATE OR REPLACE VIEW analytics.video_performance AS
SELECT 
    v.video_id,
    v.title,
    c.title as channel_name,
    v.published_at,
    m.view_count,
    m.like_count,
    m.comment_count,
    m.date as metric_date
FROM youtube_videos v
JOIN youtube_channels c ON v.channel_id = c.channel_id
JOIN youtube_metrics m ON v.video_id = m.entity_id
WHERE m.entity_type = 'video'
AND m.date = (SELECT MAX(date) FROM youtube_metrics WHERE entity_id = v.video_id);

-- View 3: Daily Growth (Aggregation over time)
CREATE OR REPLACE VIEW analytics.daily_growth AS
SELECT
    date,
    SUM(view_count) as total_views,
    SUM(subscriber_count) as total_subscribers,
    SUM(video_count) as total_videos
FROM youtube_metrics
WHERE entity_type = 'channel'
GROUP BY date
ORDER BY date DESC;
