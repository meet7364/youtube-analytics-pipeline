-- Database Schema for YouTube Analytics Pipeline (Star Schema)

-- --------------------------------------------------
-- STEP 1 — CREATE DIMENSION TABLES
-- --------------------------------------------------

-- 1. Dimension: Channel
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    description TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    country TEXT,
    custom_url TEXT,
    thumbnail_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Dimension: Video
CREATE TABLE IF NOT EXISTS dim_video (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT REFERENCES dim_channel(channel_id),
    title TEXT,
    description TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INT,
    category TEXT,
    item_count INT DEFAULT 0, -- Store for compatibility if needed or removed
    tags TEXT[],
    thumbnail_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Dimension: Date (Generated)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    day_of_week TEXT,
    quarter INT
);

-- Populate dim_date with 10 years of data (past 5, next 5)
INSERT INTO dim_date
SELECT
    datum as date_id,
    EXTRACT(DAY FROM datum) as day,
    EXTRACT(MONTH FROM datum) as month,
    EXTRACT(YEAR FROM datum) as year,
    TO_CHAR(datum, 'Day') as day_of_week,
    EXTRACT(QUARTER FROM datum) as quarter
FROM (SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 3650) AS SEQUENCE(DAY)) DQ
ON CONFLICT (date_id) DO NOTHING;


-- --------------------------------------------------
-- STEP 2 — CREATE FACT TABLES
-- --------------------------------------------------

-- 4. Fact: Channel Daily Metrics
CREATE TABLE IF NOT EXISTS fact_channel_daily (
    id SERIAL PRIMARY KEY,
    channel_id TEXT REFERENCES dim_channel(channel_id),
    date_id DATE REFERENCES dim_date(date_id),
    subscribers BIGINT,
    total_views BIGINT,
    total_videos BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, date_id)
);

-- 5. Fact: Video Daily Metrics
CREATE TABLE IF NOT EXISTS fact_video_daily (
    id SERIAL PRIMARY KEY,
    video_id TEXT REFERENCES dim_video(video_id),
    date_id DATE REFERENCES dim_date(date_id),
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(video_id, date_id)
);

-- 6. Comments Table (Optional/Separate Fact or Dimension depending on usage, kept for raw data)
CREATE TABLE IF NOT EXISTS youtube_comments (
    comment_id TEXT PRIMARY KEY,
    video_id TEXT REFERENCES dim_video(video_id),
    author_name TEXT,
    text TEXT,
    like_count BIGINT DEFAULT 0,
    published_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------
-- STEP 3 — CREATE ANALYTICS VIEWS
-- --------------------------------------------------

CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. analytics_channel_summary
CREATE OR REPLACE VIEW analytics.channel_summary AS
SELECT
    dc.channel_id,
    dc.channel_name,
    dc.country,
    fcd.total_views as latest_total_views,
    fcd.subscribers as latest_subscribers,
    fcd.total_videos as latest_video_count,
    fcd.date_id as last_updated
FROM dim_channel dc
JOIN fact_channel_daily fcd ON dc.channel_id = fcd.channel_id
WHERE fcd.date_id = (
    SELECT MAX(date_id) FROM fact_channel_daily WHERE channel_id = dc.channel_id
);

-- 2. analytics_video_performance
CREATE OR REPLACE VIEW analytics.video_performance AS
SELECT
    dv.video_id,
    dv.title as video_title,
    dc.channel_name,
    fvd.views as total_views,
    fvd.likes as total_likes,
    fvd.comments as total_comments,
    dv.published_at,
    dv.duration_seconds
FROM dim_video dv
JOIN dim_channel dc ON dv.channel_id = dc.channel_id
JOIN fact_video_daily fvd ON dv.video_id = fvd.video_id
WHERE fvd.date_id = (
    SELECT MAX(date_id) FROM fact_video_daily WHERE video_id = dv.video_id
);

-- 3. analytics_channel_growth
CREATE OR REPLACE VIEW analytics.channel_growth AS
SELECT
    fcd.channel_id,
    dc.channel_name,
    fcd.date_id,
    fcd.subscribers as daily_subscribers,
    fcd.subscribers - LAG(fcd.subscribers, 1, 0) OVER (PARTITION BY fcd.channel_id ORDER BY fcd.date_id) as subscriber_growth
FROM fact_channel_daily fcd
JOIN dim_channel dc ON fcd.channel_id = dc.channel_id
ORDER BY fcd.date_id DESC;

-- --------------------------------------------------
-- STEP 5 — PERFORMANCE ADDITIONS
-- --------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_dim_video_channel_id ON dim_video(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_channel_date ON fact_channel_daily(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_channel_cid ON fact_channel_daily(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_video_date ON fact_video_daily(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_video_vid ON fact_video_daily(video_id);

