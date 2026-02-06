-- Metabase Dashboard Queries
-- These queries are optimized for the Star Schema design.

-- 1. KPI Cards
-- Total Subscribers (Latest snapshot)
SELECT SUM(latest_subscribers) 
FROM analytics.channel_summary;

-- Total Views (Latest snapshot)
SELECT SUM(latest_total_views) 
FROM analytics.channel_summary;

-- Total Videos
SELECT SUM(latest_video_count) 
FROM analytics.channel_summary;


-- 2. Subscriber Growth Trend
-- Daily Growth Over Time
SELECT 
    date_id,
    SUM(subscriber_growth) as total_growth
FROM analytics.channel_growth
GROUP BY date_id
ORDER BY date_id ASC;


-- 3. Views Trend
-- Total Views Per Day
SELECT 
    date_id,
    SUM(views) as daily_views
FROM fact_video_daily
GROUP BY date_id
ORDER BY date_id ASC;


-- 4. Comments Trend
-- Total Comments Per Day
SELECT 
    date_id,
    SUM(comments) as daily_comments
FROM fact_video_daily
GROUP BY date_id
ORDER BY date_id ASC;


-- 5. Upload Frequency
-- Videos Uploaded Per Day
SELECT 
    DATE(published_at) as upload_date,
    COUNT(*) as videos_uploaded
FROM dim_video
GROUP BY DATE(published_at)
ORDER BY upload_date ASC;


-- 6. Top Performing Videos
-- Top 20 Videos by Views (Historical Total or Latest depending on need, here we sum daily or take from view if snapshot needed)
-- Using Analytics View for convenience if it stores totals, but here using Fact aggregation for accuracy over range if needed.
-- But analytics_video_performance has totals.
SELECT 
    video_title,
    channel_name,
    total_views,
    total_likes,
    total_comments
FROM analytics.video_performance
ORDER BY total_views DESC
LIMIT 20;


-- 7. Engagement Rate
-- (Likes + Comments) / Views (Aggregate)
SELECT 
    SUM(likes + comments)::FLOAT / NULLIF(SUM(views), 0) * 100 as engagement_rate_percentage
FROM fact_video_daily;


-- 8. Channel Comparison
-- Average Views per Channel
SELECT 
    dc.channel_name,
    AVG(fvd.views) as avg_daily_views
FROM fact_video_daily fvd
JOIN dim_channel dc ON fvd.channel_id = dc.channel_id
GROUP BY dc.channel_name
ORDER BY avg_daily_views DESC;


-- 9. Best Upload Day
-- Avg Views Grouped by Day of Week
SELECT 
    dd.day_of_week,
    AVG(fvd.views) as avg_views
FROM fact_video_daily fvd
JOIN dim_video dv ON fvd.video_id = dv.video_id
JOIN dim_date dd ON DATE(dv.published_at) = dd.date_id -- Join on publish date
GROUP BY dd.day_of_week
ORDER BY avg_views DESC;


-- 10. Viral Video Detection
-- Videos where views > 3x average
WITH avg_views AS (
    SELECT AVG(total_views) as avg_total FROM analytics.video_performance
)
SELECT 
    video_title,
    total_views
FROM analytics.video_performance, avg_views
WHERE total_views > (avg_total * 3)
ORDER BY total_views DESC;
