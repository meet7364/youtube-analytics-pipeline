import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime

def process_channels(raw_items: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw channel data into channels and metrics dataframes.
    
    Args:
        raw_items: List of raw channel resource objects from API.
        
    Returns:
        Tuple containing (channels_df, metrics_df)
    """
    channels_data = []
    metrics_data = []
    
    current_date = datetime.now().date()
    
    for item in raw_items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        
        # Channel Metadata
        channels_data.append({
            "channel_id": item.get("id"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "custom_url": snippet.get("customUrl"),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url"),
            "country": snippet.get("country")
        })
        
        # Channel Metrics
        metrics_data.append({
            "channel_id": item.get("id"),
            "date": current_date,
            "view_count": int(statistics.get("viewCount", 0)),
            "subscriber_count": int(statistics.get("subscriberCount", 0)),
            "video_count": int(statistics.get("videoCount", 0))
        })
        
    return pd.DataFrame(channels_data), pd.DataFrame(metrics_data)

def process_videos(raw_items: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw video data into videos and metrics dataframes.
    
    Args:
        raw_items: List of raw video resource objects from API.
        
    Returns:
        Tuple containing (videos_df, metrics_df)
    """
    videos_data = []
    metrics_data = []
    
    current_date = datetime.now().date()
    
    for item in raw_items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        
        # Video Metadata
        videos_data.append({
            "video_id": item.get("id"),
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url"),
            "tags": snippet.get("tags", []),
            "category_id": snippet.get("categoryId"),
            "duration": content_details.get("duration"),
            "definition": content_details.get("definition"),
            "caption": content_details.get("caption")
        })
        
        # Video Metrics
        metrics_data.append({
            "video_id": item.get("id"),
            "date": current_date,
            "view_count": int(statistics.get("viewCount", 0)),
            "like_count": int(statistics.get("likeCount", 0)),
            "comment_count": int(statistics.get("commentCount", 0))
        })
        
    return pd.DataFrame(videos_data), pd.DataFrame(metrics_data)
