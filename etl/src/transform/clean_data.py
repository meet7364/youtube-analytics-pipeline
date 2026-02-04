import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime

def process_channels(raw_items: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw channel data into channels and unified metrics dataframes.
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
        
        # Unified Metrics (Channel)
        metrics_data.append({
            "entity_id": item.get("id"),
            "entity_type": "channel",
            "date": current_date,
            "view_count": int(statistics.get("viewCount", 0)),
            "subscriber_count": int(statistics.get("subscriberCount", 0)),
            "video_count": int(statistics.get("videoCount", 0)),
            "like_count": 0,
            "comment_count": 0
        })
        
    return pd.DataFrame(channels_data), pd.DataFrame(metrics_data)

def process_videos(raw_items: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw video data into videos and unified metrics dataframes.
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
        
        # Unified Metrics (Video)
        metrics_data.append({
            "entity_id": item.get("id"),
            "entity_type": "video",
            "date": current_date,
            "view_count": int(statistics.get("viewCount", 0)),
            "like_count": int(statistics.get("likeCount", 0)),
            "comment_count": int(statistics.get("commentCount", 0)),
            "subscriber_count": 0,
            "video_count": 0
        })
        
    return pd.DataFrame(videos_data), pd.DataFrame(metrics_data)

def process_comments(raw_items: List[Dict[str, Any]], video_id: str) -> pd.DataFrame:
    """
    Process raw comment threads into a dataframe.
    """
    comments_data = []
    
    for item in raw_items:
        top_comment = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        
        comments_data.append({
            "comment_id": item.get("id"),
            "video_id": video_id,
            "author_name": top_comment.get("authorDisplayName"),
            "text": top_comment.get("textDisplay"),
            "like_count": int(top_comment.get("likeCount", 0)),
            "published_at": top_comment.get("publishedAt")
        })
        
    return pd.DataFrame(comments_data)
