import pandas as pd
import re
from typing import List, Dict, Any, Tuple
from datetime import datetime

def parse_duration(duration: str) -> int:
    """
    Parse ISO 8601 duration string (e.g., PT1H2M10S) to seconds.
    """
    if not duration:
        return 0
    
    # Regex to extract hours, minutes, seconds
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    
    if not match:
        return 0
    
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    
    return hours * 3600 + minutes * 60 + seconds

def process_channels(raw_items: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw channel data into dim_channel and fact_channel_daily dataframes.
    """
    dim_channel_data = []
    fact_channel_data = []
    
    current_date = datetime.now().date()
    
    for item in raw_items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        
        # dim_channel
        dim_channel_data.append({
            "channel_id": item.get("id"),
            "channel_name": snippet.get("title"),
            "description": snippet.get("description"),
            "custom_url": snippet.get("customUrl"),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url"),
            "country": snippet.get("country")
        })
        
        # fact_channel_daily
        fact_channel_data.append({
            "channel_id": item.get("id"),
            "date_id": current_date,
            "subscribers": int(statistics.get("subscriberCount", 0)),
            "total_views": int(statistics.get("viewCount", 0)),
            "total_videos": int(statistics.get("videoCount", 0))
        })
        
    return pd.DataFrame(dim_channel_data), pd.DataFrame(fact_channel_data)

def process_videos(raw_items: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process raw video data into dim_video and fact_video_daily dataframes.
    """
    dim_video_data = []
    fact_video_data = []
    
    current_date = datetime.now().date()
    
    for item in raw_items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        
        # dim_video
        dim_video_data.append({
            "video_id": item.get("id"),
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": snippet.get("publishedAt"),
            "duration_seconds": parse_duration(content_details.get("duration")),
            "category": snippet.get("categoryId"), # Using ID as category for now
            "tags": snippet.get("tags", []),
            "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url")
        })
        
        # fact_video_daily
        fact_video_data.append({
            "video_id": item.get("id"),
            "date_id": current_date,
            "views": int(statistics.get("viewCount", 0)),
            "likes": int(statistics.get("likeCount", 0)),
            "comments": int(statistics.get("commentCount", 0))
        })
        
    return pd.DataFrame(dim_video_data), pd.DataFrame(fact_video_data)

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
