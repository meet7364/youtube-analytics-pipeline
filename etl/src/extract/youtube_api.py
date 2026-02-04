import os
import logging
import requests
from typing import List, Dict, Optional, Any
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YouTubeAPI:
    """
    Client for interacting with the YouTube Data API v3.
    """
    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the YouTube API client.
        
        Args:
            api_key: YouTube Data API key. If not provided, reads from YOUTUBE_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        if not self.api_key:
            raise ValueError("YouTube API key must be provided or set in environment variable YOUTUBE_API_KEY")

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Helper method to make API requests with error handling.
        """
        params["key"] = self.api_key
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {str(e)}")
            if response is not None:
                logger.error(f"Response: {response.text}")
            raise

    def get_channel_details(self, channel_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch details for a list of channel IDs.
        
        Args:
            channel_ids: List of YouTube channel IDs.
            
        Returns:
            List of channel resource objects.
        """
        if not channel_ids:
            return []

        # Join IDs with commas (API limit is usually 50 ids per request)
        ids_string = ",".join(channel_ids)
        
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ids_string
        }
        
        data = self._make_request("channels", params)
        return data.get("items", [])

    def get_videos(self, channel_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Fetch videos for a specific channel using the upload playlist.
        
        Args:
            channel_id: The ID of the channel.
            limit: Maximum number of videos to retrieve.
            
        Returns:
            List of video resource objects (with snippet and statistics).
        """
        # 1. Get the uploads playlist ID
        channel_data = self.get_channel_details([channel_id])
        if not channel_data:
            logger.warning(f"Channel {channel_id} not found.")
            return []
            
        uploads_playlist_id = channel_data[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        
        videos = []
        next_page_token = None
        
        # 2. Fetch videos from the playlist
        while len(videos) < limit:
            request_limit = min(50, limit - len(videos))
            
            params = {
                "part": "snippet,contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": request_limit,
            }
            if next_page_token:
                params["pageToken"] = next_page_token
                
            data = self._make_request("playlistItems", params)
            items = data.get("items", [])
            
            if not items:
                break
                
            # We need video IDs to get statistics (views, likes, etc.) as playlistItems 
            # only give snippet and contentDetails.
            video_ids = [item["contentDetails"]["videoId"] for item in items]
            video_stats = self._get_video_statistics(video_ids)
            
            # Merge stats into video objects (simplistic merge)
            # Or just return the detailed video objects directly
            videos.extend(video_stats)
            
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
                
        return videos[:limit]

    def _get_video_statistics(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch detailed statistics for a list of video IDs.
        """
        if not video_ids:
            return []
            
        ids_string = ",".join(video_ids)
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ids_string
        }
        
        return data.get("items", [])

    def get_video_comments(self, video_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Fetch top comments for a video.
        """
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": min(limit, 100),
            "textFormat": "plainText",
            "order": "relevance"
        }
        
        try:
            data = self._make_request("commentThreads", params)
            return data.get("items", [])
        except Exception as e:
            logger.warning(f"Could not fetch comments for video {video_id}: {e}")
            return []
