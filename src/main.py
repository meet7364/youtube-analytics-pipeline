import os
import logging
import sys
from dotenv import load_dotenv

# Add src to path to allow imports if running directly
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract.youtube_api import YouTubeAPI
from src.transform.clean_data import process_channels, process_videos
from src.load.load_sql import DataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Main pipeline execution function.
    """
    try:
        load_dotenv()
        logger.info("Starting YouTube Analytics Pipeline")

        # Configuration
        channel_ids_env = os.getenv("YOUTUBE_CHANNEL_IDS", "")
        if not channel_ids_env:
            logger.error("YOUTUBE_CHANNEL_IDS environment variable is not set locally.")
            # For testing purposes, if no env var, we might fallback or exit. 
            # Letting it exit to force proper config.
            return

        channel_ids = [cid.strip() for cid in channel_ids_env.split(",") if cid.strip()]
        logger.info(f"Targeting {len(channel_ids)} channels: {channel_ids}")

        # Initialize modules
        api = YouTubeAPI()
        loader = DataLoader()

        # --- EXTRACT & TRANSFORM: CHANNELS ---
        logger.info("Extracting channel details...")
        raw_channels = api.get_channel_details(channel_ids)
        if not raw_channels:
            logger.warning("No channel contents found.")
            return

        logger.info("Transforming channel data...")
        channels_df, channel_metrics_df = process_channels(raw_channels)

        # --- LOAD: CHANNELS ---
        logger.info("Loading channel data...")
        loader.load_data(channels_df, "channels", ["channel_id"])
        loader.load_data(channel_metrics_df, "channel_daily_metrics", ["channel_id", "date"])

        # --- EXTRACT & TRANSFORM & LOAD: VIDEOS ---
        for channel_id in channel_ids:
            logger.info(f"Processing videos for channel: {channel_id}")
            
            # Extract
            raw_videos = api.get_videos(channel_id, limit=50) # Limit 50 for dev/test, can be higher
            
            if not raw_videos:
                logger.info(f"No videos found for channel {channel_id}")
                continue
                
            # Transform
            videos_df, video_metrics_df = process_videos(raw_videos)
            
            # Load
            logger.info(f"Loading {len(videos_df)} videos for channel {channel_id}...")
            loader.load_data(videos_df, "videos", ["video_id"])
            loader.load_data(video_metrics_df, "video_daily_metrics", ["video_id", "date"])

        logger.info("Pipeline finished successfully.")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
