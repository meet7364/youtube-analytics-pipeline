import os
import logging
import sys
from dotenv import load_dotenv

# Add src to path to allow imports if running directly
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract.youtube_api import YouTubeAPI
from src.transform.clean_data import process_channels, process_videos, process_comments
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
        channel_ids_env = os.getenv("CHANNEL_IDS") or os.getenv("YOUTUBE_CHANNEL_IDS", "")
        if not channel_ids_env:
            logger.error("CHANNEL_IDS (or YOUTUBE_CHANNEL_IDS) environment variable is not set.")
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
        loader.load_data(channels_df, "youtube_channels", ["channel_id"])
        loader.load_data(channel_metrics_df, "youtube_metrics", ["entity_id", "date"])

        # --- EXTRACT & TRANSFORM & LOAD: VIDEOS & COMMENTS ---
        for channel_id in channel_ids:
            logger.info(f"Processing videos for channel: {channel_id}")
            
            # Extract Videos
            raw_videos = api.get_videos(channel_id, limit=50)
            
            if not raw_videos:
                logger.info(f"No videos found for channel {channel_id}")
                continue
                
            # Transform Videos
            videos_df, video_metrics_df = process_videos(raw_videos)
            
            # Load Videos
            logger.info(f"Loading {len(videos_df)} videos for channel {channel_id}...")
            loader.load_data(videos_df, "youtube_videos", ["video_id"])
            loader.load_data(video_metrics_df, "youtube_metrics", ["entity_id", "date"])
            
            # Extract & Process Comments
            logger.info("Processing comments...")
            all_comments_df = pd.DataFrame()
            
            for index, row in videos_df.iterrows():
                video_id = row['video_id']
                raw_comments = api.get_video_comments(video_id, limit=20)
                if raw_comments:
                    comments_df = process_comments(raw_comments, video_id)
                    all_comments_df = pd.concat([all_comments_df, comments_df], ignore_index=True)
            
            # Load Comments
            if not all_comments_df.empty:
               logger.info(f"Loading {len(all_comments_df)} comments for channel {channel_id}...")
               loader.load_data(all_comments_df, "youtube_comments", ["comment_id"])

        logger.info("Pipeline finished successfully.")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
