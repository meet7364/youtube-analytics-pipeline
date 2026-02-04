import os
import sys
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_db():
    load_dotenv()
    
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    
    if not all([user, password, host, dbname]):
        logger.error("Missing database credentials in .env")
        return False

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        engine = create_engine(connection_string)
        
        # Read schema file
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        with engine.connect() as conn:
            # 1. Drop existing tables in reverse dependency order
            logger.info("Dropping existing tables...")
            conn.execute(text("DROP TABLE IF EXISTS youtube_comments CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS youtube_metrics CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS youtube_videos CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS youtube_channels CASCADE;"))
            # Cleanup old tables if they exist
            conn.execute(text("DROP TABLE IF EXISTS video_daily_metrics CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS channel_daily_metrics CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS videos CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS channels CASCADE;"))
            
            # 2. Apply Schema
            logger.info("Applying schema...")
            # Split by ';' to handle multiple statements if driver doesn't support bulk
            # But sqlalchemy/psycopg2 usually handles it or we can execute the whole block
            conn.execute(text(schema_sql))
            
            conn.commit()
            logger.info("Database initialized successfully!")
            return True
            
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

if __name__ == "__main__":
    if init_db():
        sys.exit(0)
    else:
        sys.exit(1)
