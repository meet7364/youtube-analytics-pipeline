from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text

def verify_data():
    load_dotenv()
    
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        print("--- Verification Report ---")
        
        # 1. Check Channels
        result = conn.execute(text("SELECT COUNT(*) FROM youtube_channels")).scalar()
        print(f"Channels Loaded: {result}")
        
        # 2. Check Videos
        result = conn.execute(text("SELECT COUNT(*) FROM youtube_videos")).scalar()
        print(f"Videos Loaded:   {result}")
        
        # 3. Check Comments
        result = conn.execute(text("SELECT COUNT(*) FROM youtube_comments")).scalar()
        print(f"Comments Loaded: {result}")
        
        # 4. Check Metrics
        result = conn.execute(text("SELECT COUNT(*) FROM youtube_metrics")).scalar()
        print(f"Metrics Rows:    {result}")
        
        # 5. Check View: Channel Summary
        print("\n--- Channel Summary View (Top 1) ---")
        try:
            df = pd.read_sql("SELECT * FROM analytics.channel_summary LIMIT 1", conn)
            print(df.to_string(index=False))
        except Exception as e:
            print(f"Error reading view: {e}")

if __name__ == "__main__":
    verify_data()
