import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def test_connection():
    load_dotenv()
    
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    
    print(f"Testing connection to: {host}:{port}/{dbname} as {user}")
    
    if not all([user, password, host, dbname]):
        print("Error: Missing database credentials in .env")
        return False

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Connection successful! Result:", result.scalar())
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    if not success:
        sys.exit(1)
