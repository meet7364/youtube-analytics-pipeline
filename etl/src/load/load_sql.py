import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Handles loading data into Cloud PostgreSQL using SQLAlchemy.
    """
    def __init__(self, connection_string: str = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: SQLAlchemy connection string. If None, built from env vars.
        """
        if not connection_string:
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASSWORD")
            host = os.getenv("DB_HOST")
            port = os.getenv("DB_PORT", "5432")
            dbname = os.getenv("DB_NAME")
            
            if not all([user, password, host, dbname]):
                 raise ValueError("Database credentials must be set in environment variables.")
            
            connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def load_data(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        unique_keys: list[str]
    ) -> int:
        """
        Load dataframe into database with upsert (merge) logic.
        
        Args:
            df: Pandas DataFrame to load.
            table_name: Target table name.
            unique_keys: List of column names that form the unique constraint (for conflict resolution).
            
        Returns:
            Number of rows affected.
        """
        if df.empty:
            logger.info(f"No data to load for table {table_name}")
            return 0
            
        # Create a list of dictionaries for bulk insert
        records = df.to_dict(orient='records')
        
        # Prepare the insert statement with ON CONFLICT logic
        # Prepare the insert statement with ON CONFLICT logic
        # Note: pd.io.sql.get_schema is a bit hacky for table object, better to assume table exists or reflect it.
        # But for raw sql construction, we can just use the table name string if we trust input.
        
        # Let's rely on sqlalchemy Table reflection or just construct text/table object if needed.
        # However, dialects.postgresql.insert requires a Table object usually.
        # To keep it simple and efficient without reflecting specific schema classes repeatedly:
        from sqlalchemy import MetaData, Table
        metadata = MetaData()
        
        # Reflect table from database to ensure we match columns
        try:
            target_table = Table(table_name, metadata, autoload_with=self.engine)
        except Exception as e:
            logger.error(f"Error reflecting table {table_name}: {e}")
            raise

        stmt = insert(target_table).values(records)
        
        # Define what to do on conflict (update all columns except unique keys and created_at)
        # We need to exclude columns that shouldn't be updated or don't exist in source
        update_cols = {col.name: col for col in stmt.excluded if col.name not in unique_keys + ['created_at']}
        
        if not update_cols:
            # If all columns are keys (rare), do nothing
            on_conflict_stmt = stmt.on_conflict_do_nothing(index_elements=unique_keys)
        else:
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=unique_keys,
                set_=update_cols
            )

        # Execute
        with self.engine.connect() as conn:
            result = conn.execute(on_conflict_stmt)
            conn.commit()
            logger.info(f"Upserted {result.rowcount} rows into {table_name}")
            return result.rowcount
