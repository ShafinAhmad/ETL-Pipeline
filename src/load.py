import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def load(
    input: pd.DataFrame,
    rejected: pd.DataFrame,
    host: str = "localhost",
    port: int = 5432,
    database: str = "etl_pipeline",
    user: str = "postgres",
    password: str = "postgres"
) -> None:
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                Series_Title VARCHAR(500) UNIQUE NOT NULL,
                Released_Year INT,
                Runtime INT,
                Genre TEXT[],
                IMDB_Rating FLOAT,
                Meta_score INT,
                Director VARCHAR(500),
                Star1 VARCHAR(500),
                Star2 VARCHAR(500),
                Star3 VARCHAR(500),
                Star4 VARCHAR(500),
                No_of_Votes INT,
                Gross INT,
                Decade INT,
                Genre_Count INT,
                Stars TEXT[],
                Movie_Length VARCHAR(50),
                Rating_Category VARCHAR(50),
                Popularity_Score FLOAT,
                Gross_Inflation_Adjusted INT
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rejected_records (
                id SERIAL PRIMARY KEY,
                reason VARCHAR(500),
                raw_data JSONB
            );
        """)
        
        # Insert valid records
        if not input.empty:
            cols = [col for col in input.columns if col != 'id']
            data_tuples = [tuple(row[col] for col in cols) for _, row in input.iterrows()]
            cols_str = ', '.join(cols)
            placeholders = ', '.join(['%s'] * len(cols))
            
            update_cols = [col for col in cols if col != 'Series_Title']
            set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            
            insert_query = f"INSERT INTO movies ({cols_str}) VALUES ({placeholders}) ON CONFLICT (Series_Title) DO UPDATE SET {set_clause}"
            execute_values(cursor, insert_query, data_tuples, page_size=1000)
            print(f"Inserted or updated {len(data_tuples)} valid records in 'movies' table")
        
        # Insert rejected records
        if not rejected.empty:
            rejected_tuples = [(None, row.to_json()) for _, row in rejected.iterrows()]
            execute_values(cursor, "INSERT INTO rejected_records (reason, raw_data) VALUES (%s, %s)", rejected_tuples)
            print(f"Inserted {len(rejected_tuples)} rejected records into 'rejected_records' table")
        
        conn.commit()
        print("Data successfully loaded to PostgreSQL")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()