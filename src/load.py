import pandas as pd
import psycopg2
from psycopg2 import OperationalError


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
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
        except OperationalError as e:
            # Attempt to create the database if it does not exist
            msg = str(e).lower()
            if f'database "{database}" does not exist' in msg or 'does not exist' in msg:
                tmp_conn = None
                try:
                    tmp_conn = psycopg2.connect(
                        host=host,
                        port=port,
                        database="postgres",
                        user=user,
                        password=password
                    )
                    tmp_conn.autocommit = True
                    tmp_cur = tmp_conn.cursor()
                    tmp_cur.execute(f'CREATE DATABASE "{database}"')
                    tmp_cur.close()
                    tmp_conn.close()
                    # try connecting to the newly created database
                    conn = psycopg2.connect(
                        host=host,
                        port=port,
                        database=database,
                        user=user,
                        password=password
                    )
                except Exception:
                    # Re-raise original connect error to preserve behavior if creation failed
                    raise
                finally:
                    if tmp_conn:
                        try:
                            tmp_conn.close()
                        except Exception:
                            pass
            else:
                raise
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
                No_of_Votes BIGINT,
                Gross BIGINT,
                Decade INT,
                Genre_Count INT,
                Stars TEXT[],
                Movie_Length VARCHAR(50),
                Rating_Category VARCHAR(50),
                Popularity_Score FLOAT,
                Gross_Inflation_Adjusted BIGINT
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
            cursor.executemany(insert_query, data_tuples)
            print(f"Inserted or updated {len(data_tuples)} valid records in 'movies' table")
        
        # Insert rejected records
        if not rejected.empty:
            rejected_tuples = [(
                row.get("reason") if isinstance(row, dict) else row.get("reason", None),
                row.to_json()
            ) for _, row in rejected.iterrows()]
            cursor.executemany("INSERT INTO rejected_records (reason, raw_data) VALUES (%s, %s)", rejected_tuples)
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