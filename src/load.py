# import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
import psycopg2
from psycopg2 import OperationalError
from logger import logger


def load(
    input: SparkDataFrame,
    rejected: SparkDataFrame,
    host: str = "localhost",
    port: int = 5432,
    database: str = "etl_pipeline",
    user: str = "postgres",
    password: str = "postgres"
) -> None:
    conn = None
    try:
        def _tryConnect():
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )
                return conn
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
                        return conn
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
                
        conn = _tryConnect()
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
            CREATE TABLE IF NOT EXISTS rejected (
                id SERIAL PRIMARY KEY,
                reason VARCHAR(500),
                raw_data JSONB
            );
        """)
        
        cursor.close()
        
        # Insert valid records
        cols = [col for col in input.columns if col != 'id']
        def _upsertPartition(rows):
            tempCon = _tryConnect()
            cursor = tempCon.cursor()
            data_tuples = [tuple(getattr(row, col) for col in cols) for row in rows]
            cols_str = ', '.join(cols)
            placeholders = ', '.join(['%s'] * len(cols))

            update_cols = [col for col in cols if col != 'Series_Title']
            set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])

            insert_query = f"INSERT INTO movies ({cols_str}) VALUES ({placeholders}) ON CONFLICT (Series_Title) DO UPDATE SET {set_clause}"
            cursor.executemany(insert_query, data_tuples)
            logger.info(f"Inserted or updated {len(data_tuples)} valid records in 'movies' table")
            
            tempCon.commit()
            tempCon.close()
        if not input.isEmpty():
            input.foreachPartition(_upsertPartition)
        
        # Insert rejected records
        def _upsertRejectedPartition(rows):
            tempCon = _tryConnect()
            cursor = tempCon.cursor()
            rejected_tuples = []
            for row in rows:
                raw_data = row.json() if hasattr(row, 'json') else str(row)
                rejected_tuples.append((row['reason'], raw_data))
            cursor.executemany("INSERT INTO rejected (reason, raw_data) VALUES (%s, %s)", rejected_tuples)
            logger.info(f"Inserted {len(rejected_tuples)} rejected records into 'rejected' table")
            
            tempCon.commit()
            tempCon.close()
        if not rejected.isEmpty():
            rejected.foreachPartition(_upsertRejectedPartition)
            
        
        conn.commit()
        logger.info(f"load.completed inserted={input.count()} rejected={rejected.count()}")
        
    except psycopg2.Error as e:
        logger.info(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()