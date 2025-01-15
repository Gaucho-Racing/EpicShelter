from typing import Any, Dict, List
import aiomysql
import pandas as pd
import time

from engine.connectors.connector import Connector

class SingleStoreConnector(Connector):
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool = None

    async def connect(self) -> None:
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            autocommit=True
        )

    async def disconnect(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def test_connection(self) -> bool:
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    return True
        except Exception:
            return False

    async def get_tables(self) -> List[str]:
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SHOW TABLES")
                    tables = await cur.fetchall()
                    return [table[0] for table in tables]
        except Exception as e:
            print(f"Error getting tables: {str(e)}")
            return []
        
    async def get_table_schema(self, table_name: str) -> Dict[str, str]:
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"DESCRIBE {table_name}")
                    columns = await cur.fetchall()
                    schema = {}
                    for col in columns:
                        schema[col[0]] = col[1]
                    return schema
        except Exception as e:
            print(f"Error getting schema for table {table_name}: {str(e)}")
            return {}
        
    async def get_row_count(self, table_name: str) -> int:
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = await cur.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            print(f"Error getting row count for table {table_name}: {str(e)}")
            return 0
        
    async def get_primary_key_columns(self, table_name: str) -> List[str]:
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                        WHERE TABLE_SCHEMA = '{self.database}' 
                        AND TABLE_NAME = '{table_name}' 
                        AND CONSTRAINT_NAME = 'PRIMARY'
                        ORDER BY ORDINAL_POSITION
                    """)
                    columns = await cur.fetchall()
                    return [col[0] for col in columns]
        except Exception as e:
            print(f"Error getting primary key columns for table {table_name}: {str(e)}")
            return []

    async def read_table(self, table_name: str, interval: int, offset: int = 0, sort_column: str = "") -> pd.DataFrame:
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Get column names
                    await cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
                    columns = [desc[0] for desc in cur.description]
                    
                    query = f"""
                        SELECT *
                        FROM {table_name}
                    """

                    if sort_column:
                        query += f" ORDER BY {sort_column}"

                    query += f" LIMIT {interval} OFFSET {offset}"

                    query_start = time.time()
                    await cur.execute(query)
                    rows = await cur.fetchall()
                    query_time = time.time() - query_start
                    print(f"Query execution time: {query_time:.2f} seconds")
                    
                    # Create DataFrame directly from rows
                    df_start = time.time()
                    df = pd.DataFrame(rows, columns=columns)
                    df_time = time.time() - df_start
                    print(f"DataFrame conversion time: {df_time:.2f} seconds")
                    
                    total_time = time.time() - start_time
                    print(f"Total execution time: {total_time:.2f} seconds")
                    return df
        except Exception as e:
            print(f"Error reading table {table_name}: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
        
    async def write_table(self, table_name: str, df: pd.DataFrame) -> None:
        if df.empty:
            print(f"Warning: Empty DataFrame provided for table {table_name}")
            return

        try:
            start_time = time.time()
            
            # Get column names and create placeholders for SQL query
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)
            
            # Prepare the insert query
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples for batch insertion
            rows = df.to_records(index=False).tolist()
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Use executemany for batch insertion
                    await cur.executemany(query, rows)
                    
                    total_time = time.time() - start_time
                    print(f"Inserted {len(df)} rows into {table_name} in {total_time:.2f} seconds")
                    
        except Exception as e:
            print(f"Error writing to table {table_name}: {str(e)}")
            raise

    async def ingest_parquet(self, table_name: str, parquet_path: str, aws_access_key_id: str, aws_secret_access_key: str) -> None:
        try:
            # Extract job ID from the last path segment before .parquet
            job_id = parquet_path.split("/*.parquet")[0].split("/")[-1]
            print(f"Starting parquet ingestion for Job ID: {job_id}")
            pipeline_name = f"es_{job_id.replace('-', '_')}_pipeline"
            
            # Get the table schema to map columns
            schema = await self.get_table_schema(table_name)
            if not schema:
                raise Exception(f"Could not get schema for table {table_name}")
            
            # Create column mappings for the pipeline
            column_mappings = []
            for column_name, column_type in schema.items():
                if 'timestamp' in column_type.lower():
                    # Handle timestamp columns specially
                    column_mappings.append(f"@{column_name} <- {column_name}")
                else:
                    column_mappings.append(f"{column_name} <- {column_name}")
            
            # Join column mappings with commas
            column_mapping_str = ",\n    ".join(column_mappings)
            
            # Create the pipeline query
            pipeline_query = f"""
            CREATE OR REPLACE PIPELINE {pipeline_name}
            AS LOAD DATA S3 '{parquet_path}'
            CONFIG '{{"region": "us-west-2"}}'
            CREDENTIALS '{{"aws_access_key_id": "{aws_access_key_id}", "aws_secret_access_key": "{aws_secret_access_key}"}}'
            REPLACE INTO TABLE {table_name}
            FORMAT PARQUET
            (
                {column_mapping_str}
            )
            """
            
            # Add timestamp conversions if needed
            timestamp_sets = []
            for column_name, column_type in schema.items():
                if 'timestamp' in column_type.lower():
                    timestamp_sets.append(
                        f"{column_name} = FROM_UNIXTIME(@{column_name}/1000000)"
                    )
            
            if timestamp_sets:
                pipeline_query += f"\nSET {', '.join(timestamp_sets)};"
            else:
                pipeline_query += ";"

            print(f"Generated pipeline definition for {pipeline_name}")
            print(pipeline_query)
            
            start_time = time.time()
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Create the pipeline
                    await cur.execute(pipeline_query)
                    
                    # Test the pipeline
                    await cur.execute(f"START PIPELINE {pipeline_name} FOREGROUND")
                    
                    elapsed_time = time.time() - start_time
                    print(f"Successfully ingested in {elapsed_time:.2f} seconds")

                    # Drop the pipeline after use
                    await cur.execute(f"DROP PIPELINE {pipeline_name}")
                    
        except Exception as e:
            print(f"Error creating pipeline for table {table_name}: {str(e)}")
            raise