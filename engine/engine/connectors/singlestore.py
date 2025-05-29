from typing import Any, Dict, List
import pymysql
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
        self.connection = None

    def connect(self) -> None:
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            autocommit=True
        )

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()

    def test_connection(self) -> bool:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1")
                return True
        except Exception:
            return False

    def get_tables(self) -> List[str]:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SHOW TABLES")
                tables = cur.fetchall()
                return [table[0] for table in tables]
        except Exception as e:
            print(f"Error getting tables: {str(e)}")
            return []
        
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        try:
            with self.connection.cursor() as cur:
                cur.execute(f"DESCRIBE `{table_name}`")
                columns = cur.fetchall()
                schema = {}
                for col in columns:
                    schema[col[0]] = col[1]
                return schema
        except Exception as e:
            print(f"Error getting schema for table {table_name}: {str(e)}")
            return {}
        
    def get_parquet_schema(self, table_schema: Dict[str, str]) -> Dict[str, str]:
        parquet_schema = {}
        
        for column_name, db_type in table_schema.items():
            # Convert to lowercase for easier matching
            db_type_lower = db_type.lower()
            
            # Integer types
            if any(int_type in db_type_lower for int_type in ['tinyint', 'smallint', 'mediumint', 'int', 'integer']):
                if 'unsigned' in db_type_lower:
                    parquet_schema[column_name] = 'uint64'
                else:
                    parquet_schema[column_name] = 'int64'
            
            # Big integer types
            elif 'bigint' in db_type_lower:
                if 'unsigned' in db_type_lower:
                    parquet_schema[column_name] = 'uint64'
                else:
                    parquet_schema[column_name] = 'int64'
            
            # Floating point types
            elif any(float_type in db_type_lower for float_type in ['float', 'double', 'real']):
                parquet_schema[column_name] = 'double'
            
            # Decimal/Numeric types
            elif any(decimal_type in db_type_lower for decimal_type in ['decimal', 'numeric']):
                parquet_schema[column_name] = 'double'
            
            # Boolean types
            elif any(bool_type in db_type_lower for bool_type in ['bool', 'boolean']):
                parquet_schema[column_name] = 'boolean'
            
            # Date and time types
            elif 'timestamp' in db_type_lower:
                parquet_schema[column_name] = 'timestamp[us]'
            elif 'datetime' in db_type_lower:
                parquet_schema[column_name] = 'timestamp[us]'
            elif 'date' in db_type_lower:
                parquet_schema[column_name] = 'date32'
            elif 'time' in db_type_lower:
                parquet_schema[column_name] = 'time64[us]'
            
            # Binary types
            elif any(binary_type in db_type_lower for binary_type in ['binary', 'varbinary', 'blob']):
                parquet_schema[column_name] = 'binary'
            
            # String types
            elif any(string_type in db_type_lower for string_type in ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext']):
                parquet_schema[column_name] = 'string'
            
            # Unsupported types (default case)
            else:
                parquet_schema[column_name] = 'unsupported'
        
        return parquet_schema

    def get_row_count(self, table_name: str) -> int:
        try:
            with self.connection.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            print(f"Error getting row count for table {table_name}: {str(e)}")
            return 0
        
    def get_primary_key_columns(self, table_name: str) -> List[str]:
        try:
            with self.connection.cursor() as cur:
                cur.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                    WHERE TABLE_SCHEMA = '{self.database}' 
                    AND TABLE_NAME = '{table_name}' 
                    AND CONSTRAINT_NAME = 'PRIMARY'
                    ORDER BY ORDINAL_POSITION
                """)
                columns = cur.fetchall()
                return [col[0] for col in columns]
        except Exception as e:
            print(f"Error getting primary key columns for table {table_name}: {str(e)}")
            return []

    def read_table(self, table_name: str, interval: int, offset: int = 0, sort_column: str = "") -> pd.DataFrame:
        try:
            start_time = time.time()
            
            with self.connection.cursor() as cur:
                # Get column names
                cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
                columns = [desc[0] for desc in cur.description]
                
                query = f"""
                    SELECT *
                    FROM {table_name}
                """

                if sort_column:
                    query += f" ORDER BY {sort_column}"

                query += f" LIMIT {interval} OFFSET {offset}"

                query_start = time.time()
                cur.execute(query)
                rows = cur.fetchall()
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
        
    def write_table(self, table_name: str, df: pd.DataFrame) -> None:
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
            
            with self.connection.cursor() as cur:
                # Use executemany for batch insertion
                cur.executemany(query, rows)
                
                total_time = time.time() - start_time
                print(f"Inserted {len(df)} rows into {table_name} in {total_time:.2f} seconds")
                
        except Exception as e:
            print(f"Error writing to table {table_name}: {str(e)}")
            raise

    def ingest_parquet(self, table_name: str, parquet_path: str, aws_access_key_id: str, aws_secret_access_key: str) -> None:
        try:
            # Extract job ID from the last path segment before .parquet
            job_id = parquet_path.split("/*.parquet")[0].split("/")[-1]
            print(f"Starting parquet ingestion for Job ID: {job_id}")
            pipeline_name = f"es_{job_id.replace('-', '_')}_pipeline"
            
            # Get the table schema to map columns
            schema = self.get_table_schema(table_name)
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
            with self.connection.cursor() as cur:
                # Create the pipeline
                cur.execute(pipeline_query)
                
                # Test the pipeline
                cur.execute(f"START PIPELINE {pipeline_name} FOREGROUND")
                
                elapsed_time = time.time() - start_time
                print(f"Successfully ingested in {elapsed_time:.2f} seconds")

                # Drop the pipeline after use
                cur.execute(f"DROP PIPELINE {pipeline_name}")
                
        except Exception as e:
            print(f"Error creating pipeline for table {table_name}: {str(e)}")
            raise

    def delete_table(self, table_name: str) -> None:
        try:
            with self.connection.cursor() as cur:
                delete_query = f"DELETE FROM {table_name}"
                
                cur.execute(delete_query)
                
        except Exception as e:
            print(f"Error deleting from table {table_name}: {str(e)}")
            raise