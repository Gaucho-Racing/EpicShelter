import asyncio
from datetime import datetime
import os
import time
import uuid

from engine.services.s3 import S3Service
from engine.services.parquet import ParquetService
from engine.connectors.singlestore import SingleStoreConnector
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

class Job:
    def __init__(self, source_engine: str, source_host: str, source_port: int, source_user: str, source_password: str, source_database: str, source_table: str, dest_engine: str, dest_host: str, dest_port: int, dest_user: str, dest_password: str, dest_database: str, dest_table: str, s3_bucket: str, s3_access_key_id: str, s3_secret_access_key: str):
        self.job_id = str(uuid.uuid4())
        self.source_engine = source_engine
        self.source_host = source_host
        self.source_port = source_port
        self.source_user = source_user
        self.source_password = source_password
        self.source_database = source_database
        self.source_table = source_table
        self.dest_engine = dest_engine
        self.dest_host = dest_host
        self.dest_port = dest_port
        self.dest_user = dest_user
        self.dest_password = dest_password
        self.dest_database = dest_database
        self.dest_table = dest_table
        self.s3_bucket = s3_bucket
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key        

class JobService:
    def __init__(self, job: Job):
        self.job = job
        self.source = None
        self.dest = None
        self.s3 = None
        self.batch_size = 5000000

    async def process_batch(self, offset: int) -> int:
        # Create a new connector instance for each batch
        batch_source = SingleStoreConnector(
            self.job.source_host,
            int(self.job.source_port),
            self.job.source_user,
            self.job.source_password,
            self.job.source_database
        )
        await batch_source.connect()
        
        batch_num = offset // self.batch_size
        print(f"Processing batch {batch_num} starting at offset {offset:,}")
        
        data = await batch_source.read_table(
            self.job.source_table,
            interval=self.batch_size,
            offset=offset,
        )
        
        parquet_service = ParquetService()
        output_path = f"exports/{self.job.job_id}/{self.job.source_table}_{batch_num}.parquet"
        await parquet_service.dataframe_to_parquet(data, output_path)
        
        self.s3.upload_parquet(output_path, f"epic-shelter/{self.job.job_id}/{self.job.source_table}_{batch_num}.parquet")
        
        # If destination doesn't support parquet ingestion, write directly
        if not hasattr(self.dest, 'ingest_parquet'):
            batch_dest = SingleStoreConnector(
                self.job.dest_host,
                int(self.job.dest_port),
                self.job.dest_user,
                self.job.dest_password,
                self.job.dest_database
            )
            await batch_dest.connect()
            await batch_dest.write_table(self.job.dest_table, data)
            await batch_dest.disconnect()
            
        await batch_source.disconnect()
        print(f"Batch {batch_num} saved to {output_path}")
        return len(data)

    def process_batch_sync(self, offset: int) -> int:
        return asyncio.run(self.process_batch(offset))

    async def run_job(self):
        start_time = time.time()
        self.reset_export_dir()
        await self.initialize_connectors()
        schemas_match = await self.validate_schemas()
        if not schemas_match:
            raise Exception("Source and destination schemas do not match")
        
        total_rows = await self.source.get_row_count(self.job.source_table)
        print(f"Total rows: {total_rows}")

        with ThreadPoolExecutor(max_workers=2 * multiprocessing.cpu_count()) as executor:  # Adjust max_workers as needed
            futures = [
                executor.submit(self.process_batch_sync, offset)
                for offset in range(0, total_rows, self.batch_size)
            ]
            results = [future.result() for future in futures]

        await self.dest.ingest_parquet(self.job.dest_table, f"{self.job.s3_bucket}/epic-shelter/{self.job.job_id}/*.parquet", self.job.s3_access_key_id, self.job.s3_secret_access_key)
        
        row_counts_match = await self.validate_row_counts()
        if not row_counts_match:
            raise Exception("Source and destination row counts do not match")
        
        await self.source.disconnect()
        await self.dest.disconnect()

        end_time = time.time()
        elapsed_time = end_time - start_time
        rows_per_second = total_rows / elapsed_time

        print("Job completed successfully!")
        print("\n=== Export Summary ===")
        print(f"Total time: {elapsed_time:.2f} seconds")
        print(f"Total rows processed: {total_rows:,}")
        print(f"Average processing speed: {rows_per_second:.2f} rows/second")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=====================")

    def reset_export_dir(self):
        if os.path.exists(f"exports/{self.job.job_id}"):
            for file in os.listdir(f"exports/{self.job.job_id}"):
                file_path = os.path.join(f"exports/{self.job.job_id}", file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
        else:
            os.makedirs(f"exports/{self.job.job_id}")

    async def initialize_connectors(self):
        """
        Initialize the source and destination connectors
        """
        if self.job.source_engine == "singlestore":
            self.source = SingleStoreConnector(
                self.job.source_host,
                int(self.job.source_port),
                self.job.source_user,
                self.job.source_password,
                self.job.source_database,
            )
        else:
            raise Exception(f"Unsupported engine: {self.job.source_engine}")
        
        if self.job.dest_engine == "singlestore":
            self.dest = SingleStoreConnector(
                self.job.dest_host, 
                int(self.job.dest_port),
                self.job.dest_user,
                self.job.dest_password, 
                self.job.dest_database, 
            )
        else:
            raise Exception(f"Unsupported engine: {self.job.dest_engine}")

        await self.source.connect()
        await self.dest.connect()

        can_connect_source = await self.source.test_connection()
        can_connect_dest = await self.dest.test_connection()

        if not can_connect_source:
            raise Exception("Failed to connect to source")
        if not can_connect_dest:
            raise Exception("Failed to connect to destination")
        
        self.s3 = S3Service(
            self.job.s3_bucket,
            self.job.s3_access_key_id,
            self.job.s3_secret_access_key
        )

    async def validate_schemas(self) -> bool:
        """
        Validate the schemas of the source and destination tables
        """
        source_schema = await self.source.get_table_schema(self.job.source_table)
        dest_schema = await self.dest.get_table_schema(self.job.dest_table)

        return source_schema == dest_schema
    
    async def validate_row_counts(self) -> bool:
        """
        Validate the row counts of the source and destination tables
        """
        source_row_count = await self.source.get_row_count(self.job.source_table)
        dest_row_count = await self.dest.get_row_count(self.job.dest_table)

        return source_row_count == dest_row_count
    