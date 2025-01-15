from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Any
import os
import time

class CompressionType(str, Enum):
    NONE = "none"
    SNAPPY = "snappy" 
    GZIP = "gzip"
    BROTLI = "brotli"
    LZ4 = "lz4"
    ZSTD = "zstd"

class ParquetConfig:
    def __init__(
        self,
        compression: CompressionType = CompressionType.SNAPPY,
        row_group_size: int = 100000,
        enable_statistics: bool = True
    ):
        self.compression = compression
        self.row_group_size = row_group_size
        self.enable_statistics = enable_statistics

@dataclass
class ParquetMetrics:
    num_rows: int
    num_row_groups: int
    num_columns: int
    file_size_bytes: int
    schema: Dict[str, str]
    compression: str
    column_statistics: Dict[str, Dict[str, Any]]
    memory_size_bytes: int
    creation_time: float
    
class ParquetService:
    def __init__(self):
        self.thread_pool = ThreadPoolExecutor()

    async def dataframe_to_parquet(
        self,
        df: pd.DataFrame,
        output_path: str,
        config: Optional[ParquetConfig] = None
    ) -> str:
        if config is None:
            config = ParquetConfig()

        start_time = time.time()
        table = pa.Table.from_pandas(df)
        self._write_parquet(table, output_path, config)
        creation_time = time.time() - start_time
        print(f"Parquet file creation time: {creation_time:.2f} seconds")
        return output_path

    def _write_parquet(
        self,
        table: pa.Table,
        output_path: str,
        config: ParquetConfig
    ) -> None:
        pq.write_table(
            table,
            output_path,
            compression=config.compression.value,
            row_group_size=config.row_group_size,
            write_statistics=config.enable_statistics
        )