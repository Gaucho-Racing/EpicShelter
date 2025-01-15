from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pandas as pd

class Connector(ABC):
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the database"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the database connection"""
        pass

    @abstractmethod
    async def test_connection(self) -> bool:
        """Test if the connection is working"""
        pass

    @abstractmethod
    async def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        pass

    @abstractmethod
    async def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get schema definition for a specific table"""
        pass

    @abstractmethod
    async def get_row_count(self, table_name: str) -> int:
        """Get total number of rows in a table"""
        pass

    @abstractmethod
    async def get_primary_key_columns(self, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        pass

    @abstractmethod
    async def read_table(self, table_name: str, interval: int, offset: int = 0, sort_column: str = "") -> pd.DataFrame:
        """Read data from a table"""
        pass

    @abstractmethod
    async def write_table(self, table_name: str, df: pd.DataFrame) -> None:
        """Write data to a table"""
        pass

    @abstractmethod
    async def ingest_parquet(self, table_name: str, parquet_path: str, aws_access_key_id: str, aws_secret_access_key: str) -> None:
        """Optional: Ingest parquet files directly into a table"""
        pass