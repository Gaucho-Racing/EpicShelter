from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pandas as pd

class Connector(ABC):
    
    @staticmethod
    def create_connector(engine: str, host: str, port: int, user: str, password: str, database: str) -> Any:
        
        from .singlestore import SingleStoreConnector

        connector_map = {
            "singlestore": SingleStoreConnector
        }

        connector_class = connector_map.get(engine)
        if not connector_class:
            raise ValueError(f"Unsupported engine: {engine}")

        return connector_class(host, port, user, password, database)

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is working"""
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get schema definition for a specific table"""
        pass

    @abstractmethod
    def get_parquet_schema(self, table_schema: Dict[str, str]) -> Dict[str, str]:
        """Get the equivalent parquet schema for a specific table schema"""
        pass

    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """Get total number of rows in a table"""
        pass

    @abstractmethod
    def get_primary_key_columns(self, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        pass

    @abstractmethod
    def read_table(self, table_name: str, interval: int, offset: int = 0, sort_column: str = "") -> pd.DataFrame:
        """Read data from a table"""
        pass

    @abstractmethod
    def write_table(self, table_name: str, df: pd.DataFrame) -> None:
        """Write data to a table"""
        pass

    @abstractmethod
    def ingest_parquet(self, table_name: str, parquet_path: str, aws_access_key_id: str, aws_secret_access_key: str) -> None:
        """Optional: Ingest parquet files directly into a table"""
        pass

    @abstractmethod
    def delete_table(self, table_name: str) -> None:
        """Delete data from a table"""
        pass
