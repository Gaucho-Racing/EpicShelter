import typer
from engine.connectors.connector import Connector
from engine.config.config import Config, EngineConfig
from typing import Tuple
from rich import print

def validate_config() -> Tuple[Connector, Connector]:
    if Config.src_engine not in EngineConfig.supported_engines:
        print(f"Unsupported source engine: {Config.src_engine}")
        raise typer.Exit(1)
    if Config.dest_engine not in EngineConfig.supported_engines:
        print(f"Unsupported destination engine: {Config.dest_engine}")
        raise typer.Exit(1)
    
    src_connector = Connector.create_connector(
        engine=Config.src_engine,
        host=Config.src_host,
        port=Config.src_port,
        user=Config.src_user,
        password=Config.src_password,
        database=Config.src_database
    )
    src_connector.connect()
    if not src_connector.test_connection():
        print(f"Failed to connect to source database: {Config.src_database}")
        raise typer.Exit(2)
    
    dst_connector = Connector.create_connector(
        engine=Config.dest_engine,
        host=Config.dest_host,
        port=Config.dest_port,
        user=Config.dest_user,
        password=Config.dest_password,
        database=Config.dest_database
    )
    dst_connector.connect()
    if not dst_connector.test_connection():
        print(f"Failed to connect to destination database: {Config.dest_database}")
        raise typer.Exit(2)
    
    print("Established connections to source and destination databases!")
    return src_connector, dst_connector

def validate_migration(src_connector: Connector, dest_connector: Connector):
    """Validate the migration of a table from the source to the destination"""
    src_table_schema = src_connector.get_table_schema(Config.src_table)
    dest_table_schema = dest_connector.get_table_schema(Config.dest_table)

    if not src_table_schema:
        print(f"Source table {Config.src_table} not found")
        raise typer.Exit(3)
    if not dest_table_schema:
        print(f"Destination table {Config.dest_table} not found")
        raise typer.Exit(3)

    src_parquet_schema = src_connector.get_parquet_schema(src_table_schema)
    dest_parquet_schema = dest_connector.get_parquet_schema(dest_table_schema)
    print(src_parquet_schema)
    print(dest_parquet_schema)
    for column_name, parquet_type in src_parquet_schema.items():
        if parquet_type == 'unsupported':
            print(f"Unsupported column type: {column_name}")
            raise typer.Exit(3)
    for column_name, parquet_type in dest_parquet_schema.items():
        if parquet_type == 'unsupported':
            print(f"Unsupported column type: {column_name}")

            raise typer.Exit(3)
    
    # Verify source and destination columns match in name and type
    for column_name, src_type in src_parquet_schema.items():
        if column_name not in dest_parquet_schema:
            print(f"Column {column_name} exists in source but not in destination")
            raise typer.Exit(3)
        dest_type = dest_parquet_schema[column_name]
        if src_type != dest_type:
            print(f"Column {column_name} type mismatch: source={src_type}, destination={dest_type}")
            raise typer.Exit(3)
    
    # Check for extra columns in destination
    for column_name in dest_parquet_schema:
        if column_name not in src_parquet_schema:
            print(f"Column {column_name} exists in destination but not in source")
            raise typer.Exit(3)
        
    print("Table schemas are compatible!")