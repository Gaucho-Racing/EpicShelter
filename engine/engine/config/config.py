from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    __version__ = "2.0.0"

    job_id: str = None
    chunk_id: str = None
    local_dir: str = str(Path.home() / "epic-shelter")
    verbose: bool = False

    # Source Config
    src_engine: str = None
    src_host: str = None
    src_port: int = None
    src_user: str = None
    src_password: str = None
    src_database: str = None
    src_table: str = None

    # Destination Config
    dest_engine: str = None
    dest_host: str = None
    dest_port: int = None
    dest_user: str = None
    dest_password: str = None
    dest_database: str = None
    dest_table: str = None

    # S3 Config
    s3_bucket: str = None
    s3_access_key_id: str = None
    s3_secret_access_key: str = None

@dataclass
class EngineConfig:
    supported_engines = ["singlestore"]