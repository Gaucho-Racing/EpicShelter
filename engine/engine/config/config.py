from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    job_id: str = None
    reset_dest_table: bool = True
    migrate_only: bool = False
    use_s3: bool = False
    local_dir: str = str(Path.home() / "epic-shelter")
    verbose: bool = False