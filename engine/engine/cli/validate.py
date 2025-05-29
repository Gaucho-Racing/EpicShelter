from engine.services.validate import validate_config, validate_migration
from engine.config.config import Config
import typer

app = typer.Typer()

@app.command()
def validate(
    src_engine: str = typer.Option(..., "--src-engine", "-se", help="The source engine to connect to"),
    src_host: str = typer.Option(..., "--src-host", "-sh", help="The source host to connect to"),
    src_port: int = typer.Option(..., "--src-port", "-sp", help="The source port to connect to"),
    src_user: str = typer.Option(..., "--src-user", "-su", help="The source user to connect to"),
    src_password: str = typer.Option(..., "--src-password", "-spw", help="The source password to connect to"),
    src_database: str = typer.Option(..., "--src-database", "-sd", help="The source database to connect to"),
    src_table: str = typer.Option(..., "--src-table", "-st", help="The source table to connect to"),
    dest_engine: str = typer.Option(..., "--dest-engine", "-de", help="The destination engine to connect to"),
    dest_host: str = typer.Option(..., "--dest-host", "-dh", help="The destination host to connect to"),
    dest_port: int = typer.Option(..., "--dest-port", "-dp", help="The destination port to connect to"),
    dest_user: str = typer.Option(..., "--dest-user", "-du", help="The destination user to connect to"),
    dest_password: str = typer.Option(..., "--dest-password", "-dpw", help="The destination password to connect to"),
    dest_database: str = typer.Option(..., "--dest-database", "-dd", help="The destination database to connect to"),
    dest_table: str = typer.Option(..., "--dest-table", "-dt", help="The destination table to connect to"),
):
    """Validate the provided configuration and make sure the source and destination tables are compatitible"""
    Config.src_engine = src_engine
    Config.src_host = src_host
    Config.src_port = src_port
    Config.src_user = src_user
    Config.src_password = src_password
    Config.src_database = src_database
    Config.src_table = src_table
    Config.dest_engine = dest_engine
    Config.dest_host = dest_host
    Config.dest_port = dest_port
    Config.dest_user = dest_user
    Config.dest_password = dest_password
    Config.dest_database = dest_database
    Config.dest_table = dest_table
    
    src_connector, dest_connector = validate_config()
    validate_migration(src_connector, dest_connector)