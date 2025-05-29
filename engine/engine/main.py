from engine.services.validate import validate_config, validate_migration
import typer
from engine.connectors.singlestore import SingleStoreConnector
from engine.services.job import Job, JobService
from engine.config.config import Config
from engine.cli.validate import app as validate_app
from engine.cli.version import app as version_app

app = typer.Typer()
app.add_typer(version_app)
app.add_typer(validate_app)

if __name__ == "__main__":
    app()