from engine.config.config import Config
import typer

app = typer.Typer()

@app.command()
def version():
    """Show the version of the currently installed engine"""
    typer.echo(f"Epic Shelter Engine v{Config.__version__}")