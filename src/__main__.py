import typer

from .cli import Command

app = typer.Typer()


@app.command()
def gen(name: str):
    """Introspects the database tables in the given database and generates pony models"""
    for line in Command().get_output():
        print(line)


if __name__ == "__main__":
    app()
