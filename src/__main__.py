import re
from typing import Annotated

import typer

from .cli import Command

app = typer.Typer()


def validate_database_import_str(value: str) -> str:
    if not re.match(r"^[^.]+(\.[^:]+)+:[^:]+$", value):
        raise typer.BadParameter("Value must be in the format 'app.database:db'")
    return value


@app.command()
def gen(database_import_str: Annotated[str, typer.Argument(help="Pony Database instance import string in the format 'app.path.to.file:db_var_name'. ",
                                                           callback=validate_database_import_str)]):
    """Introspects the database tables in the given database and generates pony models"""
    for line in Command(database_import_str).get_output():
        print(line)


if __name__ == "__main__":
    app()
