import re
from ast import Module, parse, unparse
from pathlib import Path
from typing import Annotated

import typer

from .cli import Command

app = typer.Typer()


def validate_database_import_str(value: str) -> str:
    if not re.match(r"^[^.]+(\.[^:]+)+:[^:]+$", value):
        raise typer.BadParameter("Value must be in the format 'app.database:db'")
    return value


database_import_str_help = "Pony Database instance import string in the format 'app.path.to.file:db_var_name'. "
out_file_help = "Output file path. Defaults to 'generated_pony_models.py' in cwd."


@app.command()
def gen(database_import_str: Annotated[str, typer.Argument(help=database_import_str_help, callback=validate_database_import_str)],
        out_file: Annotated[str, typer.Option("-o", "--out",  help=out_file_help)] = "generated_pony_models"):
    """Introspects the database tables in the given database and generates pony models"""
    with Path(f"{out_file}.py").open("w") as file:
        _ = file.write(unparse(Module(type_ignores=[], body=list(Command(database_import_str).get_output()))))
