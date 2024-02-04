from importlib import import_module
from typing import cast

from pony.orm.core import Database


def import_from_string(import_str: str):
    module_str, _, attr_str = import_str.partition(':')
    module = import_module(module_str)
    try:
        return cast(Database, getattr(module, attr_str) if attr_str else module)
    except AttributeError:
        raise ImportError(f"Module '{module_str}' does not have a '{attr_str}' attribute.")
