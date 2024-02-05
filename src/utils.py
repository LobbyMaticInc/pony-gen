import re
from ast import Assign, Attribute, ClassDef, Constant, Name
from importlib import import_module
from keyword import iskeyword
from typing import Iterable, Literal, cast

import deal
import inflection
from pony.orm.core import Database
from slugify import slugify


def import_from_string(import_str: str):
    module_str, _, attr_str = import_str.partition(':')
    module = import_module(module_str)
    try:
        return cast(Database, getattr(module, attr_str) if attr_str else module)
    except AttributeError:
        raise ImportError(f"Module '{module_str}' does not have a '{attr_str}' attribute.")


@deal.pure
def str_to_py_identifier(input_string: str, *, is_related: bool = False, case_type: Literal['snake', 'camel', 'title', 'const'] = 'snake'):
    sanitized_string = slugify(input_string, separator='_').replace('.', '_')
    if not sanitized_string or not sanitized_string[0].isalpha():
        sanitized_string = f"_{''.join(filter(str.isalnum, sanitized_string))}"
    if iskeyword(sanitized_string):
        sanitized_string = f"{sanitized_string}_"
    match case_type:
        case 'snake':
            return inflection.underscore(sanitized_string)
        case 'camel':
            return inflection.camelize(sanitized_string, False)
        case 'title':
            has_leading_underscore = '_' if sanitized_string.startswith('_') else ''
            title_cased = ''.join(word.capitalize() for word in sanitized_string.split('_'))
            return has_leading_underscore + title_cased
        case 'const':
            return inflection.underscore(sanitized_string).upper()


@deal.pure
def normalize_col_name(col_name: str, is_related=False):
    """
    Modify the column name to make it Python-compatible as a field name
    """
    field_params: dict[str, str] = {}
    field_notes: list[str] = []
    new_name = col_name.lower()
    if new_name != col_name:
        field_notes.append('Field name made lowercase.')
    if is_related and col_name.endswith('_id'):
        new_name = new_name[:-3]
    new_name, num_repl = re.subn(r'\W', '_', new_name)
    if num_repl > 0:
        field_notes.append('Field renamed to remove nonalphanumerics and replace them with underscores.')
    if new_name.startswith('_'):
        new_name = f'attr{new_name}'
        field_notes.append("Field renamed because it started with '_'.")
    if new_name.endswith('_'):
        new_name = f'{new_name}attr'
        field_notes.append("Field renamed because it ended with '_'.")
    if new_name[0].isdigit():
        new_name = f'number_{new_name}'
        field_notes.append("Field renamed because it wasn't a valid Python identifier.")
    if iskeyword(new_name):
        new_name += '_attr'
        field_notes.append('Field renamed because it was a Python reserved word.')
    if not new_name.isidentifier():
        new_name = f'{new_name}_attr'
        field_notes.append('Field renamed to ensure it is a valid Python identifier.')
    if col_name != new_name:
        field_params['column'] = col_name
    return new_name, field_params, field_notes


def create_enum_ast(name: str, values: Iterable[str]):
    trimmed_name = "e_" + "_".join(name.split("_")[:-1])
    enum_name = str_to_py_identifier(trimmed_name, case_type='title')
    return ClassDef(decorator_list=[Attribute(value=Name(id='strawberry'), attr='enum')],
                    name=enum_name,
                    bases=[Name(id='Enum')],
                    keywords=[],
                    body=[Assign(targets=[Name(id=str_to_py_identifier(enum_value, case_type='const'))],
                                 value=Constant(value=(enum_value), kind=None),
                                 lineno=None,
                                 type_comment=None) for enum_value in values])
