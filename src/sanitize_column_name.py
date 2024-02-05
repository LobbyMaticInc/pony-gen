import re
from keyword import iskeyword
from typing import Literal

import deal
import inflection
from slugify import slugify


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
