import os
import re
from collections import OrderedDict, defaultdict
from itertools import chain
from typing import DefaultDict, Generator, NotRequired, TypedDict, cast

from attrs import define
from pony.utils import cached_property
from psycopg2.extensions import connection as Connection

from src.sanitize_column_name import normalize_col_name

from .base import ColumnInfo, FieldInfo
from .mysql import Introspection as MysqlIntrospection
from .postgres import Introspection as PostgresIntrospection
from .sqlite import Introspection as SqliteIntrospection
from .utils import import_from_string

INROSPECTION_IMPL = {'postgresql': PostgresIntrospection, 'mysql': MysqlIntrospection, 'sqlite': SqliteIntrospection}


class TRelAttr(TypedDict):
    name: str
    table: str
    cls: str
    reverse: str
    kwargs: NotRequired[dict[str, str]]


class TIntrospection(TypedDict):
    relations: dict[str, tuple[str, str]]
    rel_attrs: list[TRelAttr]
    description: list[FieldInfo]
    unique_columns: list[str]
    primary_key_columns: list[str]
    constraints: dict[str, ColumnInfo]


@define
class Command:
    KWARGS_ORDER = ['unique', 'nullable', 'default', 'column']
    imports = {'from pony.orm import *'}

    def get_output(self) -> Generator[str, None, None]:
        lines = list(self._get_output())
        yield "# This is an auto-generated module with pony entities."
        yield ''
        yield from self.imports
        yield ''
        yield from lines

    def table2model(self, table_name: str):
        return re.sub(r'[^a-zA-Z0-9]', '', table_name.title())

    def is_pony_table(self, table: str):
        return table in ['migration', 'pony_version']

    @cached_property
    def field_counters(self) -> DefaultDict[str, int]:
        return defaultdict(int)

    @cached_property
    def relations_counters(self) -> DefaultDict[str, int]:
        return defaultdict(int)

    def _make_introspection(self):
        database = ""
        db = import_from_string(database)
        connection = cast(Connection, db.get_connection())
        Introspection = INROSPECTION_IMPL[db.provider.dialect.lower()]
        introspection = Introspection(connection, provider=db.provider)
        self.introspection = introspection
        ret: DefaultDict[str, TIntrospection] = defaultdict(lambda: {"relations": {},
                                                                     "rel_attrs": [],
                                                                     "description": [],
                                                                     "unique_columns": [],
                                                                     "primary_key_columns": [],
                                                                     "constraints": {}})
        # FIXME
        cursor = connection.cursor()
        tables_to_introspect = introspection.table_names(cursor)
        counters = self.field_counters
        rel_counters = self.relations_counters
        for table_name in tables_to_introspect:
            if self.is_pony_table(table_name):
                continue
            try:
                relations = introspection.get_relations(cursor, table_name)
            except NotImplementedError:
                if os.environ.get('DEBUG'):
                    raise
                relations = {}
            try:
                constraints = introspection.get_constraints(cursor, table_name)
            except NotImplementedError:
                if os.environ.get('DEBUG'):
                    raise
                constraints = {}
            primary_key_columns = introspection.get_primary_key_columns(cursor, table_name)
            unique_columns = chain.from_iterable((c['columns'] for c in constraints.values() if c['unique']))
            table_description = introspection.get_table_description(cursor, table_name)
            # normalize field names & increment field name counters
            for field in table_description:
                if field.name in relations:
                    continue
                field.name, _, _ = normalize_col_name(field.name)
                field.name = f"{field.name}{counters[(table_name, field.name)] or ''}"
                counters[(table_name, field.name)] += 1
            # check if it's an m2m table
            related_tables: list[dict[str, str]] = []
            is_m2m = False
            for field in table_description:
                # relations: {'a_id': ('id', 'm2m_a')}
                if field.name not in relations:
                    if field.name in primary_key_columns:
                        continue
                    else:
                        break
                tblname = relations[field.name][1]
                related_tables.append({'table': tblname, 'field': field.name})
            else:
                is_m2m = len(set(t['table'] for t in related_tables)) == 2
            if is_m2m:
                # store the relation in the each of related tables
                this, other = related_tables
                this_field, other_field = this['field'], other['field']
                this['field'] = f"{this['field']}_set{counters[(other['table'], this['field'])] or ''}"
                other['field'] = f"{other['field']}_set{counters[(this['table'], other['field'])] or ''}"
                counters[(this['table'], other_field)] += 1
                counters[(other['table'], this_field)] += 1
                for this, other in ((this, other), (other, this)):
                    ret[this['table']]['rel_attrs'].append({'name': other['field'],
                                                            'table': other['table'],
                                                            'cls': 'Set',
                                                            'reverse': this['field']})
                    rel_counters[(this['table'], other['table'])] += 1
                continue
            # calculate relation attributes
            for column_name, (_attr, ref_table) in relations.items():
                att_name, kwargs, _notes = normalize_col_name(column_name, is_related=True)
                index = counters[(table_name, att_name)]
                counters[(table_name, att_name)] += 1
                att_name = f"{att_name}{index or ''}"
                rel_attrs = ret[table_name]["rel_attrs"]
                # getting reverse name
                reverse = table_name.lower()
                index = counters[(ref_table, reverse)]
                counters[(ref_table, reverse)] += 1
                reverse = f"{reverse}_set{index or ''}"
                rel_attrs.append({'name': att_name, 'cls': 'Required', 'reverse': reverse, 'table': ref_table, 'kwargs': kwargs})
                rel_counters[(table_name, ref_table)] += 1
                rel_attrs = ret[ref_table]['rel_attrs']
                rel_attrs.append({'name': reverse, 'cls': 'Set', 'reverse': att_name, 'table': table_name})
                rel_counters[(ref_table, table_name)] += 1
            table_intro = ret[table_name]
            table_intro["relations"] = relations
            table_intro["description"] = table_description
            table_intro["unique_columns"] = list(unique_columns)
            table_intro["primary_key_columns"] = primary_key_columns
            table_intro["constraints"] = constraints
        return ret

    def _get_output(self):
        # not requires connection?
        all_data = self._make_introspection()
        table2model = self.table2model
        yield 'db = Database()'
        known_models: list[str] = []
        for table_name, data in all_data.items():
            relations = data['relations']
            primary_key_columns = data['primary_key_columns']
            unique_columns = data['unique_columns']
            table_description = data['description']
            yield ''
            yield ''
            model_name = table2model(table_name)
            yield f'class {model_name}(db.Entity):'
            yield f'    _table_ = "{table_name}"'
            known_models.append(model_name)
            # used_column_names = []  # Holds column names used in the table so far
            column_to_field_name: dict[str, str] = {}  # Maps column names to names of model fields
            for row in table_description:
                comment_notes: list[str] = []  # Holds Field notes, to be displayed in a Python comment.
                extra_params: OrderedDict[str, str | bool | int] = OrderedDict()  # Holds Field parameters such as 'column'.
                column_name = att_name = row.name
                # used_column_names.append(att_name)
                column_to_field_name[column_name] = att_name
                field_kwargs: dict[str, str | bool | int] = {}
                # Add primary_key and unique, if necessary.
                if [column_name] == primary_key_columns:
                    extra_params['primary_key'] = True
                elif column_name in unique_columns:
                    field_kwargs['unique'] = True
                is_relation = column_name in relations
                # Calling `get_field_type` to get the field type string and any
                # additional parameters and notes.
                field_type, field_params, field_notes = self.get_field_type(table_name, row)
                if not is_relation:
                    extra_params.update(field_params)
                    field_kwargs.update(field_params)
                    comment_notes.extend(field_notes)
                if att_name == 'id' and extra_params == {'primary_key': True}:
                    if field_type == 'AUTO':
                        continue
                if row.null_ok:  # If it's NULL...
                    extra_params['null'] = True

                def sort_key(item: tuple[str, str | bool | int]):
                    key, _ = item
                    try:
                        index = self.KWARGS_ORDER.index(key)
                    except ValueError:
                        return len(field_kwargs)
                    return index
                sorted_kwargs = sorted(field_kwargs.items(), key=sort_key)
                kwargs_list = [f'{key}={repr(val)}' for key, val in sorted_kwargs]
                kwargs_list = ', '.join(kwargs_list)
                if kwargs_list:
                    kwargs_list = f', {kwargs_list}'
                if not is_relation:
                    if extra_params.get('primary_key'):
                        cls = 'PrimaryKey'
                    elif extra_params.get('null'):
                        cls = 'Optional'
                    else:
                        cls = 'Required'
                    field_desc = f'{att_name} = {cls}({field_type}{kwargs_list})'
                    if comment_notes:
                        field_desc += f'  # {" ".join(comment_notes)}'
                    yield f'    {field_desc}'
            rel_attrs = data.get('rel_attrs', ())
            for attr in rel_attrs:
                model = self.table2model(attr['table'])
                kwargs = attr.get('kwargs', {})
                if self.relations_counters[(table_name, attr['table'])] > 1:
                    kwargs['reverse'] = attr['reverse']
                kwargs = [f'{key}={repr(val)}' for key, val in kwargs.items()]
                kwargs = ', '.join(kwargs)
                kwargs = f', {kwargs}' if kwargs else ''
                yield f'''    {attr['name']} = {attr['cls']}("{model}"{kwargs})'''
            # compound primary key
            if len(primary_key_columns) > 1:
                attrs = [column_to_field_name[c] for c in primary_key_columns]
                yield f"    PrimaryKey({', '.join(attrs)})"

    def get_field_type(self, table_name: str, row: FieldInfo):
        """
        Given the database connection, the table name, and the cursor row
        description, this routine will return the given field type name, as
        well as any additional keyword parameters and notes for the field.
        """
        field_params: OrderedDict[str, str | int] = OrderedDict()
        field_notes: list[str] = []
        try:
            field_type, opts, _import = self.introspection.get_field_type(row.type_code, row)
        except KeyError:
            field_type = 'LongStr'
            _import = 'from pony.orm.ormtypes import LongStr'
            field_notes.append('This field type is a guess.')
            opts = None
        if _import:
            self.imports.add(_import)
        if opts:
            field_params.update(opts)
        # This is a hook for data_types_reverse to return a tuple of
        # (field_type, field_params_dict).
        # if type(field_type) is tuple:
        #     field_type, new_params = field_type
        #     field_params.update(new_params)
        # Add max_length for all str fields.
        if field_type == 'str' and row.internal_size:
            max_length = int(row.internal_size)
            if max_length != -1:
                field_params['max_len'] = max_length
        if field_type == 'Decimal':
            if row.precision is None or row.scale is None:
                field_notes.append('scale and precision have been guessed, as this database handles decimal fields as float')
                field_params['precision'] = row.precision if row.precision is not None else 10
                field_params['scale'] = row.scale if row.scale is not None else 5
            else:
                field_params['precision'] = row.precision
                field_params['scale'] = row.scale
        return field_type, field_params, field_notes
