import os
from collections import OrderedDict, defaultdict
from itertools import chain
from typing import DefaultDict, Generator, cast

from attrs import define, field
from pony.orm import db_session
from psycopg2.extensions import connection as Connection

from src.sanitize_column_name import normalize_col_name, str_to_py_identifier

from .base import ColumnInfo, FieldInfo, Introspection, TRelation
from .mysql import Introspection as MysqlIntrospection
from .postgres import Introspection as PostgresIntrospection
from .sqlite import Introspection as SqliteIntrospection
from .utils import import_from_string

INROSPECTION_IMPL = {'postgresql': PostgresIntrospection, 'mysql': MysqlIntrospection, 'sqlite': SqliteIntrospection}


@define
class TRelAttr:
    name: str
    table: str
    cls: str
    reverse: str
    kwargs: dict[str, str] | None = None


@define
class TIntrospection:
    relations: dict[str, TRelation] = field(factory=dict)
    rel_attrs: list[TRelAttr] = field(factory=list)
    description: list[FieldInfo] = field(factory=list)
    unique_columns: list[str] = field(factory=list)
    primary_key_columns: list[str] = field(factory=list)
    constraints: dict[str, ColumnInfo] = field(factory=dict)


@define
class Command:
    KWARGS_ORDER = ['unique', 'nullable', 'default', 'column']
    imports = {'from pony.orm import *'}

    database_import_str: str
    introspection: Introspection = field(init=False)
    field_counters: DefaultDict[tuple[str, str], int] = field(factory=lambda: defaultdict(int))
    relations_counters: DefaultDict[tuple[str, str], int] = field(factory=lambda: defaultdict(int))

    def get_output(self) -> Generator[str, None, None]:
        lines = list(self._get_output())
        yield "# This is an auto-generated module with pony entities."
        yield ''
        yield from self.imports
        yield ''
        yield from lines

    def _make_introspection(self, introspection: Introspection):
        ret: DefaultDict[str, TIntrospection] = defaultdict(TIntrospection)
        cursor = introspection.connection.cursor()
        tables_to_introspect = introspection.table_names(cursor)
        for table_name in tables_to_introspect:
            if table_name in ['migration', 'pony_version']:
                continue
            try:
                ret[table_name].relations = introspection.get_relations(cursor, table_name)
            except NotImplementedError:
                if os.environ.get('DEBUG'):
                    raise
            try:
                ret[table_name].constraints = introspection.get_constraints(cursor, table_name)
            except NotImplementedError:
                if os.environ.get('DEBUG'):
                    raise
            ret[table_name].primary_key_columns = introspection.get_primary_key_columns(cursor, table_name)
            ret[table_name].unique_columns = list(chain.from_iterable((c['columns'] for c in ret[table_name].constraints.values() if c['unique'])))
            ret[table_name].description = introspection.get_table_description(cursor, table_name)
            # normalize field names & increment field name counters
            for field_info in ret[table_name].description:
                if field_info.name in ret[table_name].relations:
                    continue
                field_info.name, _, _ = normalize_col_name(field_info.name)
                field_info.name = f"{field_info.name}{self.field_counters[(table_name, field_info.name)] or ''}"
                self.field_counters[(table_name, field_info.name)] += 1
            # check if it's an m2m table
            related_tables: list[dict[str, str]] = []
            is_m2m = False
            for field_info in ret[table_name].description:
                # relations: {'a_id': ('id', 'm2m_a')}
                if field_info.name not in ret[table_name].relations:
                    if field_info.name in ret[table_name].primary_key_columns:
                        continue
                    else:
                        break
                tblname = ret[table_name].relations[field_info.name].table_ref
                related_tables.append({'table': tblname, 'field': field_info.name})
            else:
                is_m2m = len(set(t['table'] for t in related_tables)) == 2
            if is_m2m:
                # store the relation in the each of related tables
                this, other = related_tables
                this_field, other_field = this['field'], other['field']
                this['field'] = f"{this['field']}_set{self.field_counters[(other['table'], this['field'])] or ''}"
                other['field'] = f"{other['field']}_set{self.field_counters[(this['table'], other['field'])] or ''}"
                self.field_counters[(this['table'], other_field)] += 1
                self.field_counters[(other['table'], this_field)] += 1
                for this, other in ((this, other), (other, this)):
                    ret[this['table']].rel_attrs.append(TRelAttr(name=other['field'],
                                                                 table=other['table'],
                                                                 cls='Set',
                                                                 reverse=this['field']))
                    self.relations_counters[(this['table'], other['table'])] += 1
                continue
            # calculate relation attributes
            for column_name, relation in ret[table_name].relations.items():
                att_name, kwargs, _notes = normalize_col_name(column_name, is_related=True)
                index = self.field_counters[(table_name, att_name)]
                self.field_counters[(table_name, att_name)] += 1
                att_name = f"{att_name}{index or ''}"
                # getting reverse name
                reverse = table_name.lower()
                index = self.field_counters[(relation.table_ref, reverse)]
                self.field_counters[(relation.table_ref, reverse)] += 1
                reverse = f"{reverse}_set{index or ''}"
                ret[table_name].rel_attrs += [TRelAttr(name=att_name, cls='Required', reverse=reverse, table=relation.table_ref, kwargs=kwargs),
                                              TRelAttr(name=reverse, cls='Set', reverse=att_name, table=table_name)]
                self.relations_counters[(table_name, relation.table_ref)] += 1
                self.relations_counters[(relation.table_ref, table_name)] += 1
        return ret

    def _get_output(self):
        # not requires connection?
        db = import_from_string(self.database_import_str)
        with db_session():
            connection = cast(Connection, db.get_connection())
            Introspection = INROSPECTION_IMPL[db.provider.dialect.lower()]
            introspection = Introspection(connection, provider=db.provider)
            all_data = self._make_introspection(introspection)
        yield 'db = Database()'
        for table_name, data in all_data.items():
            model_name = str_to_py_identifier(table_name, case_type='title')
            yield f'class {model_name}(db.Entity):'
            yield f'    _table_ = "{table_name}"'
            column_to_field_name: dict[str, str] = {}
            for row in data.description:
                comment_notes: list[str] = []
                extra_params: OrderedDict[str, str | bool | int] = OrderedDict()  # Holds Field parameters such as 'column'.
                column_name = att_name = row.name
                # used_column_names.append(att_name)
                column_to_field_name[column_name] = att_name
                field_kwargs: dict[str, str | bool | int] = {}
                # Add primary_key and unique, if necessary.
                if [column_name] == data.primary_key_columns:
                    extra_params['primary_key'] = True
                elif column_name in data.unique_columns:
                    field_kwargs['unique'] = True
                is_relation = column_name in data.relations
                # Calling `get_field_type` to get the field type string and any
                # additional parameters and notes.
                field_type = self.get_field_type(introspection, table_name, row)
                if not is_relation:
                    extra_params.update(field_type.params)
                    field_kwargs.update(field_type.params)
                    comment_notes.extend(field_type.notes)
                if att_name == 'id' and extra_params == {'primary_key': True}:
                    if field_type.type == 'AUTO':
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
                    field_desc = f'{att_name} = {cls}({field_type.type}{kwargs_list})'
                    if comment_notes:
                        field_desc += f'  # {" ".join(comment_notes)}'
                    yield f'    {field_desc}'
            rel_attrs = data.rel_attrs
            for attr in rel_attrs:
                model = str_to_py_identifier(attr.table, case_type='title')
                kwargs = attr.kwargs or {}
                if self.relations_counters[(table_name, attr.table)] > 1:
                    kwargs['reverse'] = attr.reverse
                kwargs = [f'{key}={repr(val)}' for key, val in kwargs.items()]
                kwargs = ', '.join(kwargs)
                kwargs = f', {kwargs}' if kwargs else ''
                yield f'''    {attr.name} = {attr.cls}("{model}"{kwargs})'''
            # compound primary key
            if len(data.primary_key_columns) > 1:
                attrs = [column_to_field_name[c.lower()] for c in data.primary_key_columns]
                yield f"    PrimaryKey({', '.join(attrs)})"

    def get_field_type(self, introspection: Introspection, table_name: str, row: FieldInfo):
        """
        Given the database connection, the table name, and the cursor row
        description, this routine will return the given field type name, as
        well as any additional keyword parameters and notes for the field.
        """
        field_params: OrderedDict[str, str | int] = OrderedDict()
        field_notes: list[str] = []
        try:
            field_type, opts, _import = introspection.get_field_type(row.type_code, row)
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
        return FieldType(type=field_type, params=field_params, notes=field_notes)


@define(kw_only=True)
class FieldType:
    type: str
    params: OrderedDict[str, str | int]
    notes: list[str]
