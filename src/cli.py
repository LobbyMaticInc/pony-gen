from collections import OrderedDict, defaultdict
from itertools import chain
from typing import DefaultDict, Generator, cast

from attrs import define, field
from pony.orm import db_session
from psycopg2.extensions import connection as Connection

from src.base import ColumnInfo, FieldInfo, Introspection, TRelation
from src.mysql import Introspection as MysqlIntrospection
from src.postgres import Introspection as PostgresIntrospection
from src.sanitize_column_name import normalize_col_name, str_to_py_identifier
from src.sqlite import Introspection as SqliteIntrospection
from src.utils import import_from_string

INROSPECTION_IMPL = {'postgresql': PostgresIntrospection, 'mysql': MysqlIntrospection, 'sqlite': SqliteIntrospection}


@define
class RelatedTable:
    table: str
    field: str


@define(kw_only=True)
class FieldType:
    type: str
    params: OrderedDict[str, str | int]
    notes: list[str]


@define
class TRelAttr:
    name: str
    table: str
    cls: str
    reverse: str
    kwargs: dict[str, str] = field(factory=dict)


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
    imports = {'from pony.orm import Required, Optional, PrimaryKey, Database'}

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
        cursor = introspection.connection.cursor()
        table_names = introspection.table_names(cursor)
        ret = {table_name: TIntrospection(relations=introspection.get_relations(cursor, table_name),
                                          constraints=(constraints := introspection.get_constraints(cursor, table_name)),
                                          primary_key_columns=introspection.get_primary_key_columns(cursor, table_name),
                                          unique_columns=list(chain.from_iterable((c['columns'] for c in constraints.values() if c['unique']))),
                                          description=introspection.get_table_description(cursor, table_name)) for table_name in table_names}
        for table_name in table_names:
            # normalize field names & increment field name counters
            for field_info in ret[table_name].description:
                if field_info.name in ret[table_name].relations:
                    continue
                field_info.name, _kwargs, _notes = normalize_col_name(field_info.name)
                field_info.name = f"{field_info.name}{self.field_counters[(table_name, field_info.name)] or ''}"
                self.field_counters[(table_name, field_info.name)] += 1
            # check if it's an m2m table
            is_m2m = False
            related_tables: list[RelatedTable] = []
            for field_info in ret[table_name].description:
                if field_info.name in ret[table_name].relations and field_info.name not in ret[table_name].primary_key_columns:
                    related_tables.append(RelatedTable(table=ret[table_name].relations[field_info.name].table_ref, field=field_info.name))
            else:
                is_m2m = len(set(related_table.table for related_table in related_tables)) == 2
            if is_m2m:
                this, that = related_tables
                this_field, that_field = this.field, that.field
                this.field = f"{this.field}_set{self.field_counters[(that.table, this.field)] or ''}"
                that.field = f"{that.field}_set{self.field_counters[(this.table, that.field)] or ''}"
                self.field_counters[(this.table, that_field)] += 1
                self.field_counters[(that.table, this_field)] += 1
                for this, that in ((this, that), (that, this)):
                    ret[this.table].rel_attrs.append(TRelAttr(name=that.field, table=that.table, cls='Set', reverse=this.field))
                    self.relations_counters[(this.table, that.table)] += 1
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
            for row in data.description:
                comment_notes: list[str] = []
                extra_params: OrderedDict[str, str | bool | int] = OrderedDict()  # Holds Field parameters such as 'column'.
                field_kwargs: dict[str, str | bool | int] = {}
                if [row.name] == data.primary_key_columns:
                    extra_params['primary_key'] = True
                elif row.name in data.unique_columns:
                    field_kwargs['unique'] = True
                field_type = self.get_field_type(introspection, table_name, row)
                if row.name not in data.relations:
                    extra_params.update(field_type.params)
                    field_kwargs.update(field_type.params)
                    comment_notes.extend(field_type.notes)
                if row.name == 'id' and extra_params == {'primary_key': True} and field_type.type == 'AUTO':
                    continue
                if row.null_ok:
                    extra_params['null'] = True
                if row.name not in data.relations:
                    cls = 'PrimaryKey' if extra_params.get('primary_key') else 'Optional' if extra_params.get('null') else 'Required'
                    attr_name, kwargs, notes = normalize_col_name(row.name)
                    comment_notes += notes
                    field_kwargs.update(kwargs)
                    ordered_kwargs = OrderedDict((key, repr(field_kwargs[key])) for key in self.KWARGS_ORDER if key in field_kwargs)
                    if (kwargs_list := ', '.join(f'{key}={val}' for key, val in ordered_kwargs.items())):
                        kwargs_list = f', {kwargs_list}'
                    field_desc = f'{attr_name} = {cls}({field_type.type}{kwargs_list})'
                    if comment_notes:
                        field_desc += '  # ' + " ".join(comment_notes)
                    yield f'    {field_desc}'
            for attr in data.rel_attrs:
                model = str_to_py_identifier(attr.table, case_type='title')
                if self.relations_counters[(table_name, attr.table)] > 1:
                    attr.kwargs['reverse'] = attr.reverse
                kwargs = [f'{key}={repr(val)}' for key, val in attr.kwargs.items()]
                kwargs = ', '.join(kwargs)
                kwargs = f', {kwargs}' if kwargs else ''
                yield f'''    {attr.name} = {attr.cls}("{model}"{kwargs})'''
            if len(data.primary_key_columns) > 1:
                attrs = [c.lower() for c in data.primary_key_columns]
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
            field_type, opts, import_str = introspection.get_field_type(row.type_code, row)
        except KeyError:
            field_type = 'LongStr'
            import_str = 'from pony.orm.ormtypes import LongStr'
            field_notes.append('This field type is a guess.')
            opts = None
        if import_str:
            self.imports.add(import_str)
        if opts:
            field_params.update(opts)
        if field_type == 'str' and row.internal_size and (max_length := int(row.internal_size)) != -1:
            field_params['max_len'] = max_length
        if field_type == 'Decimal':
            if row.precision is None or row.scale is None:
                field_notes.append('scale and/or precision have been guessed, as this database handles decimal fields as float')
            field_params['precision'] = row.precision if row.precision is not None else 10
            field_params['scale'] = row.scale if row.scale is not None else 5
        return FieldType(type=field_type, params=field_params, notes=field_notes)
