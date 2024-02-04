from typing import Any, NamedTuple, NotRequired, TypedDict

from attrs import define
from pony.orm.dbapiprovider import DBAPIProvider
from psycopg2.extensions import connection as Connection
from psycopg2.extensions import cursor as Cursor

# Structure returned by DatabaseIntrospection.get_table_list()


class TableInfo(NamedTuple):
    name: str
    type: str


class ColumnInfo(TypedDict):
    columns: list[str]
    primary_key: bool
    orders: NotRequired[list[str]]
    unique: bool
    foreign_key: NotRequired[tuple[str, ...] | None]
    check: bool
    index: bool
    definition: NotRequired[None]
    options: NotRequired[Any]


# Structure returned by the DB-API cursor.description interface (PEP 249)

# def FieldInfo(*args):
#     return _FieldInfo(args)


@define(kw_only=True)
class FieldInfo:
    name: str
    type_code: str | int
    display_size: int | None = None
    internal_size: int | None = None
    precision: int | None = None
    scale: int | None = None
    null_ok: bool | None = None
    default: str | None = None
    extra: Any = None
    is_unsigned: bool | None = None


@define
class Introspection:
    connection: Connection
    provider: DBAPIProvider

    def table_name_converter(self, name: str):
        """
        Apply a conversion to the name for the purposes of comparison.

        The default table name converter is for case sensitive comparison.
        """
        return name

    def column_name_converter(self, name: str):
        """
        Apply a conversion to the column name for the purposes of comparison.

        Use table_name_converter() by default.
        """
        return self.table_name_converter(name)

    def table_names(self, cursor: Cursor | None = None, include_views=False) -> list[str]:
        """
        Return a list of names of all tables that exist in the database.
        Sort the returned table list by Python's default sorting. Do NOT use
        the database's ORDER BY here to avoid subtle differences in sorting
        order between databases.
        """
        def get_names(cursor: Cursor) -> list[str]:
            return sorted(ti.name for ti in self.get_table_list(cursor) if include_views or ti.type == 't')
        if cursor is None:
            with self.connection.cursor() as cursor:
                return get_names(cursor)
        return get_names(cursor)

    def get_table_list(self, cursor: Cursor) -> list[TableInfo]:
        """
        Return an unsorted list of TableInfo named tuples of all tables and
        views that exist in the database.
        """
        raise NotImplementedError('subclasses of BaseDatabaseIntrospection may require a get_table_list() method')

    # def sequence_list(self):
    #     """
    #     Return a list of information about all DB sequences for all models in
    #     all apps.
    #     """
    #     from django.apps import apps
    #     from django.db import models, router

    #     sequence_list = []

    #     for app_config in apps.get_app_configs():
    #         for model in router.get_migratable_models(app_config, self.connection.alias):
    #             if not model._meta.managed:
    #                 continue
    #             if model._meta.swapped:
    #                 continue
    #             for f in model._meta.local_fields:
    #                 if isinstance(f, models.AutoField):
    #                     sequence_list.append({'table': model._meta.db_table, 'column': f.column})
    #                     break  # Only one AutoField is allowed per model, so don't bother continuing.

    #             for f in model._meta.local_many_to_many:
    #                 # If this is an m2m using an intermediate table,
    #                 # we don't need to reset the sequence.
    #                 if f.remote_field.through is None:
    #                     sequence_list.append({'table': f.m2m_db_table(), 'column': None})

    #     return sequence_list

    # def get_key_columns(self, cursor, table_name):
    #     """
    #     Backends can override this to return a list of:
    #         (column_name, referenced_table_name, referenced_column_name)
    #     for all key columns in given table.
    #     """
    #     raise NotImplementedError('subclasses of BaseDatabaseIntrospection may require a get_key_columns() method')

    def get_constraints(self,  cursor: Cursor, table_name: str) -> dict[str, ColumnInfo]:
        ...

    def get_primary_key_columns(self, cursor: Cursor, table_name: str) -> list[str]:
        """
        Return the name of the primary key column for the given table.
        """
        for constraint in self.get_constraints(cursor, table_name).values():
            if constraint['primary_key']:
                return constraint['columns']
        return []
