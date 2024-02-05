from typing import Any, ClassVar, NamedTuple, NotRequired, TypedDict

from attrs import define
from pony.orm.dbapiprovider import DBAPIProvider
from psycopg2.extensions import connection as Connection
from psycopg2.extensions import cursor as Cursor


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


@define(kw_only=True)
class FieldInfo:
    # Structure returned by the DB-API cursor.description interface (PEP 249)
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
class TRelation:
    field_name_ref: str
    table_ref: str


@define
class Introspection:
    imports: ClassVar[dict[str, str]]
    data_types_reverse: ClassVar[dict[str | int, str]]
    ignored_tables: ClassVar[list[str]]

    connection: Connection
    provider: DBAPIProvider

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

    def get_primary_key_columns(self, cursor: Cursor, table_name: str) -> list[str]:
        """
        Return the name of the primary key column for the given table.
        """
        for constraint in self.get_constraints(cursor, table_name).values():
            if constraint['primary_key']:
                return constraint['columns']
        return []

    def get_table_list(self, cursor: Cursor) -> list[TableInfo]:
        """
        Return an unsorted list of TableInfo named tuples of all tables and
        views that exist in the database.
        """
        ...

    def get_key_columns(self, cursor: Cursor, table_name: str) -> list[tuple[str, str, str]]:
        """
        Backends can override this to return a list of:
            (column_name, referenced_table_name, referenced_column_name)
        for all key columns in given table.
        """
        ...

    def get_constraints(self,  cursor: Cursor, table_name: str) -> dict[str, ColumnInfo]:
        ...

    def get_table_description(self, cursor: Cursor, table_name: str) -> list[FieldInfo]:
        ...

    def get_relations(self, cursor: Cursor, table_name: str) -> dict[str, TRelation]:
        ...

    def get_field_type(self, data_type: int | str, description: FieldInfo) -> tuple[str, dict[str, int] | None, str | None]:
        ...
