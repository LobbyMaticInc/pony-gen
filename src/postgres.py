from typing import cast

from psycopg2.extensions import cursor as Cursor
from typing_extensions import override

from .base import ColumnInfo, FieldInfo
from .base import Introspection as BaseIntrospection
from .base import TableInfo


class Introspection(BaseIntrospection):
    # Maps type codes to Pony attr types.
    data_types_reverse = {16: 'bool',
                          17: 'buffer',  # 'BinaryField',
                          20: 'int',  # 'BigIntegerField',
                          21: 'int',  # 'SmallIntegerField',
                          23: 'int',  # 'IntegerField',
                          25: 'str',  # 'TextField',
                          700: 'float',  # 'FloatField',
                          701: 'float',  # 'FloatField',
                          869: 'str',  # 'GenericIPAddressField',
                          1042: 'str',  # 'CharField',  # blank-padded
                          1043: 'str',  # 'CharField',
                          1082: 'date',  # 'DateField',
                          1083: 'time',  # 'TimeField',
                          1114: 'datetime',  # 'DateTimeField',
                          1184: 'datetime',  # 'DateTimeField',
                          1266: 'time',  # 'TimeField',
                          1700: 'Decimal',  # 'DecimalField',
                          2950: 'UUID'}  # 'UUIDField',
    imports = {'UUID': 'from uuid import UUID',
                       'LongStr': 'from pony.orm.ormtypes import LongStr',
                       'datetime': 'from datetime import datetime',
                       'time': 'from datetime import time',
                       'date': 'from datetime import date',
                       'Decimal': 'from decimal import Decimal'}
    ignored_tables = []

    @override
    def get_field_type(self, data_type: int | str, description: FieldInfo) -> tuple[str, dict[str, int] | None, str | None]:
        field_type, opts = self.data_types_reverse[int(data_type)], None
        _import = self.imports.get(field_type)
        if description.default and 'nextval' in description.default:
            if field_type == 'int':
                return 'AUTO', opts, _import
        return field_type, opts, _import

    @override
    def get_table_list(self, cursor: Cursor):
        """Return a list of table and view names in the current database."""
        cursor.execute("""
            SELECT c.relname, c.relkind
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'v')
                AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
                AND pg_catalog.pg_table_is_visible(c.oid)""")
        return [TableInfo(row[0], {'r': 't', 'v': 'v'}[row[1]]) for row in cursor.fetchall() if row[0] not in self.ignored_tables]

    @override
    def get_table_description(self, cursor: Cursor, table_name: str) -> list[FieldInfo]:
        """
        Return a description of the table with the DB-API cursor.description
        interface.
        """
        # As cursor.description does not return reliably the nullable property,
        # we have to query the information_schema (#7783)
        cursor.execute("""SELECT column_name, is_nullable, column_default
                            FROM information_schema.columns
                            WHERE table_name = %s""", [table_name])
        field_map: dict[str, tuple[str, ...]] = {line[0]: line[1:] for line in cursor.fetchall()}
        cursor.execute("SELECT * FROM %s LIMIT 1" % self.provider.quote_name(table_name))
        if cursor.description is None:
            return []
        return [FieldInfo(display_size=line.display_size,
                          internal_size=line.internal_size,
                          name=line.name,
                          null_ok=field_map[line.name][0] == 'YES',
                          precision=line.precision,
                          scale=line.scale,
                          type_code=line.type_code,
                          default=field_map[line.name][1]) for line in cursor.description]

    @override
    def get_relations(self,  cursor: Cursor, table_name: str):
        """
        Return a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        cursor.execute("""
            SELECT c2.relname, a1.attname, a2.attname
            FROM pg_constraint con
            LEFT JOIN pg_class c1 ON con.conrelid = c1.oid
            LEFT JOIN pg_class c2 ON con.confrelid = c2.oid
            LEFT JOIN pg_attribute a1 ON c1.oid = a1.attrelid AND a1.attnum = con.conkey[1]
            LEFT JOIN pg_attribute a2 ON c2.oid = a2.attrelid AND a2.attnum = con.confkey[1]
            WHERE c1.relname = %s
                AND con.contype = 'f'""", [table_name])
        relations: dict[str, tuple[str, str]] = {}
        fetched = cursor.fetchall()
        for row in fetched:
            relations[row[1]] = (row[2], row[0])
        return relations

    @override
    def get_key_columns(self,  cursor: Cursor, table_name: str):
        cursor.execute("""
            SELECT kcu.column_name, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column
            FROM information_schema.constraint_column_usage ccu
            LEFT JOIN information_schema.key_column_usage kcu
                ON ccu.constraint_catalog = kcu.constraint_catalog
                    AND ccu.constraint_schema = kcu.constraint_schema
                    AND ccu.constraint_name = kcu.constraint_name
            LEFT JOIN information_schema.table_constraints tc
                ON ccu.constraint_catalog = tc.constraint_catalog
                    AND ccu.constraint_schema = tc.constraint_schema
                    AND ccu.constraint_name = tc.constraint_name
            WHERE kcu.table_name = %s AND tc.constraint_type = 'FOREIGN KEY'""", [table_name])
        return cast(list[tuple[str, str, str]], cursor.fetchall())

    def get_indexes(self,  cursor: Cursor, table_name: str):
        # This query retrieves each index on the given table, including the
        # first associated field name
        cursor.execute("""
        SELECT attr.attname, idx.indkey, idx.indisunique, idx.indisprimary
        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2,
            pg_catalog.pg_index idx, pg_catalog.pg_attribute attr
        WHERE c.oid = idx.indrelid
            AND idx.indexrelid = c2.oid
            AND attr.attrelid = c.oid
            AND attr.attnum = idx.indkey[0]
            AND c.relname = %s""", [table_name])
        indexes: dict[str, dict[str, bool]] = {}
        for row in cursor.fetchall():
            # row[1] (idx.indkey) is stored in the DB as an array. It comes out as
            # a string of space-separated integers. This designates the field
            # indexes (1-based) of the fields that have indexes on the table.
            # Here, we skip any indexes across multiple fields.
            if ' ' in row[1]:
                continue
            if row[0] not in indexes:
                indexes[row[0]] = {'primary_key': False, 'unique': False}
            # It's possible to have the unique and PK constraints in separate indexes.
            if row[3]:
                indexes[row[0]]['primary_key'] = True
            if row[2]:
                indexes[row[0]]['unique'] = True
        return indexes

    def get_constraints(self,  cursor: Cursor, table_name: str) -> dict[str, ColumnInfo]:
        """
        Retrieve any constraints or keys (unique, pk, fk, check, index) across
        one or more columns. Also retrieve the definition of expression-based
        indexes.
        """
        constraints: dict[str, ColumnInfo] = {}
        # Loop over the key table, collecting things as constraints. The column
        # array must return column names in the same order in which they were
        # created.
        # The subquery containing generate_series can be replaced with
        # "WITH ORDINALITY" when support for PostgreSQL 9.3 is dropped.
        cursor.execute("""
            SELECT
                c.conname,
                array(
                    SELECT attname
                    FROM (
                        SELECT unnest(c.conkey) AS colid,
                               generate_series(1, array_length(c.conkey, 1)) AS arridx
                    ) AS cols
                    JOIN pg_attribute AS ca ON cols.colid = ca.attnum
                    WHERE ca.attrelid = c.conrelid
                    ORDER BY cols.arridx
                ),
                c.contype,
                (SELECT fkc.relname || '.' || fka.attname
                FROM pg_attribute AS fka
                JOIN pg_class AS fkc ON fka.attrelid = fkc.oid
                WHERE fka.attrelid = c.confrelid AND fka.attnum = c.confkey[1]),
                cl.reloptions
            FROM pg_constraint AS c
            JOIN pg_class AS cl ON c.conrelid = cl.oid
            JOIN pg_namespace AS ns ON cl.relnamespace = ns.oid
            WHERE ns.nspname = %s AND cl.relname = %s
        """, ["public", table_name])
        for constraint, columns, kind, used_cols, options in cursor.fetchall():
            constraints[cast(str, constraint)] = {"columns": cast(list[str], columns),
                                                  "primary_key": kind == "p",
                                                  "unique": kind in ["p", "u"],
                                                  "foreign_key": tuple(cast(str, used_cols).split(".", 1)) if kind == "f" else None,
                                                  "check": kind == "c",
                                                  "index": False,
                                                  "options": options}
        # Now get indexes
        # The row_number() function for ordering the index fields can be
        # replaced by WITH ORDINALITY in the unnest() functions when support
        # for PostgreSQL 9.3 is dropped.
        cursor.execute("""
            SELECT
                indexname, array_agg(attname ORDER BY rnum), indisunique, indisprimary,
                array_agg(ordering ORDER BY rnum), amname, exprdef, s2.attoptions
            FROM (
                SELECT
                    row_number() OVER () as rnum, c2.relname as indexname,
                    idx.*, attr.attname, am.amname,
                    CASE
                        WHEN idx.indexprs IS NOT NULL THEN
                            pg_get_indexdef(idx.indexrelid)
                    END AS exprdef,
                    CASE am.amname
                        WHEN 'btree' THEN
                            CASE (option & 1)
                                WHEN 1 THEN 'DESC' ELSE 'ASC'
                            END
                    END as ordering,
                    c2.reloptions as attoptions
                FROM (
                    SELECT
                        *, unnest(i.indkey) as key, unnest(i.indoption) as option
                    FROM pg_index i
                ) idx
                LEFT JOIN pg_class c ON idx.indrelid = c.oid
                LEFT JOIN pg_class c2 ON idx.indexrelid = c2.oid
                LEFT JOIN pg_am am ON c2.relam = am.oid
                LEFT JOIN pg_attribute attr ON attr.attrelid = c.oid AND attr.attnum = idx.key
                WHERE c.relname = %s
            ) s2
            GROUP BY indexname, indisunique, indisprimary, amname, exprdef, attoptions;
        """, [table_name])
        for index, columns, unique, primary, orders, _type_, definition, options in cursor.fetchall():
            if index not in constraints:
                constraints[index] = {"columns": columns if columns != [None] else [],
                                      "orders": orders if orders != [None] else [],
                                      "primary_key": primary,
                                      "unique": unique,
                                      "foreign_key": None,
                                      "check": False,
                                      "index": True,
                                      # "type": Index.suffix if type_ == 'btree' else type_,
                                      "definition": definition,
                                      "options": options}
        return constraints
