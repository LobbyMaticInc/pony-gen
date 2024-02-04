from typing import Any, cast

from MySQLdb.constants import FIELD_TYPE
from psycopg2.extensions import cursor as Cursor

from .base import ColumnInfo, FieldInfo
from .base import Introspection as BaseIntrospection
from .base import TableInfo


class Introspection(BaseIntrospection):
    #
    data_types_reverse = {FIELD_TYPE.BLOB: 'buffer',  # 'TextField',
                          FIELD_TYPE.CHAR: 'str',  # 'CharField',
                          FIELD_TYPE.DECIMAL: 'Decimal',  # 'DecimalField',
                          FIELD_TYPE.NEWDECIMAL: 'Decimal',  # 'DecimalField',
                          FIELD_TYPE.DATE: 'date',  # 'DateField',
                          FIELD_TYPE.DATETIME: 'datetime',  # 'DateTimeField',
                          FIELD_TYPE.DOUBLE: 'float',  # 'FloatField',
                          FIELD_TYPE.FLOAT: 'float',  # 'FloatField',
                          FIELD_TYPE.INT24: 'int',  # 'IntegerField',
                          FIELD_TYPE.LONG: 'int',  # 'IntegerField',
                          FIELD_TYPE.LONGLONG: 'int',  # 'BigIntegerField',
                          FIELD_TYPE.SHORT: 'int',  # 'SmallIntegerField',
                          FIELD_TYPE.STRING: 'str',  # 'CharField',
                          FIELD_TYPE.TIME: 'time',  # 'TimeField',
                          FIELD_TYPE.TIMESTAMP: 'time',  # 'DateTimeField',
                          FIELD_TYPE.TINY: 'int',  # 'IntegerField',
                          FIELD_TYPE.TINY_BLOB: 'str',  # 'TextField',
                          FIELD_TYPE.MEDIUM_BLOB: 'str',  # 'TextField',
                          FIELD_TYPE.LONG_BLOB: 'str',  # 'TextField',
                          FIELD_TYPE.VAR_STRING: 'str'}  # 'CharField',
    imports = {'LongStr': 'from pony.orm.ormtypes import LongStr',
               'datetime': 'from datetime import datetime',
               'time': 'from datetime import time',
               'date': 'from datetime import date',
               'Decimal': 'from decimal import Decimal'}
    # TODO

    def get_field_type(self, data_type: int | str, description: FieldInfo) -> tuple[str, dict[str, int] | None, str | None]:
        field_type, opts, _import = self.data_types_reverse[int(data_type)], None, None
        assert _import is None
        _import = self.imports.get(field_type)
        if description.default and 'nextval' in description.default:
            if field_type == 'int':
                return 'AUTO', opts, _import
        return field_type, opts, _import

    def get_table_list(self, cursor: Cursor):
        """Return a list of table and view names in the current database."""
        cursor.execute("SHOW FULL TABLES")
        return [TableInfo(row[0], {'BASE TABLE': 't', 'VIEW': 'v'}[row[1]]) for row in cursor.fetchall()]

    def get_table_description(self, cursor: Cursor, table_name: str):
        """
        Return a description of the table with the DB-API cursor.description
        interface."
        """
        # information_schema database gives more accurate results for some figures:
        # - varchar length returned by cursor.description is an internal length,
        #   not visible length (#5725)
        # - precision and scale (for decimal fields) (#5014)
        # - auto_increment is not available in cursor.description
        cursor.execute("""
            SELECT
                column_name, data_type, character_maximum_length,
                numeric_precision, numeric_scale, extra, column_default,
                CASE
                    WHEN column_type LIKE '%% unsigned' THEN 1
                    ELSE 0
                END AS is_unsigned
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = DATABASE()""", [table_name])
        field_info = {line[0]: line for line in cursor.fetchall()}
        quote_name = self.provider.quote_name
        cursor.execute("SELECT * FROM %s LIMIT 1" % quote_name(table_name))

        def to_int(i: int | None):
            return int(i) if i is not None else i
        fields: list[FieldInfo] = []
        for line in cast(Any, cursor.description):
            col_name = cast(str, line[0])
            fields.append(FieldInfo(name=col_name,
                                    type_code=line[1],
                                    display_size=line[2],
                                    internal_size=to_int(field_info[col_name][2]) or line[3],
                                    precision=to_int(field_info[col_name][3]) or line[4],
                                    scale=to_int(field_info[col_name][4]) or line[5],
                                    null_ok=line[6],
                                    default=field_info[col_name][6],
                                    extra=field_info[col_name][5],
                                    is_unsigned=field_info[col_name][7]))
        return fields

    def get_relations(self, cursor: Cursor, table_name: str):
        """
        Return a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        constraints = self.get_key_columns(cursor, table_name)
        relations: dict[str, tuple[str, str]] = {}
        for my_fieldname, other_table, other_field in constraints:
            relations[my_fieldname] = (other_field, other_table)
        return relations

    def get_key_columns(self, cursor: Cursor, table_name: str):
        """
        Return a list of (column_name, referenced_table_name, referenced_column_name)
        for all key columns in the given table.
        """
        key_columns: list[tuple[Any, ...]] = []
        cursor.execute("""
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s
                AND table_schema = DATABASE()
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL""", [table_name])
        key_columns.extend(cursor.fetchall())
        return key_columns

    def get_storage_engine(self, cursor: Cursor, table_name: str):
        """
        Retrieve the storage engine for a given table. Return the default
        storage engine if the table doesn't exist.
        """
        cursor.execute("SELECT engine FROM information_schema.tables WHERE table_name = %s", [table_name])
        result = cursor.fetchone()
        if not result:
            return self.connection.features._mysql_storage_engine
        return result[0]

    def get_constraints(self, cursor: Cursor, table_name: str) -> dict[str, ColumnInfo]:
        """
        Retrieve any constraints or keys (unique, pk, fk, check, index) across
        one or more columns.
        """
        quote_name = self.provider.quote_name
        constraints: dict[str, ColumnInfo] = {}
        # Get the actual constraint names and columns
        name_query = """
            SELECT kc.`constraint_name`, kc.`column_name`,
                kc.`referenced_table_name`, kc.`referenced_column_name`
            FROM information_schema.key_column_usage AS kc
            WHERE
                kc.table_schema = DATABASE() AND
                kc.table_name = %s
        """
        cursor.execute(name_query, [table_name])
        for constraint, column, ref_table, ref_column in cursor.fetchall():
            if constraint not in constraints:
                constraints[constraint] = {'columns': [],
                                           'primary_key': False,
                                           'unique': False,
                                           'index': False,
                                           'check': False,
                                           'foreign_key': (ref_table, ref_column) if ref_column else None}
            constraints[constraint]['columns'].append(column)
        # Now get the constraint types
        type_query = """
            SELECT c.constraint_name, c.constraint_type
            FROM information_schema.table_constraints AS c
            WHERE
                c.table_schema = DATABASE() AND
                c.table_name = %s
        """
        cursor.execute(type_query, [table_name])
        for constraint, kind in cursor.fetchall():
            if kind.lower() == "primary key":
                constraints[constraint]['primary_key'] = True
                constraints[constraint]['unique'] = True
            elif kind.lower() == "unique":
                constraints[constraint]['unique'] = True
        # Now add in the indexes
        cursor.execute("SHOW INDEX FROM %s" % quote_name(table_name))
        for _table, _non_unique, index, _colseq, column, _type_ in [x[:5] + (x[10],) for x in cursor.fetchall()]:
            if index not in constraints:
                constraints[index] = {'columns': [],
                                      'primary_key': False,
                                      'unique': False,
                                      'check': False,
                                      'index': True,
                                      'foreign_key': None}
            constraints[index]['index'] = True
            # constraints[index]['type'] = Index.suffix if type_ == 'BTREE' else type_.lower()
            constraints[index]['columns'].append(column)
        # Convert the sorted sets to lists
        for constraint in constraints.values():
            constraint['columns'] = list(constraint['columns'])
        return constraints
