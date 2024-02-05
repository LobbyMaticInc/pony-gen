import ast
from ast import (AST, AnnAssign, Assign, Attribute, Call, ClassDef, Constant,
                 Expr, FunctionDef, Module, Name, Return, arg, arguments,
                 keyword, unparse)
from collections import defaultdict
from itertools import chain
from keyword import iskeyword
from pathlib import Path
from typing import cast

from pony.orm import (Database, Optional, PrimaryKey, Required, Set,
                      db_session, select)

from src.utils import str_to_py_identifier
from utils import create_enum_ast

PG_TO_PY_TYPE_MAP = {'integer': 'int',
                     'bigint': 'int',
                     'smallint': 'int',
                     'text': 'str',
                     'varchar': 'str',
                     'character varying': 'str',
                     'char': 'str',
                     'character': 'str',
                     'date': 'date',
                     'timestamp': 'datetime',
                     'timestamp without time zone': 'datetime',
                     'timestamp with time zone': 'datetime',
                     'time': 'time',
                     'time without time zone': 'time',
                     'time with time zone': 'time',
                     'boolean': 'bool',
                     'json': 'dict',
                     'jsonb': 'dict',
                     'array': 'list',
                     'uuid': 'str',
                     'decimal': 'Decimal',
                     'numeric': 'Decimal',
                     'real': 'float',
                     'double precision': 'float',
                     'ARRAY': 'list'}

PG_ARRAY_TYPE_TO_PY = {
    '_int2': 'int',       # smallint array
    '_int4': 'int',       # integer array
    '_int8': 'int',       # bigint array
    '_float4': 'float',   # real array
    '_float8': 'float',   # double precision array
    '_numeric': 'float',  # arbitrary precision number array, may require handling for precision
    '_text': 'str',       # text array
    '_varchar': 'str',    # character varying array
    '_char': 'str',       # character array
    '_bpchar': 'str',     # blank-padded char array
}

PY_ARRAY_TYPE_TO_PONY = {
    'int': 'IntArray',
    'float': 'FloatArray',
    'str': 'StrArray',
}

db = Database()


def setup_db_and_mapping(db_user: str, db_password: str, db_name: str, db_port: int = 5432):
    try:
        db.bind("postgres", user=db_user, password=db_password, database=db_name, host='127.0.0.1', port=db_port)
        db.generate_mapping(create_tables=False)
    except Exception as e:
        print(f"ERROR: could not create database: {str(e)}")
        exit(1)


class PgType(db.Entity):
    _table_ = "pg_type"
    typname = Required(str)
    typtype = Required(str)
    oid = PrimaryKey(int)
    enums = cast("set[PgEnum]", Set("PgEnum"))


class PgEnum(db.Entity):
    _table_ = "pg_enum"
    enumtypid = Required(PgType, column="enumtypid")
    enumlabel = Required(str)

    PrimaryKey(enumtypid, enumlabel)


class InformationSchemaTableConstraints(db.Entity):
    _table_ = ("information_schema", "table_constraints")
    constraint_catalog = Required(str)
    constraint_schema = Required(str)
    constraint_name = Required(str)
    table_schema = Required(str)
    table_name = Required(str)
    constraint_type = Required(str)

    PrimaryKey(table_schema, constraint_name)


class InformationSchemaKeyColumnUsage(db.Entity):
    _table_ = ("information_schema", "key_column_usage")
    constraint_catalog = Required(str)
    constraint_schema = Required(str)
    constraint_name = Required(str)
    table_schema = Required(str)
    table_name = Required(str)
    column_name = Required(str)
    ordinal_position = Required(int)
    position_in_unique_constraint = Required(int, nullable=True)

    PrimaryKey(table_name, column_name, constraint_name)


class InformationSchemaColumns(db.Entity):
    _table_ = ("information_schema", "columns")
    table_schema = Required(str)
    table_name = Required(str)
    column_name = Required(str)
    data_type = Required(str)
    is_nullable = Required(str)
    column_default = Optional(str, nullable=True)
    character_maximum_length = Optional(int, nullable=True)
    numeric_precision = Optional(int, nullable=True)
    numeric_scale = Optional(int, nullable=True)
    datetime_precision = Optional(int, nullable=True)
    interval_type = Optional(str, nullable=True)
    interval_precision = Optional(int, nullable=True)
    character_set_name = Optional(str, nullable=True)
    collation_name = Optional(str, nullable=True)
    udt_name = Required(str)
    domain_schema = Optional(str, nullable=True)
    domain_name = Optional(str, nullable=True)
    dtd_identifier = Optional(str, nullable=True)

    PrimaryKey(table_schema, table_name, column_name)


def get_table_asts(table_name: str, columns: list[InformationSchemaColumns]) -> list[AST]:
    pony_model_body: list[Expr | Assign | FunctionDef] = [Assign(targets=[Name(id="_table_")], value=Constant(value=table_name), lineno=None, simple=1)]
    enums: list[ClassDef] = []
    primary_keys: list[str] = []
    unique_keys: list[str] = []
    gql_type_body: list[AnnAssign] = []
    for col in columns:
        for constraint_type in select(tc.constraint_type for kcu in InformationSchemaKeyColumnUsage
                                      for tc in InformationSchemaTableConstraints if
                                      tc.constraint_name == kcu.constraint_name and
                                      kcu.table_name == col.table_name and
                                      kcu.table_schema == col.table_schema and
                                      kcu.column_name == col.column_name):
            if constraint_type == "PRIMARY KEY":
                primary_keys.append(col.column_name)
            elif constraint_type == "UNIQUE":
                unique_keys.append(col.column_name)
    for col in columns:
        if col.data_type == "USER-DEFINED":
            pony_assign_type = 'str'
            gql_assign_type = 'str'
            enum_data: dict[str, list[str]] = defaultdict(list)
            for typname, enumlabel in select((t.typname, e.enumlabel) for t in PgType for e in PgEnum
                                             if t.typtype == 'e' and t.typname == col.udt_name and e.enumtypid == t):
                enum_data[typname].append(enumlabel)
            assert enum_data
            enums.append(create_enum_ast(col.udt_name.replace(table_name, ""), enum_data[col.udt_name]))
        elif col.data_type in ["ARRAY", "array"]:
            py_type = PG_ARRAY_TYPE_TO_PY[col.udt_name]
            pony_assign_type = PY_ARRAY_TYPE_TO_PONY[py_type]
            gql_assign_type = f"list[{py_type}]"
        elif col.data_type in ["json", "jsonb"]:
            pony_assign_type = "Json"
            gql_assign_type = "dict[str, Any]"
        else:
            py_type = PG_TO_PY_TYPE_MAP[col.data_type]
            pony_assign_type = py_type
            gql_assign_type = py_type
        match (len(primary_keys), col.column_name in primary_keys, col.is_nullable):
            case (1, True, _):
                pony_col_wrapper = 'PrimaryKey'
            case (_, _, "NO"):
                pony_col_wrapper = 'Required'
            case _:
                pony_col_wrapper = 'Optional'
                gql_assign_type += " | None"
        keyword_dict = {'column': col.column_name,
                        'sql_default': col.column_default,
                        "nullable": col.is_nullable == "YES",
                        'max_len': col.character_maximum_length,
                        'precision': col.numeric_precision if pony_assign_type in ["Decimal", "time", "timedelta", "datetime"] else None,
                        'scale': col.numeric_scale if pony_assign_type == "Decimal" else None,
                        'unique': True if col.column_name in unique_keys else None}
        keyword_asts = [keyword(arg=arg, value=Constant(value=value)) for arg, value in keyword_dict.items() if value is not None]
        santiized_column_name = str_to_py_identifier(col.column_name, case_type='snake')
        pony_model_body.append(Assign(targets=[Name(id=santiized_column_name)],
                                      value=Call(func=Name(id=pony_col_wrapper),
                                                 args=[Name(id=pony_assign_type)],
                                                 keywords=keyword_asts),
                                      lineno=None, simple=1))
        gql_type_body.append(AnnAssign(target=Name(id=santiized_column_name), annotation=Name(id=gql_assign_type), value=None, simple=1))
    if len(primary_keys) > 1:
        pony_model_body.append(Expr(value=Call(func=Name(id='PrimaryKey'), args=[Name(id=key) for key in primary_keys], keywords=[])))
    pony_class_name = str_to_py_identifier(table_name, case_type='title')
    gql_class_name = pony_class_name + "Type"
    pony_model_body.append(FunctionDef(name='to_gql_type',
                                       args=arguments(posonlyargs=[],
                                                      args=[arg(arg='self', annotation=None, type_comment=None)],
                                                      vararg=None,
                                                      kwonlyargs=[],
                                                      kw_defaults=[],
                                                      kwarg=None,
                                                      defaults=[]),
                                       body=[Return(value=Call(func=Name(id=gql_class_name),
                                                               args=[],
                                                               keywords=[keyword(arg=None,
                                                                                 value=Call(func=Attribute(value=Name(id='self'), attr='to_dict'),
                                                                                            args=[],
                                                                                            keywords=[]))]))],
                                       decorator_list=[],
                                       returns=None,
                                       lineno=None,
                                       type_comment=None))
    return [*enums,
            ClassDef(name=gql_class_name,
                     decorator_list=[Attribute(value=Name(id='strawberry'), attr='type')],
                     bases=[],
                     keywords=[],
                     body=gql_type_body),
            ClassDef(name=pony_class_name,
                     decorator_list=[],
                     bases=[Name(id='db.Entity')],
                     body=pony_model_body,
                     keywords=[])]


DEFAULT_IMPORTS = """
import strawberry
from typing import Any
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from enum import Enum
from pony.orm import Database, PrimaryKey, Json, Optional, PrimaryKey, Required, Set, composite_key, LongStr, Json, IntArray, StrArray, FloatArray
db = Database()
"""


@db_session
def generate_pony_orm_model_ast(filepath: Path):
    table_to_columns: dict[str, list[InformationSchemaColumns]] = defaultdict(list)
    for col in select(c for c in InformationSchemaColumns if c.table_schema == 'public'):
        table_to_columns[col.table_name].append(col)
    tables = chain.from_iterable(get_table_asts(table_name, columns) for table_name, columns in table_to_columns.items())
    models_module = Module(type_ignores=[], body=ast.parse(DEFAULT_IMPORTS).body + list(tables))
    # models_module: Module = ImportConsolidator().traverse(models_module)
    # models_module: Module = DedupNodes().traverse(models_module)
    with filepath.joinpath("pony_models.py").open("w") as file:
        _ = file.write(unparse(models_module))
