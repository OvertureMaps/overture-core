"""Spark SQL table definitions rendered by SQLGlot, without Spark or Arrow."""

import re
from collections.abc import Iterable
from dataclasses import dataclass

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError


@dataclass(frozen=True)
class SqlColumn:
    name: str
    sql_type: str
    nullable: bool = True
    comment: str | None = None


def create_table_sql(
    table_name: str,
    columns: Iterable[SqlColumn],
    *,
    table_format: str = "iceberg",
    if_not_exists: bool = False,
) -> str:
    """Build a Spark SQL CREATE TABLE statement from caller-owned columns.

    Table names use Spark syntax. Column names and comments are plain text.
    Types exclude nullability and comments. Formats are short provider names
    like iceberg, parquet or delta.

    The result has no semicolon, so callers can append their own PARTITIONED BY,
    LOCATION or TBLPROPERTIES clauses. This function does not execute SQL.

    Example:
        sql = create_table_sql(
            "catalog.db.events",
            [SqlColumn("id", "STRING", nullable=False, comment="Source identifier")],
            if_not_exists=True,
        )
    """
    if not table_name.strip() or not table_format.strip():
        raise ValueError("table_name and table_format must not be empty")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", table_format):
        raise ValueError("table_format must be a short provider name")

    definitions = []
    for column in columns:
        if not column.name.strip() or not column.sql_type.strip():
            raise ValueError("Column name and sql_type must not be empty")
        try:
            sql_type = parse_one(column.sql_type, read="spark", into=exp.DataType)
        except ParseError as error:
            raise ValueError(f"Invalid sql_type for column {column.name!r}") from error
        if not isinstance(sql_type, exp.DataType):
            raise ValueError(
                f"Invalid sql_type for column {column.name!r}: expected one type"
            )
        constraints = []
        if not column.nullable:
            constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
        if column.comment is not None:
            constraints.append(
                exp.ColumnConstraint(
                    kind=exp.CommentColumnConstraint(
                        this=exp.Literal.string(column.comment)
                    )
                )
            )
        definitions.append(
            exp.ColumnDef(
                this=exp.to_identifier(column.name),
                kind=sql_type,
                constraints=constraints,
            )
        )

    if not definitions:
        raise ValueError("At least one column is required")

    statement = exp.Create(
        this=exp.Schema(
            this=exp.to_table(table_name, dialect="spark"), expressions=definitions
        ),
        kind="TABLE",
        exists=if_not_exists,
        properties=exp.Properties(
            expressions=[exp.FileFormatProperty(this=exp.Var(this=table_format))]
        ),
    )
    return statement.sql(dialect="spark", pretty=True)
