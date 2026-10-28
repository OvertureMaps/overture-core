"""Spark SQL table definitions rendered by SQLGlot, without Spark or Arrow."""

from collections.abc import Iterable
from dataclasses import dataclass

from sqlglot import exp


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

    Names and SQL types use Spark syntax, including backticks for quoted names.
    Comments are plain text. SQLGlot handles parsing, escaping and rendering in
    the Spark dialect, including complex types and the table format.

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

    definitions = []
    for column in columns:
        if not column.name.strip() or not column.sql_type.strip():
            raise ValueError("Column name and sql_type must not be empty")
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
                this=exp.parse_identifier(column.name, dialect="spark"),
                kind=exp.DataType.build(column.sql_type, dialect="spark"),
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
