"""pypika-validate: Join cardinality validation for pypika queries."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Flag, auto, Enum
from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from pypika.queries import JoinOn, QueryBuilder

# Pseudotypes: the exact cursor and row-value types depend on the SQL engine.
pCursor = Any
pValue = Any


class Validate(Flag):
    NONE = 0              # empty flag: no validation
    ONE_TO_MANY = auto()  # left key unique: each right row maps to at most 1 left row
    MANY_TO_ONE = auto()  # right key unique: each left row maps to at most 1 right row
    ONE_TO_ONE = ONE_TO_MANY | MANY_TO_ONE
    LEFT_TOTAL = auto()   # every left row has at least 1 match on the right
    RIGHT_TOTAL = auto()  # every right row has at least 1 match on the left
    TOTAL = LEFT_TOTAL | RIGHT_TOTAL
    MANDATORY = ONE_TO_ONE | TOTAL


class Status(Enum):
    OK = "ok"
    VALIDATION_ERROR = "validation_error"
    SQL_ERROR = "sql_error"
    NOT_VALIDATED = "not_validated"


@dataclass
class Results:
    status: Status
    value: Optional[List[pValue]] = None
    error_msg: Optional[str] = None
    error_loc: Optional[str] = None
    error_size: Optional[int] = None
    error_sample: Optional[List[pValue]] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _q(name: str) -> str:
    """Return name wrapped in double-quotes, escaping any internal double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _tname(table: Any) -> str:
    """Return the raw table name string from a Table object."""
    return table._table_name


def _get_left_table(criterion: Any, right_table: Any) -> Any:
    """
    Extract the single left-side table from a join criterion.

    Returns None if the left side cannot be identified as a single table
    (e.g. no field references, or fields from multiple different left tables).
    """
    all_fields = criterion.fields_()
    left_tables = {f.table for f in all_fields if f.table is not None and f.table != right_table}
    if len(left_tables) != 1:
        return None
    return next(iter(left_tables))


def _check_many_to_one(
    cursor: pCursor, left_table: Any, right_table: Any, criterion_sql: str
) -> Optional[Results]:
    """
    Verify every row in *left_table* matches at most one row in *right_table*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <left> WHERE (SELECT COUNT(*) FROM <right> WHERE <criterion>) > 1
    """
    ltbl = _q(_tname(left_table))
    rtbl = _q(_tname(right_table))
    count_sql = f"SELECT COUNT(*) FROM {ltbl} WHERE (SELECT COUNT(*) FROM {rtbl} WHERE {criterion_sql}) > 1"
    sample_sql = f"SELECT * FROM {ltbl} WHERE (SELECT COUNT(*) FROM {rtbl} WHERE {criterion_sql}) > 1 LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"MANY_TO_ONE violated: {count} left-side row(s) match multiple right-side rows",
        error_loc=f"{_tname(left_table)} MANY_TO_ONE → {_tname(right_table)}",
        error_size=count,
        error_sample=sample,
    )


def _check_one_to_many(
    cursor: pCursor, left_table: Any, right_table: Any, criterion_sql: str
) -> Optional[Results]:
    """
    Verify every row in *right_table* matches at most one row in *left_table*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <right> WHERE (SELECT COUNT(*) FROM <left> WHERE <criterion>) > 1
    """
    ltbl = _q(_tname(left_table))
    rtbl = _q(_tname(right_table))
    count_sql = f"SELECT COUNT(*) FROM {rtbl} WHERE (SELECT COUNT(*) FROM {ltbl} WHERE {criterion_sql}) > 1"
    sample_sql = f"SELECT * FROM {rtbl} WHERE (SELECT COUNT(*) FROM {ltbl} WHERE {criterion_sql}) > 1 LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"ONE_TO_MANY violated: {count} right-side row(s) match multiple left-side rows",
        error_loc=f"{_tname(left_table)} ONE_TO_MANY → {_tname(right_table)}",
        error_size=count,
        error_sample=sample,
    )


def _check_left_total(
    cursor: pCursor, left_table: Any, right_table: Any, criterion_sql: str
) -> Optional[Results]:
    """
    Verify every row in *left_table* has at least one match in *right_table*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <left> WHERE NOT EXISTS (SELECT 1 FROM <right> WHERE <criterion>)
    """
    ltbl = _q(_tname(left_table))
    rtbl = _q(_tname(right_table))
    count_sql = f"SELECT COUNT(*) FROM {ltbl} WHERE NOT EXISTS (SELECT 1 FROM {rtbl} WHERE {criterion_sql})"
    sample_sql = f"SELECT * FROM {ltbl} WHERE NOT EXISTS (SELECT 1 FROM {rtbl} WHERE {criterion_sql}) LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"LEFT_TOTAL violated: {count} left-side row(s) have no match on right side",
        error_loc=f"{_tname(left_table)} LEFT_TOTAL → {_tname(right_table)}",
        error_size=count,
        error_sample=sample,
    )


def _check_right_total(
    cursor: pCursor, left_table: Any, right_table: Any, criterion_sql: str
) -> Optional[Results]:
    """
    Verify every row in *right_table* has at least one match in *left_table*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <right> WHERE NOT EXISTS (SELECT 1 FROM <left> WHERE <criterion>)
    """
    ltbl = _q(_tname(left_table))
    rtbl = _q(_tname(right_table))
    count_sql = f"SELECT COUNT(*) FROM {rtbl} WHERE NOT EXISTS (SELECT 1 FROM {ltbl} WHERE {criterion_sql})"
    sample_sql = f"SELECT * FROM {rtbl} WHERE NOT EXISTS (SELECT 1 FROM {ltbl} WHERE {criterion_sql}) LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"RIGHT_TOTAL violated: {count} right-side row(s) have no match on left side",
        error_loc=f"{_tname(right_table)} RIGHT_TOTAL → {_tname(left_table)}",
        error_size=count,
        error_sample=sample,
    )


def _validate_join(cursor: pCursor, join: JoinOn) -> Optional[Results]:
    """
    Run all validation checks for a single join.  Returns the first
    Results(VALIDATION_ERROR) encountered, or None if all checks pass.

    Validation uses correlated subqueries against the full ON criterion, so
    composite keys and arbitrary expressions are handled correctly.
    """
    validate = join.validation
    right_table = join.item

    left_table = _get_left_table(join.criterion, right_table)
    if left_table is None:
        return None

    criterion_sql = join.criterion.get_sql(quote_char='"', subquery=True, with_namespace=True)

    if validate & Validate.ONE_TO_MANY:
        result = _check_one_to_many(cursor, left_table, right_table, criterion_sql)
        if result is not None:
            return result

    if validate & Validate.MANY_TO_ONE:
        result = _check_many_to_one(cursor, left_table, right_table, criterion_sql)
        if result is not None:
            return result

    if validate & Validate.LEFT_TOTAL:
        result = _check_left_total(cursor, left_table, right_table, criterion_sql)
        if result is not None:
            return result

    if validate & Validate.RIGHT_TOTAL:
        result = _check_right_total(cursor, left_table, right_table, criterion_sql)
        if result is not None:
            return result

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def execute(cursor: pCursor, query: QueryBuilder, skip_validation: bool = False) -> Results:
    """
    Execute a pypika query, optionally validating join cardinality first.

    Args:
        cursor:           A DB-API 2.0 compliant database cursor.
        query:            A pypika QueryBuilder instance.
        skip_validation:  When True, skip all validation and return NOT_VALIDATED.

    Returns:
        A Results object whose ``status`` is one of:

        * ``Status.OK``               – query executed, all validations passed.
        * ``Status.NOT_VALIDATED``    – query executed, validation was skipped.
        * ``Status.VALIDATION_ERROR`` – a cardinality constraint was violated.
        * ``Status.SQL_ERROR``        – the database raised an error.
    """
    sql = query.get_sql()

    if not skip_validation:
        for join in query._joins:
            if not (hasattr(join, "validation") and join.validation):
                continue
            try:
                result = _validate_join(cursor, join)
            except Exception as exc:
                return Results(status=Status.SQL_ERROR, error_msg=str(exc))
            if result is not None:
                return result

    try:
        cursor.execute(sql)
        value: List[pValue] = cursor.fetchall()
        status = Status.NOT_VALIDATED if skip_validation else Status.OK
        return Results(status=status, value=value)
    except Exception as exc:
        return Results(status=Status.SQL_ERROR, error_msg=str(exc))
