"""pypika-validate: Join cardinality validation for pypika queries."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Flag, auto, Enum
from typing import Any, List, Optional


class Validate(Flag):
    ONE_TO_MANY = auto()  # left key unique: each right row maps to at most 1 left row
    MANY_TO_ONE = auto()  # right key unique: each left row maps to at most 1 right row
    ONE_TO_ONE = ONE_TO_MANY | MANY_TO_ONE
    LEFT_TOTAL = auto()  # every left row has at least 1 match on the right
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
    value: Optional[List] = None
    error_msg: Optional[str] = None
    error_loc: Optional[str] = None
    error_size: Optional[int] = None
    error_sample: Optional[List] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _q(name: str) -> str:
    """Return name wrapped in double-quotes, escaping any internal double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _tname(table) -> str:
    """Return the raw table name string from a Table object."""
    return table._table_name


def _get_join_fields(criterion, right_table):
    """
    Given an ON criterion and the right-hand Table, return (left_field, right_field).
    Returns (None, None) if the fields cannot be determined.
    """
    all_fields = criterion.fields_()
    right = [f for f in all_fields if f.table is not None and f.table == right_table]
    left = [f for f in all_fields if not (f.table is not None and f.table == right_table)]
    if right and left:
        return left[0], right[0]
    return None, None


def _check_uniqueness(cursor, table, col_name: str, flag_name: str) -> Optional[Results]:
    """
    Verify that *col_name* has no duplicate values in *table*.

    SQL pattern (must return zero rows to pass):
        SELECT <col> FROM <table> GROUP BY <col> HAVING COUNT(*) > 1
    """
    tbl = _q(_tname(table))
    col = _q(col_name)
    dup_subq = f"SELECT {col} FROM {tbl} GROUP BY {col} HAVING COUNT(*) > 1"
    count_sql = f"SELECT COUNT(*) FROM {tbl} WHERE {col} IN ({dup_subq})"
    sample_sql = f"SELECT * FROM {tbl} WHERE {col} IN ({dup_subq}) LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"{flag_name} violated: duplicate values in {tbl}.{col}",
        error_loc=f"{_tname(table)}.{col_name}",
        error_size=count,
        error_sample=sample,
    )


def _check_left_total(cursor, left_table, left_col: str, right_table, right_col: str) -> Optional[Results]:
    """
    Verify every row in *left_table* has at least one match in *right_table*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <left> WHERE <left_col> NOT IN (SELECT <right_col> FROM <right>)
    """
    ltbl = _q(_tname(left_table))
    lcol = _q(left_col)
    rtbl = _q(_tname(right_table))
    rcol = _q(right_col)

    coverage_subq = f"SELECT {rcol} FROM {rtbl}"
    count_sql = f"SELECT COUNT(*) FROM {ltbl} WHERE {lcol} NOT IN ({coverage_subq})"
    sample_sql = f"SELECT * FROM {ltbl} WHERE {lcol} NOT IN ({coverage_subq}) LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"LEFT_TOTAL violated: {count} left-side row(s) have no match on right side",
        error_loc=f"{_tname(left_table)} LEFT_TOTAL → {_tname(right_table)}",
        error_size=count,
        error_sample=sample,
    )


def _check_right_total(cursor, left_table, left_col: str, right_table, right_col: str) -> Optional[Results]:
    """
    Verify every row in *right_table* has at least one match in *left_table*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <right> WHERE <right_col> NOT IN (SELECT <left_col> FROM <left>)
    """
    ltbl = _q(_tname(left_table))
    lcol = _q(left_col)
    rtbl = _q(_tname(right_table))
    rcol = _q(right_col)

    coverage_subq = f"SELECT {lcol} FROM {ltbl}"
    count_sql = f"SELECT COUNT(*) FROM {rtbl} WHERE {rcol} NOT IN ({coverage_subq})"
    sample_sql = f"SELECT * FROM {rtbl} WHERE {rcol} NOT IN ({coverage_subq}) LIMIT 10"

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"RIGHT_TOTAL violated: {count} right-side row(s) have no match on left side",
        error_loc=f"{_tname(right_table)} RIGHT_TOTAL → {_tname(left_table)}",
        error_size=count,
        error_sample=sample,
    )


def _validate_join(cursor, join) -> Optional[Results]:
    """
    Run all validation checks for a single join.  Returns the first
    Results(VALIDATION_ERROR) encountered, or None if all checks pass.
    """
    validate = join.validation
    right_table = join.item

    left_f, right_f = _get_join_fields(join.criterion, right_table)
    if left_f is None or right_f is None:
        return None

    left_table = left_f.table
    left_col = left_f.name
    right_col = right_f.name

    if validate & Validate.ONE_TO_MANY:
        # The left key must be unique (each right row maps to ≤1 left row).
        result = _check_uniqueness(cursor, left_table, left_col, "ONE_TO_MANY")
        if result is not None:
            return result

    if validate & Validate.MANY_TO_ONE:
        # The right key must be unique (each left row maps to ≤1 right row).
        result = _check_uniqueness(cursor, right_table, right_col, "MANY_TO_ONE")
        if result is not None:
            return result

    if validate & Validate.LEFT_TOTAL:
        result = _check_left_total(cursor, left_table, left_col, right_table, right_col)
        if result is not None:
            return result

    if validate & Validate.RIGHT_TOTAL:
        result = _check_right_total(cursor, left_table, left_col, right_table, right_col)
        if result is not None:
            return result

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def execute(cursor: Any, query: Any, skip_validation: bool = False) -> Results:
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

    if skip_validation:
        try:
            cursor.execute(sql)
            value = cursor.fetchall()
            return Results(status=Status.NOT_VALIDATED, value=value)
        except Exception as exc:
            return Results(status=Status.SQL_ERROR, error_msg=str(exc))

    # Run validation queries left-to-right for every join that carries a flag.
    for join in query._joins:
        if not (hasattr(join, "validation") and join.validation is not None):
            continue
        try:
            result = _validate_join(cursor, join)
        except Exception as exc:
            return Results(status=Status.SQL_ERROR, error_msg=str(exc))
        if result is not None:
            return result

    # All validations passed – execute the main query.
    try:
        cursor.execute(sql)
        value = cursor.fetchall()
        return Results(status=Status.OK, value=value)
    except Exception as exc:
        return Results(status=Status.SQL_ERROR, error_msg=str(exc))
