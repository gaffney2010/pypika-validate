"""pypika-validate: Join cardinality validation for pypika queries."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Flag, auto, Enum
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

if TYPE_CHECKING:
    from pypika.queries import JoinOn, QueryBuilder

log = logging.getLogger(__name__)

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


def _item_from_and_name(item: Any) -> Tuple[str, str]:
    """
    Return ``(from_expr, display_name)`` for any join item.

    * Regular ``Table``       → ``('"table_name"', 'table_name')``
    * ``QueryBuilder`` (aliased subquery) → ``('(SELECT ...) "alias"', 'alias')``
    * ``AliasedQuery`` (CTE / named query) → ``('"alias"', 'alias')``
    """
    if isinstance(item._table_name, str):
        # Regular Table — _table_name is the plain string name.
        name = item._table_name
        return _q(name), name

    # Subquery (QueryBuilder) or CTE reference (AliasedQuery).
    alias = item.alias
    sql = item.get_sql(quote_char='"', subquery=True, with_alias=True, with_namespace=True)
    # QueryBuilder renders as "(SELECT ...) alias"; AliasedQuery renders as just its name.
    if sql.startswith('('):
        return sql, alias
    else:
        return _q(alias), alias


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
    cursor: pCursor,
    left_from: str, right_from: str,
    criterion_sql: str,
    left_name: str, right_name: str,
    verbose: bool = False,
) -> Optional[Results]:
    """
    Verify every row in *left* matches at most one row in *right*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <left> WHERE (SELECT COUNT(*) FROM <right> WHERE <criterion>) > 1
    """
    count_sql = f"SELECT COUNT(*) FROM {left_from} WHERE (SELECT COUNT(*) FROM {right_from} WHERE {criterion_sql}) > 1"
    sample_sql = f"SELECT * FROM {left_from} WHERE (SELECT COUNT(*) FROM {right_from} WHERE {criterion_sql}) > 1 LIMIT 10"

    if verbose:
        log.debug("  MANY_TO_ONE: each %s row matches at most 1 %s row", left_name, right_name)
        log.debug("    SQL: %s", count_sql)

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]

    if verbose:
        log.debug("    → %d violation(s)", count)

    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"MANY_TO_ONE violated: {count} left-side row(s) match multiple right-side rows",
        error_loc=f"{left_name} MANY_TO_ONE → {right_name}",
        error_size=count,
        error_sample=sample,
    )


def _check_one_to_many(
    cursor: pCursor,
    left_from: str, right_from: str,
    criterion_sql: str,
    left_name: str, right_name: str,
    verbose: bool = False,
) -> Optional[Results]:
    """
    Verify every row in *right* matches at most one row in *left*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <right> WHERE (SELECT COUNT(*) FROM <left> WHERE <criterion>) > 1
    """
    count_sql = f"SELECT COUNT(*) FROM {right_from} WHERE (SELECT COUNT(*) FROM {left_from} WHERE {criterion_sql}) > 1"
    sample_sql = f"SELECT * FROM {right_from} WHERE (SELECT COUNT(*) FROM {left_from} WHERE {criterion_sql}) > 1 LIMIT 10"

    if verbose:
        log.debug("  ONE_TO_MANY: each %s row matches at most 1 %s row", right_name, left_name)
        log.debug("    SQL: %s", count_sql)

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]

    if verbose:
        log.debug("    → %d violation(s)", count)

    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"ONE_TO_MANY violated: {count} right-side row(s) match multiple left-side rows",
        error_loc=f"{left_name} ONE_TO_MANY → {right_name}",
        error_size=count,
        error_sample=sample,
    )


def _check_left_total(
    cursor: pCursor,
    left_from: str, right_from: str,
    criterion_sql: str,
    left_name: str, right_name: str,
    verbose: bool = False,
) -> Optional[Results]:
    """
    Verify every row in *left* has at least one match in *right*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <left> WHERE NOT EXISTS (SELECT 1 FROM <right> WHERE <criterion>)
    """
    count_sql = f"SELECT COUNT(*) FROM {left_from} WHERE NOT EXISTS (SELECT 1 FROM {right_from} WHERE {criterion_sql})"
    sample_sql = f"SELECT * FROM {left_from} WHERE NOT EXISTS (SELECT 1 FROM {right_from} WHERE {criterion_sql}) LIMIT 10"

    if verbose:
        log.debug("  LEFT_TOTAL: every %s row has a match in %s", left_name, right_name)
        log.debug("    SQL: %s", count_sql)

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]

    if verbose:
        log.debug("    → %d unmatched row(s)", count)

    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"LEFT_TOTAL violated: {count} left-side row(s) have no match on right side",
        error_loc=f"{left_name} LEFT_TOTAL → {right_name}",
        error_size=count,
        error_sample=sample,
    )


def _check_right_total(
    cursor: pCursor,
    left_from: str, right_from: str,
    criterion_sql: str,
    left_name: str, right_name: str,
    verbose: bool = False,
) -> Optional[Results]:
    """
    Verify every row in *right* has at least one match in *left*.

    SQL pattern (must return zero rows to pass):
        SELECT * FROM <right> WHERE NOT EXISTS (SELECT 1 FROM <left> WHERE <criterion>)
    """
    count_sql = f"SELECT COUNT(*) FROM {right_from} WHERE NOT EXISTS (SELECT 1 FROM {left_from} WHERE {criterion_sql})"
    sample_sql = f"SELECT * FROM {right_from} WHERE NOT EXISTS (SELECT 1 FROM {left_from} WHERE {criterion_sql}) LIMIT 10"

    if verbose:
        log.debug("  RIGHT_TOTAL: every %s row has a match in %s", right_name, left_name)
        log.debug("    SQL: %s", count_sql)

    cursor.execute(count_sql)
    count = cursor.fetchone()[0]

    if verbose:
        log.debug("    → %d unmatched row(s)", count)

    if count == 0:
        return None

    cursor.execute(sample_sql)
    sample: List[pValue] = cursor.fetchall()
    return Results(
        status=Status.VALIDATION_ERROR,
        error_msg=f"RIGHT_TOTAL violated: {count} right-side row(s) have no match on left side",
        error_loc=f"{right_name} RIGHT_TOTAL → {left_name}",
        error_size=count,
        error_sample=sample,
    )


def _validate_join(cursor: pCursor, join: JoinOn, verbose: bool = False) -> Optional[Results]:
    """
    Run all validation checks for a single join.  Returns the first
    Results(VALIDATION_ERROR) encountered, or None if all checks pass.

    Validation uses correlated subqueries against the full ON criterion, so
    composite keys, arbitrary expressions, and subquery join targets are all
    handled correctly.
    """
    validate = join.validation
    right_table = join.item

    left_table = _get_left_table(join.criterion, right_table)
    if left_table is None:
        return None

    left_from, left_name = _item_from_and_name(left_table)
    right_from, right_name = _item_from_and_name(right_table)
    criterion_sql = join.criterion.get_sql(quote_char='"', subquery=True, with_namespace=True)

    if verbose:
        log.debug("Validating join: %s → %s [%s]", left_name, right_name, join.validation)
        log.debug("  ON: %s", criterion_sql)

    if validate & Validate.ONE_TO_MANY:
        result = _check_one_to_many(cursor, left_from, right_from, criterion_sql, left_name, right_name, verbose=verbose)
        if result is not None:
            return result

    if validate & Validate.MANY_TO_ONE:
        result = _check_many_to_one(cursor, left_from, right_from, criterion_sql, left_name, right_name, verbose=verbose)
        if result is not None:
            return result

    if validate & Validate.LEFT_TOTAL:
        result = _check_left_total(cursor, left_from, right_from, criterion_sql, left_name, right_name, verbose=verbose)
        if result is not None:
            return result

    if validate & Validate.RIGHT_TOTAL:
        result = _check_right_total(cursor, left_from, right_from, criterion_sql, left_name, right_name, verbose=verbose)
        if result is not None:
            return result

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def execute(
    cursor: pCursor,
    query: QueryBuilder,
    skip_validation: bool = False,
    verbose: bool = False,
) -> Results:
    """
    Execute a pypika query, optionally validating join cardinality first.

    Args:
        cursor:           A DB-API 2.0 compliant database cursor.
        query:            A pypika QueryBuilder instance.
        skip_validation:  When True, skip all validation and return NOT_VALIDATED.
        verbose:          When True, emit DEBUG-level log messages describing each
                          validation check and the SQL it runs.

    Returns:
        A Results object whose ``status`` is one of:

        * ``Status.OK``               – query executed, all validations passed.
        * ``Status.NOT_VALIDATED``    – query executed, validation was skipped.
        * ``Status.VALIDATION_ERROR`` – a cardinality constraint was violated.
        * ``Status.SQL_ERROR``        – the database raised an error.
    """
    sql = query.get_sql()

    if verbose:
        log.debug("Query SQL: %s", sql)

    if not skip_validation:
        for join in query._joins:
            if not (hasattr(join, "validation") and join.validation):
                continue
            try:
                result = _validate_join(cursor, join, verbose=verbose)
            except Exception as exc:
                return Results(status=Status.SQL_ERROR, error_msg=str(exc))
            if result is not None:
                return result

    try:
        cursor.execute(sql)
        value: List[pValue] = cursor.fetchall()
        status = Status.NOT_VALIDATED if skip_validation else Status.OK
        if verbose:
            log.debug("Execution: %d row(s) returned, status=%s", len(value), status.value)
        return Results(status=status, value=value)
    except Exception as exc:
        return Results(status=Status.SQL_ERROR, error_msg=str(exc))
