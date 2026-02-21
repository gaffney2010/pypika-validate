# pypika-validation

This is a fork of pypika, with some new semantics for validating joins.

The motivation for the library is an observation that almost all SQL errors are the result of unintended join effects.  By adding validations, we can easily make our checks explicit in debug settings, while documenting expectations for improved readability.  Though there is some performance hit to do the additional checks, it's our experience that the gains in reliability and engineering time are worth the trade-off.

## Validation Semantics

A `validation` flag may be passed to any of the `join` functions of pypika.  For example:

```python
Query.from_(base_table).join(join_table, JoinType.inner, validate=Validate.ONE_TO_ONE).on_field("xkey", "ykey")
```

This does additional work to check that the tables are 1-1.

Validation works against the full `ON` criterion, so composite keys and arbitrary expressions are supported.  For a composite key join such as:

```python
Query.from_(base_table)
    .join(join_table, validate=Validate.ONE_TO_ONE)
    .on((base_table.cat == join_table.cat) & (base_table.sku == join_table.sku))
```

the generated validation queries use correlated subqueries:

```sql
-- ONE_TO_MANY: each join_table row matches at most 1 base_table row
SELECT *
FROM join_table
WHERE (
    SELECT COUNT(*) FROM base_table
    WHERE base_table.cat = join_table.cat AND base_table.sku = join_table.sku
) > 1;

-- MANY_TO_ONE: each base_table row matches at most 1 join_table row
SELECT *
FROM base_table
WHERE (
    SELECT COUNT(*) FROM join_table
    WHERE base_table.cat = join_table.cat AND base_table.sku = join_table.sku
) > 1;
```

The same pattern applies for `LEFT_TOTAL` and `RIGHT_TOTAL`, using `NOT EXISTS` instead:

```sql
-- LEFT_TOTAL: every base_table row has at least one match in join_table
SELECT *
FROM base_table
WHERE NOT EXISTS (
    SELECT 1 FROM join_table
    WHERE base_table.cat = join_table.cat AND base_table.sku = join_table.sku
);
```

The full set of validation flags are:

- `ONE_TO_MANY`: For every row on the right, there is at most 1 entry on the left.
- `MANY_TO_ONE`: For every row on the left, there is at most 1 entry on the right.
- `ONE_TO_ONE`: Both `ONE_TO_MANY` and `MANY_TO_ONE`.
- `RIGHT_TOTAL`: For every row on the right, there is at least 1 matching entry on the left (i.e., the right side is fully covered by the join).
- `LEFT_TOTAL`: For every row on the left, there is at least 1 matching entry on the right (i.e., the left side is fully covered by the join).
- `TOTAL`: Both `RIGHT_TOTAL` and `LEFT_TOTAL`.
- `MANDATORY`: Both `ONE_TO_ONE` and `TOTAL`.

You can combine arbitrary flags with the `|` separator (e.g. `Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL`).

We introduce a function `execute` that takes a given cursor, and executes the sql with validation.  The result of the function is a `Results` object, which has fields:

- `status`: One of an enum which returns `OK` if there is no validation or sql error, `VALIDATION_ERROR` if there's a validation error, `SQL_ERROR` if the execution returns some other error, or `NOT_VALIDATED` if validation is bypassed (see below).
- `value`: The resulting sql results, if no error.
- `error_msg`: A short text describing the validation error, if there is one.
- `error_loc`: Text referring to the join where the validation error occurs, if there is one.
- `error_size`: The number of rows that fail validation.
- `error_sample`: A list of tuples of the first 10 rows that fail validation.

This function can be run with the optional argument `skip_validation=True`, then none of the validation steps would be run. This would then just be a standard execution. (This would be ideal for production.)

```python
def execute(cursor: pCursor, query: Query, skip_validation: bool = False) -> Results:
    ...

    if skip_validation:
        sql = query.get_sql()
        return Results(status=Status.NOT_VALIDATED, value=cursor.execute(sql))
```

Example usage:

```python
result = execute(cursor, query)
if result.status == Status.VALIDATION_ERROR:
    print(f"Validation failed at {result.error_loc}: {result.error_msg}")
    print(f"Sample of {result.error_size} failing rows:")
    for row in result.error_sample:
        print(row)
elif result.status == Status.OK:
    for row in result.value:
        print(row)
```

### Multi-table joins

If we have a multi-table join, we will evaluate these in order from left-to-right.  For example:

```
Query.from_(x).inner_join(y, validate=Validate.ONE_TO_ONE).inner_join(z, validate=Validate.TOTAL)
```

This will first check that the join of x and y is 1:1. Then it will check that the join of (x JOIN y) to z is total. This is different from checking that y JOIN z is total, followed by x JOIN (y JOIN z) is 1:1. The left-to-right ordering is consistent with SQL joining logic.

## Internals

`execute` uses a validate-then-execute pattern.  If validation fails, then the execution step will not proceed.  The `Results` class contains the number of errant rows AND a sample of 10 of the errant rows; these are calculated with separate SQL queries.

The validate-then-execute pattern does not provide any atomicity guarantees.  It may happen that between validating and executing, the underlying table has changed so that it no longer satisfies the validation constraint.

`execute` takes a "cursor" and returns "value" as a field on `Results`.  The types of these will depend on SQL engine, but we will make pseudotypes `pCursor = Any` and `pValue = Any`.

### What the library doesn't do

As a principle, we don't try to optimize; this keeps the implementation simple, and optimizing could be counterproductive given so many SQL implementations. (Some work may be saved by server-side caching.) The validation queries use correlated subqueries which may not be optimal for very large tables; consider the performance implications for your data volumes. This means:

1. The library does not share information between the validations and the execution. For example, it does not use the result of the query to then check for new duplicates on a 1:1. Although this may be faster, it would force that check to happen locally, instead of sending the work to the SQL server.
2. The library does not share information between validations. For example, when we check every join of ((x JOIN y) JOIN z) JOIN w, we may be able to use results from one check to perform the next check.

The library follows a validate-then-execute pattern, but the execute part is not modified by the validation. For example, the query plan might prefer a hash join if it knew the tables were 1:1. We don't communicate this back to the SQL program; the execution would be the same with or without the validation. This may be a subject of a future improvement.

The library always validates at the moment of joining. It may happen that after applying a WHERE filter, the relevant rows are 1:1 but the entire table is not 1:1. The validation would fail in this case. You may be able to prefilter the tables before joining with a subquery. If not, this is a more complex validation than the library is capable of.

