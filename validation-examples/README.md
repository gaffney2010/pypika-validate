# pypika-validate Examples

This directory contains example scripts demonstrating the validation features of pypika-validate.

## Examples

### Basic Usage

- **basic_one_to_one.py** - Simple ONE_TO_ONE validation between users and profiles
- **many_to_one_lookup.py** - MANY_TO_ONE validation for lookup/dimension tables

### Coverage Validation

- **total_coverage.py** - LEFT_TOTAL, RIGHT_TOTAL, and TOTAL validation for referential integrity

### Advanced Usage

- **combined_flags.py** - Combining validation flags with the `|` operator
- **multi_table_join.py** - Validation across multiple joined tables (left-to-right evaluation)
- **composite_key_join.py** - Joins on composite (multi-column) keys; demonstrates that single-column duplicates do not trigger false violations
- **subquery_join.py** - Joins where the right-hand side is an aliased subquery; the subquery's WHERE filter affects which rows are in scope for validation

### Production Patterns

- **skip_validation_production.py** - Skipping validation in production while keeping it in development
- **handling_errors.py** - Handling different error types from the execute function

## Running the Examples

These examples require the validation module to be implemented. Once implemented, run them with:

```bash
python validation-examples/basic_one_to_one.py
```

## Validation Flags Reference

| Flag | Description |
|------|-------------|
| `ONE_TO_MANY` | For every row on the right, at most 1 entry on the left |
| `MANY_TO_ONE` | For every row on the left, at most 1 entry on the right |
| `ONE_TO_ONE` | Both ONE_TO_MANY and MANY_TO_ONE |
| `LEFT_TOTAL` | Every left row has at least 1 match on the right |
| `RIGHT_TOTAL` | Every right row has at least 1 match on the left |
| `TOTAL` | Both LEFT_TOTAL and RIGHT_TOTAL |
| `MANDATORY` | Both ONE_TO_ONE and TOTAL |

Flags can be combined: `Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL`
