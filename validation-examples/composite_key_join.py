"""
Example: Composite key joins

This example demonstrates that validation works correctly with multi-column
(composite key) ON clauses, not just simple single-column equality.

The key insight: a single column (e.g. sku=1) may appear in multiple rows
without violating MANY_TO_ONE, as long as the *composite* key (category, sku)
is unique.  A column-level uniqueness check would incorrectly raise a violation
in this case; pypika-validate uses the full ON criterion instead.
"""

import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute


conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute("CREATE TABLE products (category TEXT, sku INTEGER, name TEXT)")
cursor.execute("CREATE TABLE prices (category TEXT, sku INTEGER, price REAL)")

# sku=1 appears in two categories — single-column check would be wrong here
cursor.execute(
    "INSERT INTO products VALUES "
    "('electronics', 1, 'Phone'), ('electronics', 2, 'Tablet'), ('books', 1, 'Novel')"
)
cursor.execute(
    "INSERT INTO prices VALUES "
    "('electronics', 1, 999.0), ('electronics', 2, 599.0), ('books', 1, 19.99)"
)

products = Table("products")
prices = Table("prices")

# ----------------------------------------------------------------
# Passing case: composite key (category, sku) is unique in prices
# ----------------------------------------------------------------
query_ok = (
    Query.from_(products)
    .join(prices, validate=Validate.MANY_TO_ONE)
    .on((products.category == prices.category) & (products.sku == prices.sku))
    .select(products.name, prices.price)
)

result = execute(cursor, query_ok)
print(f"Status (expect OK): {result.status.name}")
assert result.status == Status.OK, f"Expected OK, got {result.status}"
for row in result.value:
    print(f"  {row}")

# ----------------------------------------------------------------
# Failing case: add a duplicate (electronics, 1) entry in prices
# ----------------------------------------------------------------
cursor.execute("INSERT INTO prices VALUES ('electronics', 1, 888.0)")  # duplicate composite key

result_bad = execute(cursor, query_ok)
print(f"\nStatus (expect VALIDATION_ERROR): {result_bad.status.name}")
assert result_bad.status == Status.VALIDATION_ERROR
print(f"  Error: {result_bad.error_msg}")
print(f"  Violation count: {result_bad.error_size}")
print(f"  Sample: {result_bad.error_sample}")

# ----------------------------------------------------------------
# LEFT_TOTAL on composite key: every product must have a price
# ----------------------------------------------------------------
cursor.execute("DELETE FROM prices WHERE price = 888.0")  # restore clean data
cursor.execute("INSERT INTO products VALUES ('toys', 5, 'Brick')")  # no matching price

query_total = (
    Query.from_(products)
    .join(prices, validate=Validate.LEFT_TOTAL)
    .on((products.category == prices.category) & (products.sku == prices.sku))
    .select(products.name, prices.price)
)

result_total = execute(cursor, query_total)
print(f"\nStatus (expect VALIDATION_ERROR for missing price): {result_total.status.name}")
assert result_total.status == Status.VALIDATION_ERROR
print(f"  Error: {result_total.error_msg}")
print(f"  Unmatched rows: {result_total.error_sample}")

conn.close()
print("\nAll assertions passed.")
