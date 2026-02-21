"""
Example: Composite key joins

This example demonstrates that validation works correctly with multi-column
(composite key) ON clauses, not just simple single-column equality.

The key insight: a single column (e.g. sku=1) may appear in multiple rows
without violating MANY_TO_ONE, as long as the *composite* key (category, sku)
is unique.  A column-level uniqueness check would incorrectly raise a violation
in this case; pypika-validate uses the full ON criterion instead.
"""

import logging
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)

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

log.info("Composite key join — MANY_TO_ONE (expect OK):")
result = execute(cursor, query_ok, verbose=True)
log.info("Status: %s", result.status.name)
assert result.status == Status.OK, f"Expected OK, got {result.status}"
for row in result.value:
    log.info("  %s", row)

# ----------------------------------------------------------------
# Failing case: add a duplicate (electronics, 1) entry in prices
# ----------------------------------------------------------------
cursor.execute("INSERT INTO prices VALUES ('electronics', 1, 888.0)")  # duplicate composite key

log.info("After adding duplicate composite key (expect VALIDATION_ERROR):")
result_bad = execute(cursor, query_ok, verbose=True)
log.info("Status: %s", result_bad.status.name)
assert result_bad.status == Status.VALIDATION_ERROR
log.info("  Error: %s", result_bad.error_msg)
log.info("  Violation count: %s", result_bad.error_size)
log.info("  Sample: %s", result_bad.error_sample)

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

log.info("LEFT_TOTAL with missing price for 'Brick' (expect VALIDATION_ERROR):")
result_total = execute(cursor, query_total, verbose=True)
log.info("Status: %s", result_total.status.name)
assert result_total.status == Status.VALIDATION_ERROR
log.info("  Error: %s", result_total.error_msg)
log.info("  Unmatched rows: %s", result_total.error_sample)

conn.close()
log.info("All assertions passed.")
