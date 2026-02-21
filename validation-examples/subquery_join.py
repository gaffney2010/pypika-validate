"""
Example: Subquery joins

This example demonstrates that validation works correctly when the right-hand
side of a join is a subquery (an aliased QueryBuilder) rather than a plain
table.  The same cardinality flags apply; pypika-validate executes the
subquery inline inside the validation correlated subqueries.

Scenario
--------
A products table is joined to a *filtered* view of order_items that rolls up
only fulfilled orders.  We validate that every product has exactly one
aggregated row (MANY_TO_ONE) and that every product appears in the fulfillment
data (LEFT_TOTAL).
"""

import logging
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)

conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
cursor.execute("CREATE TABLE order_items (id INTEGER, product_id INTEGER, qty INTEGER, fulfilled INTEGER)")

cursor.execute("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')")
cursor.execute("INSERT INTO order_items VALUES " "(1, 1, 5, 1), (2, 2, 3, 1), (3, 3, 2, 1)")

products = Table("products")
order_items = Table("order_items")

# Subquery: only fulfilled orders, aggregated per product
fulfilled = (
    Query.from_(order_items)
    .where(order_items.fulfilled == 1)
    .select(order_items.product_id, order_items.qty)
    .as_("fulfilled")
)

# ----------------------------------------------------------------
# MANY_TO_ONE: each product maps to exactly one fulfilled row
# ----------------------------------------------------------------
query_m2o = (
    Query.from_(products)
    .join(fulfilled, validate=Validate.MANY_TO_ONE)
    .on(products.id == fulfilled.product_id)
    .select(products.name, fulfilled.qty)
)

log.info("MANY_TO_ONE against subquery (expect OK):")
result = execute(cursor, query_m2o, verbose=True)
log.info("Status: %s", result.status.name)
assert result.status == Status.OK
for row in result.value:
    log.info("  %s", row)

# ----------------------------------------------------------------
# Add a duplicate fulfilled row for product 1 — now MANY_TO_ONE fails
# ----------------------------------------------------------------
cursor.execute("INSERT INTO order_items VALUES (4, 1, 8, 1)")

log.info("After duplicate fulfilled row (expect VALIDATION_ERROR):")
result_bad = execute(cursor, query_m2o, verbose=True)
log.info("Status: %s", result_bad.status.name)
assert result_bad.status == Status.VALIDATION_ERROR
log.info("  Error: %s", result_bad.error_msg)
log.info("  Failing rows (products with multiple fulfilled entries): %s", result_bad.error_sample)

# ----------------------------------------------------------------
# LEFT_TOTAL: every product must appear in fulfilled orders
# ----------------------------------------------------------------
cursor.execute("DELETE FROM order_items WHERE id = 4")  # restore clean state
cursor.execute("INSERT INTO products VALUES (4, 'Orphan')")  # no fulfilled row

query_lt = (
    Query.from_(products)
    .join(fulfilled, validate=Validate.LEFT_TOTAL)
    .on(products.id == fulfilled.product_id)
    .select(products.name, fulfilled.qty)
)

log.info("LEFT_TOTAL with missing product in subquery (expect VALIDATION_ERROR):")
result_lt = execute(cursor, query_lt, verbose=True)
log.info("Status: %s", result_lt.status.name)
assert result_lt.status == Status.VALIDATION_ERROR
log.info("  Error: %s", result_lt.error_msg)
log.info("  Products with no fulfilled orders: %s", result_lt.error_sample)

conn.close()
log.info("All assertions passed.")
