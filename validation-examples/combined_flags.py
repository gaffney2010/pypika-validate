"""
Example: Combining validation flags

This example demonstrates combining multiple validation flags using the | operator.

Common combinations:
- MANY_TO_ONE | RIGHT_TOTAL: Lookup table join where every lookup value must exist
- ONE_TO_ONE | TOTAL (= MANDATORY): Bijective relationship, all rows must match
"""

import logging
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# Create tables
products = Table("products")
categories = Table("categories")
prices = Table("prices")

# Example 1: MANY_TO_ONE | RIGHT_TOTAL
# Products join to categories where:
# - Each product belongs to exactly one category (MANY_TO_ONE)
# - Every category has at least one product (RIGHT_TOTAL)
query_category_check = (
    Query.from_(products)
    .join(categories, validate=Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL)
    .on(products.category_id == categories.id)
    .select(products.name, categories.category_name)
)

# Example 2: MANDATORY (= ONE_TO_ONE | TOTAL)
# Products join to prices where every product has exactly one price
# and every price belongs to exactly one product
query_price_check = (
    Query.from_(products)
    .join(prices, validate=Validate.MANDATORY)
    .on(products.id == prices.product_id)
    .select(products.name, prices.amount)
)

# Connect to database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

# Set up test data
cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER)")
cursor.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, category_name TEXT)")
cursor.execute("CREATE TABLE prices (id INTEGER PRIMARY KEY, product_id INTEGER, amount REAL)")

cursor.execute("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing')")
cursor.execute("INSERT INTO products VALUES (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Shirt', 2)")
cursor.execute("INSERT INTO prices VALUES (1, 1, 999.99), (2, 2, 599.99), (3, 3, 29.99)")

log.info("Testing MANY_TO_ONE | RIGHT_TOTAL on products-categories:")
result = execute(cursor, query_category_check, verbose=True)
if result.status == Status.OK:
    log.info("  PASSED")
    log.info("  - Each product has exactly one category")
    log.info("  - Every category has at least one product")
elif result.status == Status.VALIDATION_ERROR:
    log.info("  FAILED: %s", result.error_msg)

log.info("Testing MANDATORY on products-prices:")
result = execute(cursor, query_price_check, verbose=True)
if result.status == Status.OK:
    log.info("  PASSED")
    log.info("  - Each product has exactly one price")
    log.info("  - Each price belongs to exactly one product")
    log.info("  - No products without prices, no orphan prices")
elif result.status == Status.VALIDATION_ERROR:
    log.info("  FAILED: %s", result.error_msg)

conn.close()
