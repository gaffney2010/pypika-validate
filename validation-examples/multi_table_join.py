"""
Example: Multi-table joins with validation

This example demonstrates validation with multiple joined tables.
Validations are evaluated left-to-right:
1. First, validate the join of x and y
2. Then, validate the join of (x JOIN y) to z

This is important because the cardinality of the combined result
may differ from individual tables.
"""

import logging
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# Create tables for an e-commerce scenario
orders = Table("orders")
order_items = Table("order_items")
products = Table("products")

# Build a multi-table query with validation at each step
# 1. orders JOIN order_items should be ONE_TO_MANY
#    (one order has many items)
# 2. (orders JOIN order_items) JOIN products should be MANY_TO_ONE
#    (many order items reference one product each)
query = (
    Query.from_(orders)
    .join(order_items, validate=Validate.ONE_TO_MANY)
    .on(orders.id == order_items.order_id)
    .join(products, validate=Validate.MANY_TO_ONE)
    .on(order_items.product_id == products.id)
    .select(orders.id, order_items.quantity, products.name, products.price)
)

# Connect to database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

# Set up test data
cursor.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_name TEXT)")
cursor.execute(
    "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER)"
)
cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")

cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (2, 'Bob')")
cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99), (3, 'Gizmo', 29.99)")
cursor.execute("""
    INSERT INTO order_items VALUES
    (1, 1, 1, 2),
    (2, 1, 2, 1),
    (3, 2, 1, 5),
    (4, 2, 3, 1)
""")

log.info("Multi-table join with step-by-step validation:")
log.info("  orders -> order_items: ONE_TO_MANY (one order, many items)")
log.info("  (result) -> products: MANY_TO_ONE (many items, one product each)")

result = execute(cursor, query, verbose=True)

if result.status == Status.OK:
    log.info("All validations PASSED!")
    log.info("Query results:")
    for row in result.value:
        order_id, qty, product_name, price = row
        log.info("  Order %s: %sx %s @ $%s", order_id, qty, product_name, price)
elif result.status == Status.VALIDATION_ERROR:
    log.info("Validation FAILED at: %s", result.error_loc)
    log.info("Error: %s", result.error_msg)
    log.info("Violations: %s", result.error_size)

conn.close()
