"""
Example: Multi-table joins with validation

This example demonstrates validation with multiple joined tables.
Validations are evaluated left-to-right:
1. First, validate the join of x and y
2. Then, validate the join of (x JOIN y) to z

This is important because the cardinality of the combined result
may differ from individual tables.
"""

import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

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
cursor.execute("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER)")
cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")

cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (2, 'Bob')")
cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99), (3, 'Gizmo', 29.99)")
cursor.execute("""
    INSERT INTO order_items VALUES
    (1, 1, 1, 2),  -- Order 1: 2 Widgets
    (2, 1, 2, 1),  -- Order 1: 1 Gadget
    (3, 2, 1, 5),  -- Order 2: 5 Widgets
    (4, 2, 3, 1)   -- Order 2: 1 Gizmo
""")

print("Multi-table join with step-by-step validation:")
print("  orders -> order_items: ONE_TO_MANY (one order, many items)")
print("  (result) -> products: MANY_TO_ONE (many items, one product each)")
print()

result = execute(cursor, query)

if result.status == Status.OK:
    print("All validations PASSED!")
    print("\nQuery results:")
    for row in result.value:
        order_id, qty, product_name, price = row
        print(f"  Order {order_id}: {qty}x {product_name} @ ${price}")
elif result.status == Status.VALIDATION_ERROR:
    print(f"Validation FAILED at: {result.error_loc}")
    print(f"Error: {result.error_msg}")
    print(f"Violations: {result.error_size}")

conn.close()
