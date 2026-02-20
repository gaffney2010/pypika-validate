"""
Example: MANY_TO_ONE validation for lookup tables

This example demonstrates validating that a join from orders to customers
is many-to-one (many orders can belong to one customer, but each order
belongs to exactly one customer).

This is useful when joining to lookup/dimension tables where you expect
the lookup key to be unique.
"""

import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

# Create tables
orders = Table("orders")
customers = Table("customers")

# Build a query with MANY_TO_ONE validation
# This ensures each order maps to exactly one customer
query = (
    Query.from_(orders)
    .join(customers, validate=Validate.MANY_TO_ONE)
    .on(orders.customer_id == customers.id)
    .select(orders.order_id, orders.amount, customers.name)
)

# Connect to database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

# Set up test data - GOOD case: customer IDs are unique
cursor.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, amount REAL)")
cursor.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
cursor.execute("INSERT INTO customers VALUES (1, 'Acme Corp'), (2, 'Globex Inc')")
cursor.execute("""
    INSERT INTO orders VALUES
    (101, 1, 500.00),
    (102, 1, 750.00),
    (103, 2, 1200.00)
""")

result = execute(cursor, query)

if result.status == Status.OK:
    print("MANY_TO_ONE validation passed!")
    print("Each order correctly maps to exactly one customer.")
    for row in result.value:
        print(f"  Order {row[0]}: ${row[1]} from {row[2]}")
elif result.status == Status.VALIDATION_ERROR:
    print("MANY_TO_ONE validation FAILED!")
    print(f"Found duplicate customer IDs: {result.error_msg}")
    print("This means some orders would match multiple customers.")

conn.close()
