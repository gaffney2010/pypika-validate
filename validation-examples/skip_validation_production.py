"""
Example: Skipping validation in production

In production environments, you may want to skip validation for performance.
Use skip_validation=True to bypass all validation checks while keeping
the validation declarations in your code for documentation.
"""

import sqlite3
import os

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

# Create tables
users = Table("users")
orders = Table("orders")

# Build a query with validation
# The validation serves as documentation of expected behavior
query = (
    Query.from_(users)
    .join(orders, validate=Validate.ONE_TO_MANY)
    .on(users.id == orders.user_id)
    .select(users.name, orders.total)
)

# Connect to database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
cursor.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")
cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
cursor.execute("INSERT INTO orders VALUES (1, 1, 100.00), (2, 1, 200.00), (3, 2, 150.00)")

# Check environment to decide whether to validate
# In production, set PYPIKA_SKIP_VALIDATION=1
is_production = os.environ.get("PYPIKA_SKIP_VALIDATION", "0") == "1"

print(f"Environment: {'PRODUCTION' if is_production else 'DEVELOPMENT'}")
print(f"Validation: {'SKIPPED' if is_production else 'ENABLED'}")
print()

result = execute(cursor, query, skip_validation=is_production)

if result.status == Status.NOT_VALIDATED:
    print("Query executed without validation (production mode)")
    print("Results:")
    for row in result.value:
        print(f"  {row}")
elif result.status == Status.OK:
    print("Validation passed (development mode)")
    print("Results:")
    for row in result.value:
        print(f"  {row}")
elif result.status == Status.VALIDATION_ERROR:
    print("Validation failed (development mode)")
    print(f"Error: {result.error_msg}")
    # In development, this would alert you to data issues
    # before they cause subtle bugs in production

conn.close()

print()
print("Tip: Run with PYPIKA_SKIP_VALIDATION=1 to simulate production mode")
