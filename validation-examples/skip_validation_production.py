"""
Example: Skipping validation in production

In production environments, you may want to skip validation for performance.
Use skip_validation=True to bypass all validation checks while keeping
the validation declarations in your code for documentation.
"""

import logging
import os
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

# Check environment to decide whether to validate
# In production, set PYPIKA_SKIP_VALIDATION=1
is_production = os.environ.get("PYPIKA_SKIP_VALIDATION", "0") == "1"

# Enable debug logging only in development (verbose=True below)
log_level = logging.INFO if is_production else logging.DEBUG
logging.basicConfig(level=log_level, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)

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

log.info("Environment: %s", "PRODUCTION" if is_production else "DEVELOPMENT")
log.info("Validation: %s", "SKIPPED" if is_production else "ENABLED")

# verbose only in development; skip_validation only in production
result = execute(cursor, query, skip_validation=is_production, verbose=not is_production)

if result.status == Status.NOT_VALIDATED:
    log.info("Query executed without validation (production mode)")
    log.info("Results:")
    for row in result.value:
        log.info("  %s", row)
elif result.status == Status.OK:
    log.info("Validation passed (development mode)")
    log.info("Results:")
    for row in result.value:
        log.info("  %s", row)
elif result.status == Status.VALIDATION_ERROR:
    log.info("Validation failed (development mode)")
    log.info("Error: %s", result.error_msg)
    # In development, this would alert you to data issues
    # before they cause subtle bugs in production

conn.close()

log.info("Tip: Run with PYPIKA_SKIP_VALIDATION=1 to simulate production mode")
