"""
Example: Basic ONE_TO_ONE validation

This example demonstrates validating that a join between users and user_profiles
is truly one-to-one (each user has exactly one profile, each profile belongs to
exactly one user).
"""

import logging
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# Create tables
users = Table("users")
user_profiles = Table("user_profiles")

# Build a query with ONE_TO_ONE validation
query = (
    Query.from_(users)
    .join(user_profiles, validate=Validate.ONE_TO_ONE)
    .on(users.id == user_profiles.user_id)
    .select(users.name, user_profiles.bio)
)

# Connect to database and execute with validation
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

# Set up test data
cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
cursor.execute("CREATE TABLE user_profiles (id INTEGER PRIMARY KEY, user_id INTEGER, bio TEXT)")
cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
cursor.execute("INSERT INTO user_profiles VALUES (1, 1, 'Alice bio'), (2, 2, 'Bob bio')")

# Execute the query with validation
result = execute(cursor, query, verbose=True)

if result.status == Status.OK:
    log.info("Validation passed! Query results:")
    for row in result.value:
        log.info("  %s", row)
elif result.status == Status.VALIDATION_ERROR:
    log.info("Validation failed at %s", result.error_loc)
    log.info("Error: %s", result.error_msg)
    log.info("Number of violations: %s", result.error_size)
    log.info("Sample of failing rows:")
    for row in result.error_sample:
        log.info("  %s", row)

conn.close()
