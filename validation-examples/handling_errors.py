"""
Example: Handling validation errors

This example demonstrates how to handle different types of errors
returned by the execute function:
- VALIDATION_ERROR: Join cardinality constraints violated
- SQL_ERROR: Database error during execution
- OK: Everything succeeded
- NOT_VALIDATED: Validation was skipped
"""

import logging
import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, Results, execute

logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def handle_result(result: Results, query_name: str) -> None:
    """Handle the result of a validated query execution."""
    log.info("=" * 50)
    log.info("Query: %s", query_name)
    log.info("Status: %s", result.status.name)
    log.info("=" * 50)

    if result.status == Status.OK:
        log.info("SUCCESS: Query executed and all validations passed")
        log.info("Rows returned: %d", len(result.value))
        for row in result.value[:5]:  # Show first 5 rows
            log.info("  %s", row)
        if len(result.value) > 5:
            log.info("  ... and %d more rows", len(result.value) - 5)

    elif result.status == Status.VALIDATION_ERROR:
        log.info("VALIDATION FAILED:")
        log.info("  Location: %s", result.error_loc)
        log.info("  Message: %s", result.error_msg)
        log.info("  Violation count: %s", result.error_size)
        log.info("  Sample of violations:")
        for row in result.error_sample:
            log.info("    %s", row)

    elif result.status == Status.SQL_ERROR:
        log.info("SQL ERROR:")
        log.info("  Message: %s", result.error_msg)
        # In production, you might want to log this and alert

    elif result.status == Status.NOT_VALIDATED:
        log.info("SKIPPED: Validation was not performed")
        log.info("Rows returned: %d", len(result.value))


# Set up test database with intentional issues
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
cursor.execute("CREATE TABLE profiles (id INTEGER PRIMARY KEY, user_id INTEGER, bio TEXT)")

# Insert data with a duplicate user_id (violates ONE_TO_ONE)
cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
cursor.execute("""
    INSERT INTO profiles VALUES
    (1, 1, 'Alice profile 1'),
    (2, 1, 'Alice profile 2'),
    (3, 2, 'Bob profile')
""")

users = Table("users")
profiles = Table("profiles")

# Query 1: This should fail validation (duplicate profiles)
query_one_to_one = (
    Query.from_(users)
    .join(profiles, validate=Validate.ONE_TO_ONE)
    .on(users.id == profiles.user_id)
    .select(users.name, profiles.bio)
)

# Query 2: This should pass (MANY_TO_ONE allows duplicates on left)
query_many_to_one = (
    Query.from_(profiles)
    .join(users, validate=Validate.MANY_TO_ONE)
    .on(profiles.user_id == users.id)
    .select(profiles.bio, users.name)
)

# Query 3: Skip validation
query_skip = (
    Query.from_(users)
    .join(profiles, validate=Validate.ONE_TO_ONE)
    .on(users.id == profiles.user_id)
    .select(users.name, profiles.bio)
)

# Execute all queries
handle_result(execute(cursor, query_one_to_one, verbose=True), "ONE_TO_ONE (expected to fail)")
handle_result(execute(cursor, query_many_to_one, verbose=True), "MANY_TO_ONE (expected to pass)")
handle_result(execute(cursor, query_skip, skip_validation=True), "Skipped validation")

conn.close()
