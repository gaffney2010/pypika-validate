"""
Example: Handling validation errors

This example demonstrates how to handle different types of errors
returned by the execute function:
- VALIDATION_ERROR: Join cardinality constraints violated
- SQL_ERROR: Database error during execution
- OK: Everything succeeded
- NOT_VALIDATED: Validation was skipped
"""

import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, Results, execute


def handle_result(result: Results, query_name: str) -> None:
    """Handle the result of a validated query execution."""
    print(f"\n{'=' * 50}")
    print(f"Query: {query_name}")
    print(f"Status: {result.status.name}")
    print("=" * 50)

    if result.status == Status.OK:
        print("SUCCESS: Query executed and all validations passed")
        print(f"Rows returned: {len(result.value)}")
        for row in result.value[:5]:  # Show first 5 rows
            print(f"  {row}")
        if len(result.value) > 5:
            print(f"  ... and {len(result.value) - 5} more rows")

    elif result.status == Status.VALIDATION_ERROR:
        print("VALIDATION FAILED:")
        print(f"  Location: {result.error_loc}")
        print(f"  Message: {result.error_msg}")
        print(f"  Violation count: {result.error_size}")
        print("  Sample of violations:")
        for row in result.error_sample:
            print(f"    {row}")

    elif result.status == Status.SQL_ERROR:
        print("SQL ERROR:")
        print(f"  Message: {result.error_msg}")
        # In production, you might want to log this and alert

    elif result.status == Status.NOT_VALIDATED:
        print("SKIPPED: Validation was not performed")
        print(f"Rows returned: {len(result.value)}")


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
    (2, 1, 'Alice profile 2'),  -- Duplicate! Alice has 2 profiles
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
handle_result(execute(cursor, query_one_to_one), "ONE_TO_ONE (expected to fail)")
handle_result(execute(cursor, query_many_to_one), "MANY_TO_ONE (expected to pass)")
handle_result(execute(cursor, query_skip, skip_validation=True), "Skipped validation")

conn.close()
