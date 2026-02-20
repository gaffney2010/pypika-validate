"""
Example: LEFT_TOTAL and RIGHT_TOTAL validation

This example demonstrates validating that joins have total coverage:
- LEFT_TOTAL: Every row on the left has at least one match on the right
- RIGHT_TOTAL: Every row on the right has at least one match on the left

This is useful for ensuring referential integrity without foreign keys,
or validating that all expected data is present.
"""

import sqlite3

from pypika import Query, Table
from pypika.validation import Validate, Status, execute

# Create tables
employees = Table("employees")
departments = Table("departments")

# Build a query with LEFT_TOTAL validation
# This ensures every employee belongs to an existing department
query_left_total = (
    Query.from_(employees)
    .join(departments, validate=Validate.LEFT_TOTAL)
    .on(employees.dept_id == departments.id)
    .select(employees.name, departments.dept_name)
)

# Build a query with RIGHT_TOTAL validation
# This ensures every department has at least one employee
query_right_total = (
    Query.from_(employees)
    .join(departments, validate=Validate.RIGHT_TOTAL)
    .on(employees.dept_id == departments.id)
    .select(employees.name, departments.dept_name)
)

# Build a query with TOTAL validation (both directions)
query_total = (
    Query.from_(employees)
    .join(departments, validate=Validate.TOTAL)
    .on(employees.dept_id == departments.id)
    .select(employees.name, departments.dept_name)
)

# Connect to database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

# Set up test data
cursor.execute("CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)")
cursor.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)")
cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
cursor.execute("""
    INSERT INTO employees VALUES
    (1, 'Alice', 1),
    (2, 'Bob', 1),
    (3, 'Carol', 2)
""")
# Note: No employees in HR (dept_id=3), so RIGHT_TOTAL will fail

print("Testing LEFT_TOTAL (every employee has a department):")
result = execute(cursor, query_left_total)
if result.status == Status.OK:
    print("  PASSED - All employees belong to valid departments")
elif result.status == Status.VALIDATION_ERROR:
    print(f"  FAILED - {result.error_size} employees have invalid department IDs")

print("\nTesting RIGHT_TOTAL (every department has employees):")
result = execute(cursor, query_right_total)
if result.status == Status.OK:
    print("  PASSED - All departments have employees")
elif result.status == Status.VALIDATION_ERROR:
    print(f"  FAILED - {result.error_size} departments have no employees")
    print(f"  Sample: {result.error_sample}")

print("\nTesting TOTAL (both directions):")
result = execute(cursor, query_total)
if result.status == Status.OK:
    print("  PASSED - Complete coverage in both directions")
elif result.status == Status.VALIDATION_ERROR:
    print(f"  FAILED - {result.error_msg}")

conn.close()
