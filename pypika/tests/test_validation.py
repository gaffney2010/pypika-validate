"""Tests for pypika.validation module behavior.

Tests use in-memory SQLite databases to verify validation behavior end-to-end,
matching the scenarios demonstrated in validation-examples/.
"""

import sqlite3
import unittest

from pypika import Query, Table
from pypika.validation import Validate, Status, execute


class ValidateFlagCombinationTests(unittest.TestCase):
    """Validate flags should combine according to documented semantics."""

    def test_one_to_one_equals_one_to_many_and_many_to_one(self):
        self.assertEqual(Validate.ONE_TO_ONE, Validate.ONE_TO_MANY | Validate.MANY_TO_ONE)

    def test_total_equals_left_total_and_right_total(self):
        self.assertEqual(Validate.TOTAL, Validate.LEFT_TOTAL | Validate.RIGHT_TOTAL)

    def test_mandatory_equals_one_to_one_or_total(self):
        self.assertEqual(Validate.MANDATORY, Validate.ONE_TO_ONE | Validate.TOTAL)

    def test_flags_can_be_combined_with_or_operator(self):
        combined = Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL
        self.assertIsNotNone(combined)


class SkipValidationTests(unittest.TestCase):
    """skip_validation=True should bypass all checks and return NOT_VALIDATED."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, total REAL)")
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)")
        self.users = Table("users")
        self.orders = Table("orders")

    def tearDown(self):
        self.conn.close()

    def test_skip_validation_returns_not_validated_status(self):
        query = (
            Query.from_(self.users)
            .join(self.orders, validate=Validate.ONE_TO_MANY)
            .on(self.users.id == self.orders.user_id)
            .select(self.users.name, self.orders.total)
        )
        result = execute(self.cursor, query, skip_validation=True)
        self.assertEqual(result.status, Status.NOT_VALIDATED)

    def test_skip_validation_still_returns_query_results(self):
        query = (
            Query.from_(self.users)
            .join(self.orders, validate=Validate.ONE_TO_MANY)
            .on(self.users.id == self.orders.user_id)
            .select(self.users.name, self.orders.total)
        )
        result = execute(self.cursor, query, skip_validation=True)
        self.assertEqual(result.status, Status.NOT_VALIDATED)
        self.assertIsNotNone(result.value)
        self.assertGreater(len(result.value), 0)

    def test_skip_validation_bypasses_checks_even_for_violating_data(self):
        """Data that would fail validation returns NOT_VALIDATED when skipped."""
        self.cursor.execute("CREATE TABLE profiles (id INTEGER, user_id INTEGER, bio TEXT)")
        # user_id=1 is duplicated, would fail ONE_TO_ONE
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        profiles = Table("profiles")
        query = (
            Query.from_(self.users)
            .join(profiles, validate=Validate.ONE_TO_ONE)
            .on(self.users.id == profiles.user_id)
            .select(self.users.name, profiles.bio)
        )
        result = execute(self.cursor, query, skip_validation=True)
        self.assertEqual(result.status, Status.NOT_VALIDATED)


class OneToOneTests(unittest.TestCase):
    """ONE_TO_ONE requires unique join keys on both left and right sides."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE profiles (id INTEGER PRIMARY KEY, user_id INTEGER, bio TEXT)")
        self.users = Table("users")
        self.profiles = Table("profiles")

    def tearDown(self):
        self.conn.close()

    def _query(self):
        return (
            Query.from_(self.users)
            .join(self.profiles, validate=Validate.ONE_TO_ONE)
            .on(self.users.id == self.profiles.user_id)
            .select(self.users.name, self.profiles.bio)
        )

    def test_passes_with_clean_one_to_one_data(self):
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'Alice bio'), (2, 2, 'Bob bio')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)

    def test_ok_result_contains_query_rows(self):
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'Alice bio'), (2, 2, 'Bob bio')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)
        self.assertIsNotNone(result.value)
        self.assertEqual(len(result.value), 2)

    def test_fails_when_right_side_has_duplicate_join_key(self):
        """Alice having two profiles violates ONE_TO_ONE."""
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_failure_provides_error_message(self):
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)
        self.assertIsNotNone(result.error_msg)
        self.assertGreater(len(result.error_msg), 0)

    def test_failure_provides_error_location(self):
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)
        self.assertIsNotNone(result.error_loc)

    def test_failure_provides_violation_count(self):
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)
        self.assertIsNotNone(result.error_size)
        self.assertGreater(result.error_size, 0)

    def test_failure_provides_sample_of_failing_rows(self):
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)
        self.assertIsNotNone(result.error_sample)
        self.assertGreater(len(result.error_sample), 0)

    def test_error_sample_capped_at_ten_rows(self):
        """error_sample should contain at most 10 rows regardless of violation count."""
        self.cursor.execute(
            "INSERT INTO users VALUES " + ", ".join(f"({i}, 'User{i}')" for i in range(1, 22))
        )
        # All 21 profiles share user_id=1, generating many violations
        self.cursor.execute(
            "INSERT INTO profiles VALUES " + ", ".join(f"({i}, 1, 'bio{i}')" for i in range(1, 22))
        )
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)
        self.assertLessEqual(len(result.error_sample), 10)


class ManyToOneTests(unittest.TestCase):
    """MANY_TO_ONE requires unique join keys on the right side only."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.orders = Table("orders")
        self.customers = Table("customers")

    def tearDown(self):
        self.conn.close()

    def test_passes_when_right_key_is_unique(self):
        """Many orders can reference the same customer; customer IDs must be unique."""
        self.cursor.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, amount REAL)")
        self.cursor.execute("INSERT INTO customers VALUES (1, 'Acme Corp'), (2, 'Globex Inc')")
        self.cursor.execute("INSERT INTO orders VALUES (101, 1, 500.0), (102, 1, 750.0), (103, 2, 1200.0)")
        query = (
            Query.from_(self.orders)
            .join(self.customers, validate=Validate.MANY_TO_ONE)
            .on(self.orders.customer_id == self.customers.id)
            .select(self.orders.order_id, self.customers.name)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.OK)

    def test_fails_when_right_key_has_duplicates(self):
        """Duplicate customer IDs cause MANY_TO_ONE to fail."""
        self.cursor.execute("CREATE TABLE customers (id INTEGER, name TEXT)")
        self.cursor.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, amount REAL)")
        self.cursor.execute("INSERT INTO customers VALUES (1, 'Acme Corp'), (1, 'Acme Duplicate')")
        self.cursor.execute("INSERT INTO orders VALUES (101, 1, 500.0)")
        query = (
            Query.from_(self.orders)
            .join(self.customers, validate=Validate.MANY_TO_ONE)
            .on(self.orders.customer_id == self.customers.id)
            .select(self.orders.order_id, self.customers.name)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_passes_when_many_left_rows_share_same_right_row(self):
        """Multiple profiles per user is fine for MANY_TO_ONE profiles->users."""
        self.cursor.execute("CREATE TABLE profiles (id INTEGER, user_id INTEGER, bio TEXT)")
        self.cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        # Alice has 2 profiles - fine for MANY_TO_ONE since users.id is still unique
        self.cursor.execute("INSERT INTO profiles VALUES (1, 1, 'p1'), (2, 1, 'p2'), (3, 2, 'p3')")
        profiles = Table("profiles")
        users = Table("users")
        query = (
            Query.from_(profiles)
            .join(users, validate=Validate.MANY_TO_ONE)
            .on(profiles.user_id == users.id)
            .select(profiles.bio, users.name)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.OK)


class OneToManyTests(unittest.TestCase):
    """ONE_TO_MANY requires unique join keys on the left side only."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE orders (id INTEGER, customer_name TEXT)")
        self.cursor.execute("CREATE TABLE order_items (id INTEGER, order_id INTEGER, product TEXT)")
        self.orders = Table("orders")
        self.order_items = Table("order_items")

    def tearDown(self):
        self.conn.close()

    def _query(self):
        return (
            Query.from_(self.orders)
            .join(self.order_items, validate=Validate.ONE_TO_MANY)
            .on(self.orders.id == self.order_items.order_id)
            .select(self.orders.id, self.order_items.product)
        )

    def test_passes_when_left_key_is_unique(self):
        """One order can have many items; order IDs must be unique."""
        self.cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO order_items VALUES (1, 1, 'Widget'), (2, 1, 'Gadget'), (3, 2, 'Gizmo')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)

    def test_fails_when_left_key_has_duplicates(self):
        """Duplicate order IDs cause ONE_TO_MANY to fail."""
        self.cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (1, 'Alice Dup')")
        self.cursor.execute("INSERT INTO order_items VALUES (1, 1, 'Widget')")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)


class LeftTotalTests(unittest.TestCase):
    """LEFT_TOTAL requires every left-side row to have at least one match on the right."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)")
        self.cursor.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)")
        self.employees = Table("employees")
        self.departments = Table("departments")

    def tearDown(self):
        self.conn.close()

    def _query(self):
        return (
            Query.from_(self.employees)
            .join(self.departments, validate=Validate.LEFT_TOTAL)
            .on(self.employees.dept_id == self.departments.id)
            .select(self.employees.name, self.departments.dept_name)
        )

    def test_passes_when_all_left_rows_have_matches(self):
        """Every employee belongs to an existing department."""
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)

    def test_fails_when_left_row_has_no_match(self):
        """An employee with an invalid dept_id causes LEFT_TOTAL to fail."""
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 99)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)


class RightTotalTests(unittest.TestCase):
    """RIGHT_TOTAL requires every right-side row to have at least one match on the left."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)")
        self.cursor.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)")
        self.employees = Table("employees")
        self.departments = Table("departments")

    def tearDown(self):
        self.conn.close()

    def _query(self):
        return (
            Query.from_(self.employees)
            .join(self.departments, validate=Validate.RIGHT_TOTAL)
            .on(self.employees.dept_id == self.departments.id)
            .select(self.employees.name, self.departments.dept_name)
        )

    def test_passes_when_all_right_rows_have_matches(self):
        """Every department has at least one employee."""
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)

    def test_fails_when_right_row_has_no_match(self):
        """HR department with no employees causes RIGHT_TOTAL to fail."""
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_failure_reports_count_and_sample_of_unmatched_rows(self):
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)
        self.assertGreater(result.error_size, 0)
        self.assertIsNotNone(result.error_sample)
        self.assertGreater(len(result.error_sample), 0)


class TotalTests(unittest.TestCase):
    """TOTAL requires full coverage in both directions (LEFT_TOTAL and RIGHT_TOTAL)."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)")
        self.cursor.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)")
        self.employees = Table("employees")
        self.departments = Table("departments")

    def tearDown(self):
        self.conn.close()

    def _query(self):
        return (
            Query.from_(self.employees)
            .join(self.departments, validate=Validate.TOTAL)
            .on(self.employees.dept_id == self.departments.id)
            .select(self.employees.name, self.departments.dept_name)
        )

    def test_passes_when_both_sides_fully_covered(self):
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)

    def test_fails_when_right_side_not_covered(self):
        """A department with no employees fails TOTAL."""
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_fails_when_left_side_not_covered(self):
        """An employee with no department fails TOTAL."""
        self.cursor.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        self.cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 99)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)


class CombinedFlagTests(unittest.TestCase):
    """Combined flags enforce all constituent constraints simultaneously."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def test_many_to_one_or_right_total_passes_with_valid_data(self):
        """Products-categories: each product has one category, every category has products."""
        self.cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER)")
        self.cursor.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, category_name TEXT)")
        self.cursor.execute("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing')")
        self.cursor.execute("INSERT INTO products VALUES (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Shirt', 2)")
        products = Table("products")
        categories = Table("categories")
        query = (
            Query.from_(products)
            .join(categories, validate=Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL)
            .on(products.category_id == categories.id)
            .select(products.name, categories.category_name)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.OK)

    def test_many_to_one_or_right_total_fails_when_category_has_no_products(self):
        """An empty category violates RIGHT_TOTAL."""
        self.cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER)")
        self.cursor.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, category_name TEXT)")
        self.cursor.execute("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing'), (3, 'Empty')")
        self.cursor.execute("INSERT INTO products VALUES (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Shirt', 2)")
        products = Table("products")
        categories = Table("categories")
        query = (
            Query.from_(products)
            .join(categories, validate=Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL)
            .on(products.category_id == categories.id)
            .select(products.name, categories.category_name)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_mandatory_passes_when_join_is_fully_bijective(self):
        """MANDATORY (ONE_TO_ONE | TOTAL): every product has exactly one price."""
        self.cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE prices (id INTEGER PRIMARY KEY, product_id INTEGER, amount REAL)")
        self.cursor.execute("INSERT INTO products VALUES (1, 'Laptop'), (2, 'Phone'), (3, 'Shirt')")
        self.cursor.execute("INSERT INTO prices VALUES (1, 1, 999.99), (2, 2, 599.99), (3, 3, 29.99)")
        products = Table("products")
        prices = Table("prices")
        query = (
            Query.from_(products)
            .join(prices, validate=Validate.MANDATORY)
            .on(products.id == prices.product_id)
            .select(products.name, prices.amount)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.OK)

    def test_mandatory_fails_when_product_has_no_price(self):
        """A product without a price violates MANDATORY (LEFT_TOTAL component)."""
        self.cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
        self.cursor.execute("CREATE TABLE prices (id INTEGER PRIMARY KEY, product_id INTEGER, amount REAL)")
        self.cursor.execute("INSERT INTO products VALUES (1, 'Laptop'), (2, 'Phone'), (3, 'Unpriceable')")
        self.cursor.execute("INSERT INTO prices VALUES (1, 1, 999.99), (2, 2, 599.99)")
        products = Table("products")
        prices = Table("prices")
        query = (
            Query.from_(products)
            .join(prices, validate=Validate.MANDATORY)
            .on(products.id == prices.product_id)
            .select(products.name, prices.amount)
        )
        result = execute(self.cursor, query)
        self.assertEqual(result.status, Status.VALIDATION_ERROR)


class SqlErrorTests(unittest.TestCase):
    """SQL errors during execution should return SQL_ERROR status."""

    def test_sql_error_returns_sql_error_status(self):
        """Querying a non-existent table should yield SQL_ERROR."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        nonexistent = Table("nonexistent_table")
        query = Query.from_(nonexistent).select("*")
        result = execute(cursor, query, skip_validation=True)
        self.assertEqual(result.status, Status.SQL_ERROR)
        conn.close()

    def test_sql_error_provides_error_message(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        nonexistent = Table("nonexistent_table")
        query = Query.from_(nonexistent).select("*")
        result = execute(cursor, query, skip_validation=True)
        self.assertEqual(result.status, Status.SQL_ERROR)
        self.assertIsNotNone(result.error_msg)
        conn.close()


class MultiTableJoinTests(unittest.TestCase):
    """Multi-table join validations are evaluated left-to-right."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE orders (id INTEGER, customer_name TEXT)")
        self.cursor.execute(
            "CREATE TABLE order_items (id INTEGER, order_id INTEGER, product_id INTEGER, quantity INTEGER)"
        )
        self.cursor.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
        self.orders = Table("orders")
        self.order_items = Table("order_items")
        self.products = Table("products")

    def tearDown(self):
        self.conn.close()

    def _query(self):
        return (
            Query.from_(self.orders)
            .join(self.order_items, validate=Validate.ONE_TO_MANY)
            .on(self.orders.id == self.order_items.order_id)
            .join(self.products, validate=Validate.MANY_TO_ONE)
            .on(self.order_items.product_id == self.products.id)
            .select(self.orders.id, self.order_items.quantity, self.products.name, self.products.price)
        )

    def test_passes_when_all_joins_are_valid(self):
        self.cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (2, 'Bob')")
        self.cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99), (3, 'Gizmo', 29.99)")
        self.cursor.execute("INSERT INTO order_items VALUES (1, 1, 1, 2), (2, 1, 2, 1), (3, 2, 1, 5), (4, 2, 3, 1)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.OK)

    def test_fails_when_first_join_violates_its_validation(self):
        """Duplicate order IDs fail the ONE_TO_MANY check on the first join."""
        self.cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (1, 'Alice Dup')")
        self.cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        self.cursor.execute("INSERT INTO order_items VALUES (1, 1, 1, 2)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_fails_when_second_join_violates_its_validation(self):
        """Duplicate product IDs fail the MANY_TO_ONE check on the second join."""
        self.cursor.execute("INSERT INTO orders VALUES (1, 'Alice'), (2, 'Bob')")
        # products.id=1 is duplicated; violates MANY_TO_ONE on order_items->products
        self.cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (1, 'Widget Dup', 9.99)")
        self.cursor.execute("INSERT INTO order_items VALUES (1, 1, 1, 2), (2, 2, 1, 1)")
        result = execute(self.cursor, self._query())
        self.assertEqual(result.status, Status.VALIDATION_ERROR)


if __name__ == "__main__":
    unittest.main()
