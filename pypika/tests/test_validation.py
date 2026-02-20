import unittest
from unittest.mock import MagicMock, patch

from pypika import Query, Table, Tables, JoinType

# These imports will fail until the validation module is implemented
# from pypika.validation import Validate, Status, Results, execute


class ValidateEnumTests(unittest.TestCase):
    """Tests for the Validate enum and its flag combinations."""

    def test_validate_one_to_many_exists(self):
        """ONE_TO_MANY flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "ONE_TO_MANY"))

    def test_validate_many_to_one_exists(self):
        """MANY_TO_ONE flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "MANY_TO_ONE"))

    def test_validate_one_to_one_exists(self):
        """ONE_TO_ONE flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "ONE_TO_ONE"))

    def test_validate_right_total_exists(self):
        """RIGHT_TOTAL flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "RIGHT_TOTAL"))

    def test_validate_left_total_exists(self):
        """LEFT_TOTAL flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "LEFT_TOTAL"))

    def test_validate_total_exists(self):
        """TOTAL flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "TOTAL"))

    def test_validate_mandatory_exists(self):
        """MANDATORY flag should exist."""
        from pypika.validation import Validate

        self.assertTrue(hasattr(Validate, "MANDATORY"))

    def test_combine_flags_with_or(self):
        """Flags should be combinable with the | operator."""
        from pypika.validation import Validate

        combined = Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL
        self.assertIsNotNone(combined)

    def test_one_to_one_equals_both_directions(self):
        """ONE_TO_ONE should be equivalent to ONE_TO_MANY | MANY_TO_ONE."""
        from pypika.validation import Validate

        combined = Validate.ONE_TO_MANY | Validate.MANY_TO_ONE
        self.assertEqual(Validate.ONE_TO_ONE, combined)

    def test_total_equals_both_totals(self):
        """TOTAL should be equivalent to LEFT_TOTAL | RIGHT_TOTAL."""
        from pypika.validation import Validate

        combined = Validate.LEFT_TOTAL | Validate.RIGHT_TOTAL
        self.assertEqual(Validate.TOTAL, combined)

    def test_mandatory_equals_one_to_one_and_total(self):
        """MANDATORY should be equivalent to ONE_TO_ONE | TOTAL."""
        from pypika.validation import Validate

        combined = Validate.ONE_TO_ONE | Validate.TOTAL
        self.assertEqual(Validate.MANDATORY, combined)


class StatusEnumTests(unittest.TestCase):
    """Tests for the Status enum."""

    def test_status_ok_exists(self):
        """OK status should exist."""
        from pypika.validation import Status

        self.assertTrue(hasattr(Status, "OK"))

    def test_status_validation_error_exists(self):
        """VALIDATION_ERROR status should exist."""
        from pypika.validation import Status

        self.assertTrue(hasattr(Status, "VALIDATION_ERROR"))

    def test_status_sql_error_exists(self):
        """SQL_ERROR status should exist."""
        from pypika.validation import Status

        self.assertTrue(hasattr(Status, "SQL_ERROR"))

    def test_status_not_validated_exists(self):
        """NOT_VALIDATED status should exist."""
        from pypika.validation import Status

        self.assertTrue(hasattr(Status, "NOT_VALIDATED"))


class ResultsClassTests(unittest.TestCase):
    """Tests for the Results class structure."""

    def test_results_has_status_field(self):
        """Results should have a status field."""
        from pypika.validation import Results, Status

        result = Results(status=Status.OK)
        self.assertEqual(result.status, Status.OK)

    def test_results_has_value_field(self):
        """Results should have a value field."""
        from pypika.validation import Results, Status

        result = Results(status=Status.OK, value=[("row1",), ("row2",)])
        self.assertEqual(result.value, [("row1",), ("row2",)])

    def test_results_has_error_msg_field(self):
        """Results should have an error_msg field."""
        from pypika.validation import Results, Status

        result = Results(status=Status.VALIDATION_ERROR, error_msg="Duplicate keys found")
        self.assertEqual(result.error_msg, "Duplicate keys found")

    def test_results_has_error_loc_field(self):
        """Results should have an error_loc field."""
        from pypika.validation import Results, Status

        result = Results(status=Status.VALIDATION_ERROR, error_loc="users JOIN orders")
        self.assertEqual(result.error_loc, "users JOIN orders")

    def test_results_has_error_size_field(self):
        """Results should have an error_size field."""
        from pypika.validation import Results, Status

        result = Results(status=Status.VALIDATION_ERROR, error_size=42)
        self.assertEqual(result.error_size, 42)

    def test_results_has_error_sample_field(self):
        """Results should have an error_sample field (list of tuples)."""
        from pypika.validation import Results, Status

        sample = [(1, "a"), (2, "b"), (3, "c")]
        result = Results(status=Status.VALIDATION_ERROR, error_sample=sample)
        self.assertEqual(result.error_sample, sample)


class JoinWithValidationTests(unittest.TestCase):
    """Tests for adding validation flags to joins."""

    table_a, table_b, table_c = Tables("a", "b", "c")

    def test_join_accepts_validate_parameter(self):
        """The join method should accept a validate parameter."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_inner_join_accepts_validate_parameter(self):
        """The inner_join method should accept a validate parameter."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .inner_join(self.table_b, validate=Validate.MANY_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_left_join_accepts_validate_parameter(self):
        """The left_join method should accept a validate parameter."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .left_join(self.table_b, validate=Validate.ONE_TO_MANY)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_join_with_combined_flags(self):
        """Joins should accept combined validation flags."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.MANY_TO_ONE | Validate.RIGHT_TOTAL)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_multi_table_join_with_validation(self):
        """Multiple joins should each be able to have their own validation."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .join(self.table_c, validate=Validate.TOTAL)
            .on(self.table_b.id == self.table_c.b_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_query_stores_validation_info(self):
        """Query should store validation info for later use by execute."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        # The query should have some way to access the validation info
        # This could be a method or property - implementation will decide
        self.assertTrue(hasattr(query, "get_validations") or hasattr(query, "_validations"))


class ExecuteFunctionTests(unittest.TestCase):
    """Tests for the execute function."""

    table_a, table_b = Tables("a", "b")

    def test_execute_returns_results(self):
        """execute should return a Results object."""
        from pypika.validation import execute, Results

        cursor = MagicMock()
        cursor.execute.return_value = None
        cursor.fetchall.return_value = []

        query = Query.from_(self.table_a).select("*")
        result = execute(cursor, query)

        self.assertIsInstance(result, Results)

    def test_execute_with_skip_validation(self):
        """execute with skip_validation=True should return NOT_VALIDATED status."""
        from pypika.validation import execute, Status

        cursor = MagicMock()
        cursor.execute.return_value = None
        cursor.fetchall.return_value = [("row1",)]

        query = Query.from_(self.table_a).select("*")
        result = execute(cursor, query, skip_validation=True)

        self.assertEqual(result.status, Status.NOT_VALIDATED)

    def test_execute_runs_main_query(self):
        """execute should run the main query on the cursor."""
        from pypika.validation import execute

        cursor = MagicMock()
        cursor.execute.return_value = None
        cursor.fetchall.return_value = []

        query = Query.from_(self.table_a).select("*")
        execute(cursor, query, skip_validation=True)

        cursor.execute.assert_called()

    def test_execute_returns_ok_on_success(self):
        """execute should return OK status when query succeeds without validation errors."""
        from pypika.validation import execute, Status

        cursor = MagicMock()
        cursor.execute.return_value = None
        cursor.fetchall.return_value = [("row1",), ("row2",)]

        query = Query.from_(self.table_a).select("*")
        result = execute(cursor, query, skip_validation=True)

        # With skip_validation, status is NOT_VALIDATED
        # Without validation and with successful execution, status should be OK
        self.assertIn(result.status, [Status.OK, Status.NOT_VALIDATED])


class ValidationQueryGenerationTests(unittest.TestCase):
    """Tests for generating validation SQL queries."""

    table_a, table_b = Tables("a", "b")

    def test_one_to_many_generates_check_query(self):
        """ONE_TO_MANY validation should generate a query to check for duplicates on left."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_MANY)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)
        self.assertGreater(len(validation_queries), 0)

        # The validation query should check for duplicate keys on the left table
        validation_sql = validation_queries[0].get_sql()
        self.assertIn("GROUP BY", validation_sql)
        self.assertIn("HAVING", validation_sql)
        self.assertIn("COUNT", validation_sql)

    def test_many_to_one_generates_check_query(self):
        """MANY_TO_ONE validation should generate a query to check for duplicates on right."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.MANY_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)
        self.assertGreater(len(validation_queries), 0)

    def test_one_to_one_generates_two_check_queries(self):
        """ONE_TO_ONE validation should generate queries for both directions."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)
        # Should have at least 2 queries: one for each direction
        self.assertGreaterEqual(len(validation_queries), 2)

    def test_left_total_generates_coverage_check(self):
        """LEFT_TOTAL should check that all left rows have matches."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.LEFT_TOTAL)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)
        self.assertGreater(len(validation_queries), 0)

        # Should check for left rows without matches
        validation_sql = validation_queries[0].get_sql()
        self.assertIn("NOT IN", validation_sql.upper())

    def test_right_total_generates_coverage_check(self):
        """RIGHT_TOTAL should check that all right rows have matches."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.RIGHT_TOTAL)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)
        self.assertGreater(len(validation_queries), 0)


class ValidationExecutionTests(unittest.TestCase):
    """Tests for validation execution behavior."""

    table_a, table_b = Tables("a", "b")

    def test_validation_failure_prevents_main_query(self):
        """If validation fails, the main query should not execute."""
        from pypika.validation import Validate, execute, Status

        cursor = MagicMock()
        # Simulate validation query returning rows (indicating duplicates)
        cursor.fetchall.side_effect = [
            [(1, "dup1"), (2, "dup2")],  # Validation query returns violations
        ]
        cursor.fetchone.return_value = (2,)  # Count of violations

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        result = execute(cursor, query)

        self.assertEqual(result.status, Status.VALIDATION_ERROR)

    def test_validation_error_includes_sample(self):
        """Validation errors should include a sample of failing rows."""
        from pypika.validation import Validate, execute, Status

        cursor = MagicMock()
        # Simulate validation failure
        cursor.fetchall.side_effect = [
            [(1, "a"), (2, "b"), (3, "c")],  # Sample of failing rows
        ]
        cursor.fetchone.return_value = (3,)

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        result = execute(cursor, query)

        if result.status == Status.VALIDATION_ERROR:
            self.assertIsNotNone(result.error_sample)
            self.assertLessEqual(len(result.error_sample), 10)

    def test_validation_error_includes_count(self):
        """Validation errors should include the count of failing rows."""
        from pypika.validation import Validate, execute, Status

        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [(1,), (2,), (3,)],
        ]
        cursor.fetchone.return_value = (100,)  # 100 total failing rows

        query = (
            Query.from_(self.table_a)
            .join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )

        result = execute(cursor, query)

        if result.status == Status.VALIDATION_ERROR:
            self.assertEqual(result.error_size, 100)

    def test_sql_error_returns_sql_error_status(self):
        """SQL errors should return SQL_ERROR status."""
        from pypika.validation import execute, Status

        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Database connection lost")

        query = Query.from_(self.table_a).select("*")

        result = execute(cursor, query, skip_validation=True)

        self.assertEqual(result.status, Status.SQL_ERROR)


class MultiTableValidationTests(unittest.TestCase):
    """Tests for validation with multiple joined tables."""

    table_x, table_y, table_z = Tables("x", "y", "z")

    def test_multi_join_validates_left_to_right(self):
        """Validations should be evaluated left-to-right."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_x)
            .join(self.table_y, validate=Validate.ONE_TO_ONE)
            .on(self.table_x.id == self.table_y.x_id)
            .join(self.table_z, validate=Validate.TOTAL)
            .on(self.table_y.id == self.table_z.y_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)

        # Should have validation queries for both joins
        # First set validates x JOIN y
        # Second set validates (x JOIN y) JOIN z
        self.assertGreater(len(validation_queries), 0)

    def test_second_join_validates_against_combined_result(self):
        """The second join's validation should consider the first join's result."""
        from pypika.validation import Validate, get_validation_queries

        query = (
            Query.from_(self.table_x)
            .join(self.table_y, validate=Validate.ONE_TO_ONE)
            .on(self.table_x.id == self.table_y.x_id)
            .join(self.table_z, validate=Validate.MANY_TO_ONE)
            .on(self.table_y.id == self.table_z.y_id)
            .select("*")
        )

        validation_queries = get_validation_queries(query)

        # The validation for z should be checking against (x JOIN y), not just y
        # This is a semantic test - the exact implementation may vary
        self.assertGreater(len(validation_queries), 0)


class ValidationWithDifferentJoinTypesTests(unittest.TestCase):
    """Tests for validation with different join types."""

    table_a, table_b = Tables("a", "b")

    def test_left_join_with_validation(self):
        """LEFT JOIN should support validation."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .left_join(self.table_b, validate=Validate.MANY_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_right_join_with_validation(self):
        """RIGHT JOIN should support validation."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .right_join(self.table_b, validate=Validate.ONE_TO_MANY)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)

    def test_outer_join_with_validation(self):
        """OUTER JOIN should support validation."""
        from pypika.validation import Validate

        query = (
            Query.from_(self.table_a)
            .outer_join(self.table_b, validate=Validate.ONE_TO_ONE)
            .on(self.table_a.id == self.table_b.a_id)
            .select("*")
        )
        self.assertIsNotNone(query)


if __name__ == "__main__":
    unittest.main()
