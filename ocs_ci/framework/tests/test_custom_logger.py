# -*- coding: utf-8 -*-
"""
Unit tests for custom logger implementation.

Tests the OCSCILogger class and custom log levels (TEST_STEP, ASSERTION, AI_DATA).
"""
import logging
import threading
from io import StringIO

import pytest


# Test fixtures and utilities
@pytest.fixture
def logger_name():
    """Provide unique logger name for each test"""
    return "test_logger"


@pytest.fixture
def custom_logger(logger_name):
    """
    Create a test logger instance.

    Note: Assumes custom_logger module is imported and
    logging.setLoggerClass() has been called during framework init.
    """
    logger = logging.getLogger(logger_name)
    # Clear any existing handlers
    logger.handlers = []
    logger.propagate = False
    yield logger
    # Cleanup
    logger.handlers = []


@pytest.fixture
def log_capture():
    """Capture log output for verification"""
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.NOTSET)  # Capture all levels including AI_DATA (5)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    yield handler, stream
    handler.close()


@pytest.fixture(autouse=True)
def reset_step_counts():
    """Reset step counters before each test"""
    # Import here to avoid circular dependencies
    try:
        from ocs_ci.framework.custom_logger import reset_step_counts

        reset_step_counts()
        yield
        reset_step_counts()
    except ImportError:
        # Custom logger not yet implemented
        yield


class TestCustomLogLevels:
    """Test custom log level registration"""

    def test_test_step_level_registered(self):
        """Test TEST_STEP level is registered with correct value"""
        from ocs_ci.framework.custom_logger import TEST_STEP

        assert TEST_STEP == 25
        assert logging.getLevelName(TEST_STEP) == "TEST_STEP"
        assert logging.getLevelName("TEST_STEP") == TEST_STEP

    def test_assertion_level_registered(self):
        """Test ASSERTION level is registered with correct value"""
        from ocs_ci.framework.custom_logger import ASSERTION

        assert ASSERTION == 27
        assert logging.getLevelName(ASSERTION) == "ASSERTION"
        assert logging.getLevelName("ASSERTION") == ASSERTION

    def test_ai_data_level_registered(self):
        """Test AI_DATA level is registered with correct value"""
        from ocs_ci.framework.custom_logger import AI_DATA

        assert AI_DATA == 5
        assert logging.getLevelName(AI_DATA) == "AI_DATA"
        assert logging.getLevelName("AI_DATA") == AI_DATA

    def test_level_ordering(self):
        """Test custom levels are ordered correctly relative to standard levels"""
        from ocs_ci.framework.custom_logger import TEST_STEP, ASSERTION, AI_DATA

        # AI_DATA < DEBUG < INFO < TEST_STEP < ASSERTION < WARNING < ERROR < CRITICAL
        assert AI_DATA < logging.DEBUG  # 5 < 10
        assert logging.INFO < TEST_STEP  # 20 < 25
        assert TEST_STEP < ASSERTION  # 25 < 27
        assert ASSERTION < logging.WARNING  # 27 < 30
        assert logging.WARNING < logging.ERROR  # 30 < 40

    def test_idempotent_registration(self):
        """Test that re-registering levels doesn't cause errors"""
        from ocs_ci.framework.custom_logger import TEST_STEP, ASSERTION, AI_DATA

        # Re-register levels (should be safe)
        logging.addLevelName(TEST_STEP, "TEST_STEP")
        logging.addLevelName(ASSERTION, "ASSERTION")
        logging.addLevelName(AI_DATA, "AI_DATA")

        # Verify still correct
        assert logging.getLevelName(TEST_STEP) == "TEST_STEP"
        assert logging.getLevelName(ASSERTION) == "ASSERTION"
        assert logging.getLevelName(AI_DATA) == "AI_DATA"


class TestOCSCILoggerClass:
    """Test OCSCILogger class functionality"""

    def test_logger_is_custom_class(self, custom_logger):
        """Test that getLogger returns OCSCILogger instance"""
        from ocs_ci.framework.custom_logger import OCSCILogger

        assert isinstance(custom_logger, OCSCILogger)

    def test_logger_has_test_step_method(self, custom_logger):
        """Test logger has test_step() method"""
        assert hasattr(custom_logger, "test_step")
        assert callable(custom_logger.test_step)

    def test_logger_has_assertion_method(self, custom_logger):
        """Test logger has assertion() method"""
        assert hasattr(custom_logger, "assertion")
        assert callable(custom_logger.assertion)

    def test_logger_has_ai_data_method(self, custom_logger):
        """Test logger has ai_data() method"""
        assert hasattr(custom_logger, "ai_data")
        assert callable(custom_logger.ai_data)

    def test_logger_inherits_standard_methods(self, custom_logger):
        """Test logger still has standard logging methods"""
        assert hasattr(custom_logger, "info")
        assert hasattr(custom_logger, "debug")
        assert hasattr(custom_logger, "warning")
        assert hasattr(custom_logger, "error")
        assert hasattr(custom_logger, "critical")


class TestStepLogging:
    """Test test_step() method functionality"""

    def test_step_basic_logging(self, custom_logger, log_capture):
        """Test basic test_step logging"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("Deploy cluster")

        output = stream.getvalue()
        assert "TEST_STEP" in output
        assert "Deploy cluster" in output

    def test_step_numbering(self, custom_logger, log_capture):
        """Test test_step numbering increments correctly"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("First step")
        custom_logger.test_step("Second step")
        custom_logger.test_step("Third step")

        output = stream.getvalue()
        assert "--- 1 ---" in output
        assert "--- 2 ---" in output
        assert "--- 3 ---" in output

    def test_step_format(self, custom_logger, log_capture):
        """Test test_step message format includes function name and number"""
        handler, stream = log_capture
        handler.setFormatter(logging.Formatter("%(message)s"))
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("Test action")

        output = stream.getvalue()
        # Format should be: "function_name --- N --- message"
        assert "---" in output
        assert "Test action" in output

    def test_step_with_args(self, custom_logger, log_capture):
        """Test test_step logging with format args"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("Create %d PVCs with size %s", 10, "10Gi")

        output = stream.getvalue()
        assert "Create 10 PVCs with size 10Gi" in output

    def test_step_respects_log_level(self, custom_logger, log_capture):
        """Test test_step logging respects logger level"""
        from ocs_ci.framework.custom_logger import TEST_STEP

        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.WARNING)  # Above TEST_STEP level

        custom_logger.test_step("This should not appear")

        output = stream.getvalue()
        assert "This should not appear" not in output

        # Set level to TEST_STEP or below
        custom_logger.setLevel(TEST_STEP)
        custom_logger.test_step("This should appear")

        output = stream.getvalue()
        assert "This should appear" in output


class TestAssertionLogging:
    """Test assertion() method functionality"""

    def test_assertion_basic_logging(self, custom_logger, log_capture):
        """Test basic assertion logging"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.assertion("PVC status: expected='Bound', actual='Bound'")

        output = stream.getvalue()
        assert "ASSERTION" in output
        assert "PVC status" in output

    def test_assertion_with_args(self, custom_logger, log_capture):
        """Test assertion logging with format args"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        expected = "Bound"
        actual = "Bound"
        custom_logger.assertion(
            "PVC status check: expected='%s', actual='%s'", expected, actual
        )

        output = stream.getvalue()
        assert "expected='Bound'" in output
        assert "actual='Bound'" in output

    def test_assertion_respects_log_level(self, custom_logger, log_capture):
        """Test assertion logging respects logger level"""
        from ocs_ci.framework.custom_logger import ASSERTION

        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.ERROR)  # Above ASSERTION level

        custom_logger.assertion("This should not appear")

        output = stream.getvalue()
        assert "This should not appear" not in output

        # Set level to ASSERTION or below
        custom_logger.setLevel(ASSERTION)
        custom_logger.assertion("This should appear")

        output = stream.getvalue()
        assert "This should appear" in output


class TestAIDataLogging:
    """Test ai_data() method functionality"""

    def test_ai_data_basic_logging(self, custom_logger, log_capture):
        """Test basic AI_DATA logging"""
        from ocs_ci.framework.custom_logger import AI_DATA

        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(AI_DATA)  # Enable AI_DATA level

        custom_logger.ai_data("Failure prediction: probability=0.85")

        output = stream.getvalue()
        assert "AI_DATA" in output
        assert "Failure prediction" in output

    def test_ai_data_below_debug(self, custom_logger, log_capture):
        """Test AI_DATA is below DEBUG level"""
        from ocs_ci.framework.custom_logger import AI_DATA

        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)  # DEBUG level

        custom_logger.ai_data("This should not appear at DEBUG level")

        output = stream.getvalue()
        # AI_DATA is below DEBUG, so it shouldn't appear
        assert "This should not appear" not in output

        # Set to AI_DATA level
        custom_logger.setLevel(AI_DATA)
        custom_logger.ai_data("This should appear at AI_DATA level")

        output = stream.getvalue()
        assert "This should appear at AI_DATA level" in output

    def test_ai_data_with_args(self, custom_logger, log_capture):
        """Test AI_DATA logging with format args"""
        from ocs_ci.framework.custom_logger import AI_DATA

        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(AI_DATA)

        custom_logger.ai_data(
            "Model prediction: score=%0.2f, model='%s'", 0.87, "failure_predictor_v2"
        )

        output = stream.getvalue()
        assert "score=0.87" in output
        assert "model='failure_predictor_v2'" in output


class TestStepCounters:
    """Test step counter management"""

    def test_step_counter_increments(self, logger_name):
        """Test step counter increments correctly"""
        from ocs_ci.framework.custom_logger import increment_step

        # Start from clean state
        step1 = increment_step(logger_name)
        step2 = increment_step(logger_name)
        step3 = increment_step(logger_name)

        assert step1 == 1
        assert step2 == 2
        assert step3 == 3

    def test_step_counter_per_module(self):
        """Test step counters are tracked per module"""
        from ocs_ci.framework.custom_logger import increment_step

        module1 = "test.module1"
        module2 = "test.module2"

        step1_m1 = increment_step(module1)
        step1_m2 = increment_step(module2)
        step2_m1 = increment_step(module1)
        step2_m2 = increment_step(module2)

        assert step1_m1 == 1
        assert step1_m2 == 1  # Independent counter
        assert step2_m1 == 2
        assert step2_m2 == 2

    def test_reset_step_counts_single_module(self):
        """Test resetting step count for single module"""
        from ocs_ci.framework.custom_logger import increment_step, reset_step_counts

        module1 = "test.module1"
        module2 = "test.module2"

        increment_step(module1)
        increment_step(module1)
        increment_step(module2)

        reset_step_counts(module1)

        # module1 should restart at 1, module2 should continue
        assert increment_step(module1) == 1
        assert increment_step(module2) == 2

    def test_reset_step_counts_all_modules(self):
        """Test resetting all step counts"""
        from ocs_ci.framework.custom_logger import increment_step, reset_step_counts

        module1 = "test.module1"
        module2 = "test.module2"

        increment_step(module1)
        increment_step(module1)
        increment_step(module2)
        increment_step(module2)

        reset_step_counts()  # Reset all

        # Both should restart at 1
        assert increment_step(module1) == 1
        assert increment_step(module2) == 1

    def test_get_current_step(self):
        """Test getting current step count"""
        from ocs_ci.framework.custom_logger import (
            increment_step,
            get_current_step,
            reset_step_counts,
        )

        module = "test.module"
        reset_step_counts(module)

        # Before any increments
        assert get_current_step(module) == 0

        increment_step(module)
        assert get_current_step(module) == 1

        increment_step(module)
        assert get_current_step(module) == 2


class TestThreadSafety:
    """Test thread safety of logger operations"""

    def test_step_counter_thread_safe(self):
        """Test step counter is thread-safe under concurrent access"""
        from ocs_ci.framework.custom_logger import (
            increment_step,
            reset_step_counts,
        )

        module = "test.concurrent"
        reset_step_counts(module)

        results = []
        num_threads = 10
        increments_per_thread = 10

        def increment_repeatedly():
            for _ in range(increments_per_thread):
                results.append(increment_step(module))

        threads = [
            threading.Thread(target=increment_repeatedly) for _ in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have unique values from 1 to num_threads * increments_per_thread
        expected_count = num_threads * increments_per_thread
        assert len(results) == expected_count
        assert set(results) == set(range(1, expected_count + 1))

    def test_concurrent_logging(self, custom_logger, log_capture):
        """Test concurrent logging from multiple threads"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        num_threads = 5
        logs_per_thread = 10

        def log_messages(thread_id):
            for i in range(logs_per_thread):
                custom_logger.info(f"Thread {thread_id} - Message {i}")

        threads = [
            threading.Thread(target=log_messages, args=(tid,))
            for tid in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        output = stream.getvalue()
        # Verify all messages appeared
        for tid in range(num_threads):
            for i in range(logs_per_thread):
                assert f"Thread {tid} - Message {i}" in output


class TestLoggerIntegration:
    """Test logger integration with framework"""

    def test_multiple_loggers_independent(self):
        """Test multiple logger instances maintain independent step counters"""
        from ocs_ci.framework.custom_logger import reset_step_counts

        reset_step_counts()

        logger1 = logging.getLogger("test.module1")
        logger2 = logging.getLogger("test.module2")

        # Clear handlers
        logger1.handlers = []
        logger2.handlers = []
        logger1.propagate = False
        logger2.propagate = False

        # Set up capture
        stream1 = StringIO()
        stream2 = StringIO()
        handler1 = logging.StreamHandler(stream1)
        handler2 = logging.StreamHandler(stream2)

        formatter = logging.Formatter("%(message)s")
        handler1.setFormatter(formatter)
        handler2.setFormatter(formatter)

        logger1.addHandler(handler1)
        logger2.addHandler(handler2)
        logger1.setLevel(logging.DEBUG)
        logger2.setLevel(logging.DEBUG)

        # Log steps
        logger1.test_step("Logger1 Step1")
        logger2.test_step("Logger2 Step1")
        logger1.test_step("Logger1 Step2")
        logger2.test_step("Logger2 Step2")

        output1 = stream1.getvalue()
        output2 = stream2.getvalue()

        # Each logger should have its own numbering
        assert "--- 1 ---" in output1
        assert "--- 2 ---" in output1
        assert "--- 1 ---" in output2
        assert "--- 2 ---" in output2

        # Cleanup
        handler1.close()
        handler2.close()

    def test_logger_name_preserved(self, custom_logger, logger_name):
        """Test logger preserves its name"""
        assert custom_logger.name == logger_name

    def test_standard_logging_still_works(self, custom_logger, log_capture):
        """Test standard logging methods still work correctly"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.debug("Debug message")
        custom_logger.info("Info message")
        custom_logger.warning("Warning message")
        custom_logger.error("Error message")
        custom_logger.critical("Critical message")

        output = stream.getvalue()
        assert "DEBUG - Debug message" in output
        assert "INFO - Info message" in output
        assert "WARNING - Warning message" in output
        assert "ERROR - Error message" in output
        assert "CRITICAL - Critical message" in output


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_step_with_empty_message(self, custom_logger, log_capture):
        """Test step with empty message"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("")

        output = stream.getvalue()
        assert "--- 1 ---" in output

    def test_step_with_multiline_message(self, custom_logger, log_capture):
        """Test step with multiline message"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("Line 1\nLine 2\nLine 3")

        output = stream.getvalue()
        assert "Line 1" in output
        assert "Line 2" in output or "Line 1\\nLine 2" in output  # Depends on formatter

    def test_step_with_special_characters(self, custom_logger, log_capture):
        """Test step with special characters"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        custom_logger.test_step("Special chars: 日本語 émojis 🚀 symbols @#$%")

        output = stream.getvalue()
        assert "Special chars" in output

    def test_assertion_with_exception_info(self, custom_logger, log_capture):
        """Test assertion logging with exception info"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        try:
            raise ValueError("Test exception")
        except ValueError:
            custom_logger.assertion("Exception occurred", exc_info=True)

        output = stream.getvalue()
        assert "ASSERTION" in output
        assert "Exception occurred" in output
        assert "ValueError" in output or "Traceback" in output


class TestBackwardsCompatibility:
    """Test backwards compatibility with existing code"""

    def test_existing_logger_calls_work(self):
        """Test that existing logging patterns still work"""
        logger = logging.getLogger("test.compat")

        # These should all work without errors
        logger.info("Test message")
        logger.debug("Debug: %s", "value")
        logger.warning("Warning", extra={"key": "value"})
        logger.error("Error", exc_info=False)

    def test_logger_configuration(self, custom_logger):
        """Test logger configuration methods still work"""
        custom_logger.setLevel(logging.INFO)
        assert custom_logger.level == logging.INFO

        custom_logger.setLevel(logging.DEBUG)
        assert custom_logger.level == logging.DEBUG

        # Test isEnabledFor
        assert custom_logger.isEnabledFor(logging.DEBUG)
        assert custom_logger.isEnabledFor(logging.INFO)

    def test_logger_hierarchy(self):
        """Test logger hierarchy is preserved"""
        parent = logging.getLogger("test.parent")
        child = logging.getLogger("test.parent.child")

        assert child.parent is parent


class TestDeprecationWarnings:
    """Test deprecation warnings for old log_step()"""

    def test_log_step_shows_deprecation_warning(self, recwarn):
        """Test that old log_step() shows deprecation warning"""
        from ocs_ci.framework.logger_helper import log_step

        # Call log_step and check for warning
        log_step("Old style step logging")

        # Check if deprecation warning was raised
        warnings = [w for w in recwarn if issubclass(w.category, DeprecationWarning)]

        # Should have at least one deprecation warning
        assert len(warnings) > 0, "log_step should emit a DeprecationWarning"

        # Verify message content
        assert any(
            "log_step" in str(w.message).lower() for w in warnings
        ), "Deprecation warning should mention 'log_step'"
        assert any(
            "deprecated" in str(w.message).lower()
            or "logger.test_step" in str(w.message)
            for w in warnings
        ), "Warning should indicate deprecation or alternative"

    def test_log_step_still_works(self, log_capture):
        """Test that old log_step() still works despite deprecation"""
        from ocs_ci.framework.logger_helper import log_step

        handler, stream = log_capture
        # Get root logger to capture log_step output
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.DEBUG)

        # Should work despite deprecation
        log_step("This should still log")

        output = stream.getvalue()
        # Verify the message was logged
        assert "This should still log" in output, "log_step should still produce output"

        root_logger.removeHandler(handler)

    def test_deprecation_message_helpful(self, recwarn):
        """Test that deprecation message provides migration guidance"""
        from ocs_ci.framework.logger_helper import log_step

        log_step("Test message")

        warnings = [w for w in recwarn if issubclass(w.category, DeprecationWarning)]

        # Should have deprecation warning
        assert len(warnings) > 0, "log_step should emit a DeprecationWarning"

        # Check that message mentions the new approach
        warning_text = " ".join(str(w.message).lower() for w in warnings)
        assert (
            "logger.test_step" in warning_text or ".test_step()" in warning_text
        ), "Deprecation should mention logger.test_step() as alternative"

    def test_migration_path_equivalence(self, log_capture):
        """Test that log_step and logger.test_step produce exactly the same output format"""
        import inspect
        from ocs_ci.framework.logger_helper import log_step, step_counts
        from ocs_ci.framework.custom_logger import reset_step_counts
        from io import StringIO

        # Save and clear root logger handlers to avoid interference
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        original_level = root_logger.level

        try:
            # Use separate streams to capture each output independently
            stream_old = StringIO()
            stream_new = StringIO()

            handler_old = logging.StreamHandler(stream_old)
            handler_new = logging.StreamHandler(stream_new)

            # Use same formatter for both - just the message
            formatter = logging.Formatter("%(message)s")
            handler_old.setFormatter(formatter)
            handler_new.setFormatter(formatter)

            # Setup logger for new way
            logger = logging.getLogger("test.migration")
            logger.handlers = []
            logger.propagate = False  # Prevent propagation to root
            logger.addHandler(handler_new)
            logger.setLevel(logging.DEBUG)

            # Setup root logger for log_step (old way)
            root_logger.handlers = []
            root_logger.addHandler(handler_old)
            root_logger.setLevel(logging.DEBUG)

            # Reset new logger's step counter
            reset_step_counts("test.migration")

            # Reset old logger's step counter (uses module object as key)
            current_module = inspect.getmodule(inspect.currentframe())
            if current_module in step_counts:
                del step_counts[current_module]

            # Old way
            log_step("Test step message")

            # New way
            logger.test_step("Test step message")

            output_old = stream_old.getvalue().strip()
            output_new = stream_new.getvalue().strip()

            # Both should produce exactly the same message format:
            # "function_name --- 1 --- Test step message"
            assert output_old == output_new, (
                f"Output formats should be identical:\n"
                f"  log_step: {output_old!r}\n"
                f"  logger.test_step: {output_new!r}"
            )

            # Verify the format matches expected pattern
            assert " --- 1 --- Test step message" in output_new

        finally:
            # Cleanup and restore original root logger state
            root_logger.handlers = original_handlers
            root_logger.setLevel(original_level)
            handler_old.close()
            handler_new.close()

    def test_log_step_not_required_for_new_code(self):
        """Test that new code doesn't need to import log_step"""
        # This test verifies the design principle: no imports needed for new code
        logger = logging.getLogger("test.new_code")

        # New code should have test_step() method without any special imports
        assert hasattr(
            logger, "test_step"
        ), "Logger should have test_step() method without importing log_step"
        assert callable(logger.test_step), "test_step should be a callable method"

        # Verify it works
        logger.test_step("New code can use this directly")  # Should not raise


class TestPytestIntegration:
    """Test integration with pytest"""

    def test_step_counter_resets_between_tests(self):
        """Test that step counters reset between pytest tests"""
        from ocs_ci.framework.custom_logger import get_current_step

        logger = logging.getLogger(__name__)

        # The reset_step_counts fixture should have reset counters
        # Get current step should be 0 at test start
        initial_step = get_current_step(__name__)
        assert (
            initial_step == 0
        ), f"Step counter should be reset to 0 at test start, got {initial_step}"

        # Log some steps
        logger.test_step("First step")
        assert get_current_step(__name__) == 1

        logger.test_step("Second step")
        assert get_current_step(__name__) == 2

    def test_step_counter_independent_per_test(self):
        """Test that each test gets independent step numbering"""
        from ocs_ci.framework.custom_logger import get_current_step

        logger = logging.getLogger(__name__)

        # Should start from 0 (reset by fixture)
        assert get_current_step(__name__) == 0

        logger.test_step("This test's first step")
        assert get_current_step(__name__) == 1

    def test_logs_captured_by_pytest(self, caplog):
        """Test that custom log levels are captured by pytest"""
        logger = logging.getLogger(__name__)

        # Clear any existing logs
        caplog.clear()

        # Set level low enough to capture everything
        with caplog.at_level(logging.DEBUG):
            logger.info("Regular info message")
            logger.test_step("Test step message")
            logger.assertion("Test assertion message")

        # Verify logs were captured
        assert "Regular info message" in caplog.text
        assert "Test step message" in caplog.text or "Test step" in caplog.text
        assert (
            "Test assertion message" in caplog.text or "Test assertion" in caplog.text
        )

    def test_ai_data_captured_with_low_level(self, caplog):
        """Test that AI_DATA logs are captured when level is set appropriately"""
        from ocs_ci.framework.custom_logger import AI_DATA

        logger = logging.getLogger(__name__)
        caplog.clear()

        # AI_DATA is level 5, below DEBUG (10)
        # Need to set capture level to AI_DATA or lower
        with caplog.at_level(AI_DATA):
            logger.ai_data("AI prediction data")

        assert "AI prediction data" in caplog.text or "AI prediction" in caplog.text

    def test_ai_data_not_captured_at_debug_level(self, caplog):
        """Test that AI_DATA logs are NOT captured at DEBUG level"""

        logger = logging.getLogger(__name__)
        caplog.clear()

        with caplog.at_level(logging.DEBUG):
            logger.debug("Debug message should appear")
            logger.ai_data("AI message should NOT appear at DEBUG level")

        # DEBUG message should appear
        assert "Debug message should appear" in caplog.text
        # AI_DATA should NOT appear when level is DEBUG (5 < 10)
        assert "AI message should NOT appear" not in caplog.text

    def test_custom_levels_in_caplog_records(self, caplog):
        """Test that custom level names appear in caplog records"""
        from ocs_ci.framework.custom_logger import TEST_STEP, ASSERTION

        logger = logging.getLogger(__name__)
        caplog.clear()

        with caplog.at_level(logging.DEBUG):
            logger.test_step("Step message")
            logger.assertion("Assertion message")

        # Check log records for custom level names
        step_records = [r for r in caplog.records if r.levelno == TEST_STEP]
        assertion_records = [r for r in caplog.records if r.levelno == ASSERTION]

        assert len(step_records) > 0, "Should have TEST_STEP level records"
        assert len(assertion_records) > 0, "Should have ASSERTION level records"

        # Verify level names
        assert step_records[0].levelname == "TEST_STEP"
        assert assertion_records[0].levelname == "ASSERTION"

    def test_step_formatting_in_pytest_output(self, caplog):
        """Test that step messages are formatted correctly in pytest output"""
        logger = logging.getLogger(__name__)
        caplog.clear()

        with caplog.at_level(logging.DEBUG):
            logger.test_step("Deploy cluster")
            logger.test_step("Create PVCs")

        output = caplog.text

        # Should contain step messages with step numbers
        assert "Deploy cluster" in output
        assert "Create PVCs" in output

        # Should contain step number format: --- N ---
        assert "--- 1 ---" in output
        assert "--- 2 ---" in output

    def test_exception_info_in_custom_levels(self, caplog):
        """Test that exception info works with custom log levels"""
        logger = logging.getLogger(__name__)
        caplog.clear()

        with caplog.at_level(logging.DEBUG):
            try:
                raise ValueError("Test exception for logging")
            except ValueError:
                logger.test_step("Step with exception", exc_info=True)
                logger.assertion("Assertion with exception", exc_info=True)

        output = caplog.text

        # Should contain exception info
        assert "ValueError" in output or "Traceback" in output
        assert "Test exception for logging" in output

    def test_extra_fields_with_custom_levels(self, caplog):
        """Test that extra fields work with custom log levels"""
        logger = logging.getLogger(__name__)
        caplog.clear()

        with caplog.at_level(logging.DEBUG):
            logger.test_step("Step with extra", extra={"custom_field": "custom_value"})
            logger.assertion("Assertion with extra", extra={"test_id": "test_123"})

        # Verify logs were created (extra fields may or may not appear in text)
        assert "Step with extra" in caplog.text
        assert "Assertion with extra" in caplog.text

        # Check records for extra fields
        if caplog.records:
            # At least verify records were created
            assert len(caplog.records) >= 2

    def test_pytest_fixture_compatibility(self, custom_logger, log_capture):
        """Test that custom logger works with pytest fixtures"""
        handler, stream = log_capture
        custom_logger.addHandler(handler)
        custom_logger.setLevel(logging.DEBUG)

        # Use custom methods
        custom_logger.test_step("Fixture test step")
        custom_logger.assertion("Fixture test assertion")
        custom_logger.info("Fixture test info")

        output = stream.getvalue()

        assert "Fixture test step" in output
        assert "Fixture test assertion" in output
        assert "Fixture test info" in output


class TestPerformance:
    """Test performance characteristics"""

    def test_logging_performance_overhead(self, custom_logger):
        """Test that custom logger doesn't add significant overhead"""
        import time

        custom_logger.handlers = []
        custom_logger.addHandler(logging.NullHandler())
        custom_logger.setLevel(logging.DEBUG)

        num_iterations = 10000

        # Time standard logging
        start = time.time()
        for i in range(num_iterations):
            custom_logger.info("Standard log message %d", i)
        standard_time = time.time() - start

        # Time custom logging
        start = time.time()
        for i in range(num_iterations):
            custom_logger.test_step("Step log message %d", i)
        custom_time = time.time() - start

        # Custom logging should be at most 50% slower
        # (This is lenient to account for test environment variations)
        assert (
            custom_time < standard_time * 1.5
        ), f"Custom logging too slow: {custom_time:.3f}s vs {standard_time:.3f}s"
