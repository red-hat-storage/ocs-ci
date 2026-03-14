# -*- coding: utf-8 -*-
"""
Custom logger for OCS-CI framework.

Provides enhanced logging with custom levels for test automation:
- TEST_STEP: Sequential test steps with automatic numbering
- ASSERTION: Test assertions and validations
- AI_DATA: AI/ML metrics, predictions, and analysis

This module patches Python's logging module via logging.setLoggerClass()
so ALL calls to logging.getLogger() automatically return OCSCILogger instances.

Usage:
    import logging  # Standard import - no changes needed!

    logger = logging.getLogger(__name__)  # Returns OCSCILogger automatically
    logger.test_step("Create PVC")
    logger.assertion("PVC status: expected='Bound', actual='Bound'")
    logger.ai_data("Failure prediction: 85% probability")

"""

import inspect
import logging
from threading import Lock

# Custom log levels
TEST_STEP = 25  # Between INFO (20) and WARNING (30)
ASSERTION = 27  # Between TEST_STEP (25) and WARNING (30)
AI_DATA = 5  # Below DEBUG (10) - verbose, specialized logging

# Register levels globally (idempotent - safe to call multiple times)
logging.addLevelName(TEST_STEP, "TEST_STEP")
logging.addLevelName(ASSERTION, "ASSERTION")
logging.addLevelName(AI_DATA, "AI_DATA")

# Global step counter storage (thread-safe)
_step_counts = {}
_step_counts_lock = Lock()


def increment_step(module_name):
    """
    Atomically increment and return step number for a module.

    Args:
        module_name (str): Name of the module/logger

    Returns:
        int: The new step number

    """
    with _step_counts_lock:
        _step_counts[module_name] = _step_counts.get(module_name, 0) + 1
        return _step_counts[module_name]


def get_current_step(module_name):
    """
    Get current step count for a module without incrementing.

    Args:
        module_name (str): Name of the module/logger

    Returns:
        int: Current step number (0 if not yet incremented)

    """
    with _step_counts_lock:
        return _step_counts.get(module_name, 0)


def reset_step_counts(module_name=None):
    """
    Reset step counters.

    Args:
        module_name (str, optional): Name of specific module to reset.
            If None, resets all counters.

    """
    with _step_counts_lock:
        if module_name:
            _step_counts.pop(module_name, None)
        else:
            _step_counts.clear()


class OCSCILogger(logging.Logger):
    """
    Custom logger for OCS-CI framework.

    Automatically used by all logging.getLogger() calls via logging.setLoggerClass().

    Provides custom log levels and methods:
    - test_step(): Log test steps with automatic numbering
    - assertion(): Log test assertions and validations
    - ai_data(): Log AI/ML metrics and predictions

    """

    def __init__(self, name, level=logging.NOTSET):
        """
        Initialize custom logger.

        Args:
            name (str): Logger name
            level (int): Logging level

        """
        super().__init__(name, level)
        self._module_name = name

    def test_step(self, message, *args, **kwargs):
        """
        Log test step at TEST_STEP level with automatic numbering.

        The step number is automatically incremented per module and includes
        the calling function name in the format: "function_name --- N --- message"

        Args:
            message (str): Step description
            *args: Format arguments for message
            **kwargs: Additional logging kwargs (exc_info, extra, etc.)

        Example:
            logger.test_step("Create PVC")
            # Output: "test_pvc_creation --- 1 --- Create PVC"

        """
        if self.isEnabledFor(TEST_STEP):
            # Get calling function name (matches log_step behavior)
            caller_frame = inspect.currentframe().f_back
            caller_name = caller_frame.f_code.co_name

            # Get and increment step number for this module
            step_num = increment_step(self._module_name)

            # Format: "function_name --- N --- message" (matches log_step)
            formatted_message = f"{caller_name} --- {step_num} --- {message}"

            # Log at TEST_STEP level
            self._log(TEST_STEP, formatted_message, args, **kwargs)

    def assertion(self, message, *args, **kwargs):
        """
        Log assertion at ASSERTION level.

        Use for test validations and assertions to make them easily identifiable
        in logs and filterable separately from regular INFO messages.

        Args:
            message (str): Assertion description
            *args: Format arguments for message
            **kwargs: Additional logging kwargs (exc_info, extra, etc.)

        Example:
            logger.assertion("PVC status: expected='Bound', actual='Bound'")

        """
        if self.isEnabledFor(ASSERTION):
            self._log(ASSERTION, message, args, **kwargs)

    def ai_data(self, message, *args, **kwargs):
        """
        Log AI/ML data at AI_DATA level.

        This level is below DEBUG (5 < 10) and requires explicit enabling.
        Use for AI/ML metrics, predictions, model information, and analysis data.
        To see AI_DATA logs, set log level to 5 or use --log-cli-level=5

        Args:
            message (str): AI/ML data description
            *args: Format arguments for message
            **kwargs: Additional logging kwargs (exc_info, extra, etc.)

        Example:
            logger.ai_data("Prediction: failure_risk=0.85, model='v2.3', confidence=0.95")

        """
        if self.isEnabledFor(AI_DATA):
            self._log(AI_DATA, message, args, **kwargs)


# Set the custom logger class globally
# All subsequent logging.getLogger() calls will return OCSCILogger instances
logging.setLoggerClass(OCSCILogger)
