import inspect
import logging
import warnings

step_counts = {}


def log_step(message: str):
    """
    Method to log step in the test case.

    .. deprecated::
        Use ``logger.test_step()`` instead. The custom logger now provides
        test_step() method on all loggers automatically::

            logger = logging.getLogger(__name__)
            logger.test_step("Your step message")

    Log will be in the format: <test_function_name> --- <step_number> --- <message>
    Similar to: odf_overview_ui --- 8 --- Navigate Storage System via breadcrumb

    Args:
        message (str): Message to be logged
    """
    # Emit deprecation warning
    warnings.warn(
        "log_step() is deprecated. Use logger.test_step() instead. "
        "All loggers now have the test_step() method available automatically.",
        DeprecationWarning,
        stacklevel=2,
    )

    caller_frame = inspect.currentframe().f_back
    caller_name = caller_frame.f_code.co_name
    caller_module = inspect.getmodule(caller_frame)

    step_counts[caller_module] = step_counts.get(caller_module, 0) + 1

    # Try to get logger from calling module
    logger = None
    if caller_module:
        module_attr = dir(caller_module)
        attr_loggers = [attr for attr in module_attr if attr in ["logger", "log"]]
        if attr_loggers:
            logger_str = attr_loggers[0]
            logger = getattr(caller_module, logger_str, None)

    # Fallback to creating a logger if none found
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"{caller_name} --- {step_counts[caller_module]} --- {message}")
