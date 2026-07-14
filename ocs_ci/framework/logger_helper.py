import inspect
import logging
import warnings
from threading import Lock

step_counts = {}
_step_counts_lock = Lock()


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

    with _step_counts_lock:
        step_counts[caller_module] = step_counts.get(caller_module, 0) + 1
        step_no = step_counts[caller_module]

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

    logger.info(f"{caller_name} --- {step_no} --- {message}")


def reset_log_steps(module=None):
    """
    Reset step counters.

    Args:
        module:
          - None: clear all
          - module object or module name string: clear only that module

    """
    with _step_counts_lock:
        if module is None:
            step_counts.clear()
            return
        target_name = (
            module if isinstance(module, str) else getattr(module, "__name__", None)
        )
        for m in list(step_counts.keys()):
            if m == module or getattr(m, "__name__", None) == target_name:
                step_counts.pop(m, None)


def reset_current_module_log_steps():
    """
    Reset step count for the module from which this helper is called.
    Call this function f.e. if log_step is used in a loop and we want to refresh counting on each iteration.
    """
    caller_frame = inspect.currentframe().f_back
    caller_module = inspect.getmodule(caller_frame)
    reset_log_steps(caller_module)


def get_step_count(module=None):
    """
    Get current step count.

    Args:
        module: None returns dict copy; else module object or name.

    """
    with _step_counts_lock:
        if module is None:
            return {getattr(m, "__name__", str(m)): c for m, c in step_counts.items()}
        if isinstance(module, str):
            for m in step_counts:
                if getattr(m, "__name__", None) == module:
                    return step_counts[m]
            return 0
        return step_counts.get(module, 0)
