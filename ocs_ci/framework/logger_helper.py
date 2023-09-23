import inspect
import logging

step_counts = {}


def log_step(message: str):
    """
    Method to log step in the test case.

    Log will be in the format: <test_function_name> --- <step_number> --- <message>
    Similar to: odf_overview_ui --- 8 --- Navigate Storage System via breadcrumb

    Args:
        message (str): Message to be logged
    """

    caller_frame = inspect.currentframe().f_back
    caller_name = caller_frame.f_code.co_name
    caller_module = inspect.getmodule(caller_frame)

    step_counts[caller_module] = step_counts.get(caller_module, 0) + 1

    logger = None
    if caller_module:
        module_attr = dir(caller_module)
        attr_loggers = [attr for attr in module_attr if attr in ["logger", "log"]]
        logger_str = attr_loggers[0]
        logger = getattr(caller_module, logger_str, logging.getLogger(__name__))

    logger.info(f"{caller_name} --- {step_counts[caller_module]} --- {message}")
