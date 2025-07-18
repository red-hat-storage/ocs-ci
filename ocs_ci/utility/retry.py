import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def validate_retry_params(tries, delay, backoff, max_timeout, max_delay):
    """
    Validate retry parameters to ensure the total retry time does not exceed max_timeout.

    Args:
        tries: Number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplicative factor for backoff.
        max_timeout: Maximum total time for retries in seconds.
        max_delay: Maximum delay between retries in seconds.

    Raises:
        ValueError: If the retry configuration exceeds the maximum timeout.
    """
    total_time = 0
    current_delay = delay

    for attempt in range(tries):
        total_time += current_delay
        if total_time > max_timeout:
            raise ValueError(
                f"The retry configuration exceeds the maximum allowed timeout ({max_timeout} seconds) "
                f"with the given parameters: tries={tries}, delay={delay}, backoff={backoff}, max_delay={max_delay}."
                f"You reached to: {max_timeout} seconds! Please fix it or adjust parameters of retry."
            )
        current_delay = min(current_delay * backoff, max_delay)


def retry(
    exception_to_check,
    tries=4,
    delay=3,
    backoff=2,
    text_in_exception=None,
    func=None,
    max_timeout=14400,
    max_delay=600,
):
    """
    Retry calling the decorated function using exponential backoff, with a maximum timeout and maximum delay.

    Args:
        exception_to_check: The exception to check. May be a tuple of exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier e.g., value of 2 will double the delay each retry. If the delay
            is greater than max_delay after multiplier, than max_delay is used as delay
        text_in_exception: Retry only when text_in_exception is in the text of exception.
        func: Function for garbage collector.
        max_timeout: Maximum total time for retries in seconds (default: 4 hours).
        max_delay: Maximum delay between retries in seconds (default: 600 seconds or 10 minutes).
    """
    # Validate parameters before proceeding
    validate_retry_params(tries, delay, backoff, max_timeout, max_delay)

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            logger.debug(
                f"Executing {f.__name__}. Tries: {mtries}. Delay: {mdelay}. Backoff: {backoff}"
            )
            exception_summary = set()
            while mtries > 1:
                try:
                    if func is not None:
                        func()
                    return f(*args, **kwargs)
                except exception_to_check as e:
                    if text_in_exception:
                        if text_in_exception in str(e):
                            logger.debug(
                                f"Text: {text_in_exception} found in exception: {e}"
                            )
                        else:
                            logger.debug(
                                f"Text: {text_in_exception} not found in exception: {e}"
                            )
                            raise
                    exception_summary.add(repr(e))
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay = min(
                        mdelay * backoff, max_delay
                    )  # Cap the delay to max_delay
                    if func is not None:
                        func()
            if exception_summary:
                logger.debug(f"Retry exception summary: {exception_summary}")
            return f(*args, **kwargs)

        return f_retry

    return deco_retry


def catch_exceptions(*exceptions):
    """
    Catch unhandled exception and log the exception. This wrapper is useful to catch the exception(s) and
    perform actions after the call to the function.
    This function is stored in retry.py module because it is related to retrying

    Args:
        *exceptions: One or more exception classes to catch.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                logger.warning(f"Exception occurred and caught in {func.__name__}: {e}")

        return wrapper

    return decorator


def retry_until_exception(
    exception_to_check, tries=4, delay=3, backoff=2, text_in_exception=None, func=None
):
    """
    Retry calling the decorated function using exponential backoff until the exception occurs.

    Args:
        exception_to_check: the exception to check. may be a tuple of exceptions to check
        tries: number of times to try (not retry) before giving up
        delay: initial delay between retries in seconds
        backoff: Backoff multiplier e.g., value of 2 will double the delay each retry. If the delay
            is greater than max_delay after multiplier, than max_delay is used as delay
        text_in_exception: Retry only when text_in_exception is in the text of exception
        func: function for garbage collector
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    if func is not None:
                        func()
                    f(*args, **kwargs)
                except exception_to_check as e:
                    if text_in_exception:
                        if text_in_exception in str(e):
                            logger.debug(
                                f"Text: {text_in_exception} found in exception: {e}"
                            )
                            return True
                        else:
                            logger.debug(
                                f"Text: {text_in_exception} not found in exception: {e}"
                            )
                            raise
                else:
                    logger.warning(
                        f"{exception_to_check} didn't seem to occur, Retrying in {mdelay} seconds..."
                    )
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    if func is not None:
                        func()
            return False

        return f_retry

    return deco_retry
