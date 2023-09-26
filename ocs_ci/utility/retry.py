import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def retry(
    exception_to_check, tries=4, delay=3, backoff=2, text_in_exception=None, func=None
):
    """
    Retry calling the decorated function using exponential backoff.

    Args:
        exception_to_check: the exception to check. may be a tuple of exceptions to check
        tries: number of times to try (not retry) before giving up
        delay: initial delay between retries in seconds
        backoff: backoff multiplier e.g. value of 2 will double the delay each retry
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
                    logger.warning("%s, Retrying in %d seconds..." % (str(e), mdelay))
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    if func is not None:
                        func()
            return f(*args, **kwargs)

        return f_retry

    return deco_retry
