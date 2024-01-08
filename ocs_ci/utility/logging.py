# -*- coding: utf8 -*-
"""
Module for logging related functions and classes.
"""

import logging
import shutil

from ocs_ci.framework import config
from ocs_ci.ocs.constants import FILE_LOGGER, CONSOLE_LOGGER


console_logger = logging.getLogger(CONSOLE_LOGGER)
file_logger = logging.getLogger(FILE_LOGGER)


class OCSLogFormatter(logging.Formatter):
    def __init__(self):
        fmt = (
            "%(asctime)s - %(threadName)s - %(levelname)s -"
            " %(name)s.%(funcName)s.%(lineno)d - %(message)s"
        )
        super(OCSLogFormatter, self).__init__(fmt)


class CustomLoggerFilter(logging.Filter):
    """
    Logger filter which will filter out the specific logger messages we would
    like to see in console output from pytest.
    """

    def filter(self, record):
        """
        Filter out only specific logs from tests folder, those which starts with
        console.logger and ocs_ci.deployment.
        """
        log_all = config.REPORTING.get("log_all")
        if log_all:
            return FILE_LOGGER not in record.name
        else:
            return (
                record.name.startswith("tests.")
                or CONSOLE_LOGGER in record.name
                or record.name.startswith("ocs_ci.deployment")
            )


def separator(symbol_="-", val="", new_line=True):
    """
    Creates nice separator text which center the val surrounded by separator with width of terminal.

    Args:
        symbol_ (str): Symbol to use as separator, e.g. "-"
        val (str): Value which should be in the center of the output text
        new_line (bool): put new line before the returned string.

    Returns:
        str: string like ------------- VAL ----------------

    """
    new_line_char = "\n"
    if not new_line:
        new_line_char = ""
    terminal_width = shutil.get_terminal_size(fallback=(80, 40))[0]
    return f"{new_line_char}{val.center(terminal_width, symbol_)}"
