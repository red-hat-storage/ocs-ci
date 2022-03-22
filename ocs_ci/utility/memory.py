# -*- coding: utf8 -*-
"""
Module for memory related util functions.
"""

import os
from collections import namedtuple
from itertools import groupby

from psutil import Process

from ocs_ci.ocs import constants

ConsumedRamLogEntry = namedtuple(
    "ConsumedRamLogEntry", ("nodeid", "on", "consumed_ram")
)
consumed_ram_log = []
_proc = Process(os.getpid())


def get_consumed_ram():
    return _proc.memory_info().rss


def get_memory_consumption_report():
    """
    Get the memory consuption report data

    Returns:
        list: lines of the report with consumed memory by TC
    """
    report_data = []
    grouped = groupby(consumed_ram_log, lambda entry: entry.nodeid)
    for nodeid, (start_entry, end_entry) in grouped:
        leaked = end_entry.consumed_ram - start_entry.consumed_ram
        if leaked > constants.LEAK_LIMIT:
            report_data.append("LEAKED {}MB in {}".format(leaked / 1024 / 1024, nodeid))
    return report_data
