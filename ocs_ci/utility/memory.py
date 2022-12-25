# -*- coding: utf8 -*-
"""
Module for memory related util functions.
"""

import os
import logging
import tempfile
from collections import namedtuple
from itertools import groupby
import numpy as np
import pandas as pd
import psutil
from psutil import Process
from psutil._common import bytes2human
from ocs_ci.ocs import constants
from threading import Timer

from ocs_ci.utility.utils import get_testrun_name

current_factory = logging.getLogRecordFactory()
log = logging.getLogger(__name__)

ConsumedRamLogEntry = namedtuple(
    "ConsumedRamLogEntry", ("nodeid", "on", "consumed_ram")
)


class MemoryMonitor(Timer):
    """
    class to implement threading.Timer running func in a background
    """

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


consumed_ram_log = []
_proc = Process(os.getpid())
_df = pd.DataFrame(columns=["pid", "name", "ts", "rss", "vms", "status"])
_mon: MemoryMonitor
_mem_csv: str


def _get_memory_per_process():
    """
    Function to add memory rss and vms of current process and all subprocesses to dataframe obj (_df)
    """
    _rec_memory(_proc)
    children = _proc.children(recursive=True)
    for child in children:
        _rec_memory(child)


def _rec_memory(proc: Process):
    """
    Helper func to update _df DataFrame object with proc stats, accordingly
    to structure: "pid", "name", "ts", "rss", "vms", "status"
    """
    try:
        _df.loc[len(_df)] = [
            proc.pid,
            proc.name(),
            pd.Timestamp.now(),
            get_consumed_ram(proc),
            get_consumed_virt_mem(proc),
            proc.status(),
        ]
    except psutil.ZombieProcess:
        pass
    except psutil.NoSuchProcess:
        pass


def get_consumed_ram(proc: Process = Process(os.getpid())):
    """
    Get consumed RAM(rss) for the process
    rss is the Resident Set Size, which is the actual physical memory the process is using
    """
    return proc.memory_info().rss


def get_consumed_virt_mem(proc: Process = Process(os.getpid())):
    """
    Get consumed Virtual mem for the process
    vms is the Virtual Memory Size which is the virtual memory that process is using

    ! Important notice !
    'vms' will be larger than physical memory capacity:
        Shared libraries and frameworks are counted as part of the virtual memory for
        every application that uses them, e.g. if you have 100 processes
        running on a computer, and a 5 MB library used by all those processes,
        then that library is counted as 500 MB of virtual memory.
    """
    return proc.memory_info().vms


def start_monitor_memory(interval: int = 3, create_csv: bool = False) -> MemoryMonitor:
    """
    Start memory monitor Timer process

    Args:
        interval (int): interval in sec to read measurements
        create_csv (bool): create csv during test run. With this option it is possible
            to upload file as artifact (to be done) or preserve csv file in the system
    Returns:
         MemoryMonitor: monitor object MemoryMonitor(Timer)
    """
    global _mem_csv
    global _mon
    _mem_csv_path = f"mem-data-{get_testrun_name()}"
    if create_csv:
        _mem_csv = tempfile.mktemp(prefix=_mem_csv_path)
    _mon = MemoryMonitor(interval, _get_memory_per_process)
    _mon.start()
    return _mon


def stop_monitor_memory(save_csv: bool = False) -> tuple:
    """
    Stop MemoryMonitor(Timer) and read memory stats

    Args:
        save_csv (bool):  saves csv temporarily, until main process is dead if save_csv = True;
            reauire create_csv=True at start_monitor_memory(...)

     Returns:
         tuple: (path to csv file with memory stats,
                table with results for rss peak memory processes,
                table with results for vms peak memory processes)
    """
    global _mon
    global _mem_csv
    _mon.cancel()
    if save_csv:
        _df.to_csv(_mem_csv)
    else:
        _mem_csv = None
    table_rss = read_peak_mem_stats(constants.RAM, _df)
    table_vms = read_peak_mem_stats(constants.VIRT, _df)
    del _mon
    return _mem_csv, table_rss, table_vms


def get_memory_consumption_report() -> list:
    """
    Get the memory consumption report data
    Returns:
        list: lines of the report with consumed memory by TC
    """
    report_data = []
    grouped = groupby(consumed_ram_log, lambda entry: entry.nodeid)
    for nodeid, (start_entry, end_entry) in grouped:
        leaked = end_entry.consumed_ram - start_entry.consumed_ram
        if leaked > constants.LEAK_LIMIT:
            report_data.append("LEAKED {}MB in {}".format(bytes2human(leaked), nodeid))
    return report_data


def read_peak_mem_stats(
    stat: constants, df: pd.DataFrame = None, csv_path: str = None
) -> pd.DataFrame:
    """
    Read peak memory stats from Dataframe or csv file. Processes with stat above avg will be taken

    Args:
        stat (constants): stat either 'rss' or 'vms' (constants.RAM | constants.VIRT)
        df (pd.DataFrame): dataframe object with structure: index,pid,name,ts,rss,vms,status
        csv_path (str): path to csv file with structure: index,pid,name,ts,rss,vms,status;
                        will be ignored in case if df != None

    Returns: DataFrame similar to:
                         name                  proc_start                    proc_end           rss_peak
    0                    Google Chrome  2022-12-23 14:25:36.301757  2022-12-23 14:27:32.194759  156 MB
    1  Google Chrome Helper (Renderer)  2022-12-23 14:25:39.451159  2022-12-23 14:27:32.214615  784 MB
    2                           Python  2022-12-23 14:25:22.814883  2022-12-23 14:27:32.151046  228 MB
    """
    excluded_stat = list(filter(lambda x: x != stat, [constants.RAM, constants.VIRT]))[
        0
    ]
    if df is None:
        df = pd.read_csv(csv_path)
    df.reset_index(drop=True)
    stat_high = df.loc[df[stat] > (df[stat].mean())]
    high_pid = stat_high.pid.unique()
    stat_high_full = df[df.pid.isin(high_pid)]
    table = (
        stat_high_full.drop(["status", excluded_stat], axis=1)
        .groupby("name", as_index=False)
        .agg({"ts": [np.min, np.max], stat: [np.max]})
    )
    table = pd.DataFrame(
        table.values, columns=["name", "proc_start", "proc_end", f"{stat}_peak"]
    )
    table[f"{stat}_peak"] = table[f"{stat}_peak"].apply(bytes2human)
    return table
