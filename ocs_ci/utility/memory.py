# -*- coding: utf8 -*-
"""
Module for memory related util functions.
"""

import os
import logging
import tempfile
import numpy as np
import pandas as pd
from psutil import Process, ZombieProcess, NoSuchProcess
from psutil._common import bytes2human
from ocs_ci.ocs import constants
from threading import Timer

from ocs_ci.utility.utils import get_testrun_name

current_factory = logging.getLogRecordFactory()
log = logging.getLogger(__name__)


class MemoryMonitor(Timer):
    """
    class to implement threading.Timer running func in a background
    """

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


consumed_ram_log = []
_columns_df = ["pid", "name", "ts", "rss", "vms", "status"]
_df = pd.DataFrame(columns=_columns_df)
mon: MemoryMonitor
_mem_csv: str


def _get_memory_per_process():
    """
    Function to add memory rss and vms of current process and all subprocesses to dataframe obj (_df)
    """
    proc = Process(os.getpid())
    _rec_memory(proc)
    children = proc.children(recursive=True)
    for child in children:
        _rec_memory(child)
    del proc


def _rec_memory(proc: Process):
    """
    Helper func to update _df DataFrame object with proc stats, accordingly
    to structure: "pid", "name", "ts", "rss", "vms", "status"
    """
    try:
        global _df
        _df = pd.concat(
            [
                _df,
                pd.DataFrame(
                    [
                        [
                            proc.pid,
                            proc.name(),
                            pd.Timestamp.now().strftime("%Y-%m-%d %X"),
                            get_consumed_ram(proc),
                            get_consumed_virt_mem(proc),
                            proc.status(),
                        ]
                    ],
                    columns=_columns_df,
                ),
            ]
        )
    # ZombieProcess's, NoSuchProcess's come too often within a test run,
    # we're polling each process once per 3 sec. ZombieProcess and NoSuchProcess
    # appear due to concurrency. Failed polls are not valuable information
    except ZombieProcess:
        pass
    except NoSuchProcess:
        pass
    except ValueError:
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
        interval (int): interval in sec to read measurements. Min interval is 2 sec
        create_csv (bool): create csv during test run. With this option it is possible
            to upload file as artifact (to be done) or preserve csv file in the system
    Returns:
         MemoryMonitor: monitor object MemoryMonitor(Timer)
    """
    global _mem_csv
    global mon
    global _df
    _df = pd.DataFrame(columns=_columns_df)
    _mem_csv_path = f"mem-data-{get_testrun_name()}"
    if create_csv:
        _mem_csv = tempfile.mktemp(prefix=_mem_csv_path)
    # interval cannot be smaller than 2 sec, otherwise we get mistakes in calculation
    if interval < 2:
        interval = 2
    mon = MemoryMonitor(interval, _get_memory_per_process)
    mon.daemon = True
    mon.start()
    return mon


def stop_monitor_memory(save_csv: bool = False) -> tuple:
    """
    Stop MemoryMonitor(Timer) and read memory stats

    Args:
        save_csv (bool):  saves csv temporarily, until main process is dead if save_csv = True;
            require create_csv=True at start_monitor_memory(...)

     Returns:
         tuple: (path to csv file with memory stats,
                table with results for rss peak memory processes,
                table with results for vms peak memory processes)
    """
    global mon
    mon.cancel()
    global _mem_csv
    if save_csv:
        _df.to_csv(_mem_csv)
    else:
        _mem_csv = None
    table_rss = peak_mem_stats_human_readable(constants.RAM)
    table_vms = peak_mem_stats_human_readable(constants.VIRT)
    del mon
    return _mem_csv, table_rss, table_vms


def read_peak_mem_stats(
    stat: constants, df: pd.DataFrame = None, csv_path: str = None
) -> pd.DataFrame:
    """
    Read peak memory stats from Dataframe or csv file. Processes with stat above avg will be taken
    Table will be reduced to only processes with stat > avg(stat) if number of processes will be
    larger than 10

    Args:
        stat (constants): stat either 'rss' or 'vms' (constants.RAM | constants.VIRT)
        df (pd.DataFrame): dataframe object with structure: index,pid,name,ts,rss,vms,status
        csv_path (str): path to csv file with structure: index,pid,name,ts,rss,vms,status;
                        will be ignored in case if df != None

    Returns: pd.DataFrame similar to:
    name                                     proc_start             proc_end                rss_peak
    0                    Google Chrome  2022-12-23 14:25:36      2022-12-23 14:27:32         156 MB
    1  Google Chrome Helper (Renderer)  2022-12-23 14:25:39      2022-12-23 14:27:32         784 MB
    2                           Python  2022-12-23 14:25:22      2022-12-23 14:27:32         228 MB
    """

    if df is None:
        df = pd.read_csv(csv_path)

    df = catch_empty_mem_df(df)

    df.reset_index(drop=True)
    if len(df.name.unique()) > 10:
        stat_high = df.loc[df[stat] > (df[stat].mean())]
        high_pid = stat_high.pid.unique()
        df = df[df.pid.isin(high_pid)]
    excluded_stat = list(filter(lambda x: x != stat, [constants.RAM, constants.VIRT]))[
        0
    ]
    table = (
        df.drop(["status", excluded_stat], axis=1)
        .groupby("name", as_index=False)
        .agg({"ts": [np.min, np.max], stat: [np.max]})
    )
    table = pd.DataFrame(
        table.values, columns=["name", "proc_start", "proc_end", f"{stat}_peak"]
    )
    return table


def peak_mem_stats_human_readable(
    stat: constants, csv_path: str = None
) -> pd.DataFrame:
    """
    make peak mem stats dataframe human-readable
    dataframe columns = [name, proc_start, proc_end, rss_peak]

    Args:
        stat (constants): stat either 'rss' or 'vms' (constants.RAM | constants.VIRT)
        csv_path (str): path to csv file with structure: index,pid,name,ts,rss,vms,status;
                        will be ignored in case if df != None
    Returns:
        pd.DataFrame: peak memory stats dataframe
    """
    global _df
    df_peak = read_peak_mem_stats(stat, _df, csv_path)
    df_peak = df_peak.sort_values(by=f"{stat}_peak", ascending=False)
    df_peak[f"{stat}_peak"] = df_peak[f"{stat}_peak"].apply(bytes2human)
    return df_peak


def get_peak_sum_mem() -> tuple:
    """
    get peak summarized memory stats for the test. Each test df file created anew.
    spikes defined per measurment (once in three seconds by default -> start_monitor_memory())
    """
    global _df
    _df = catch_empty_mem_df(_df)

    _df = _df.drop_duplicates(subset=["pid", "ts"], keep="last").reset_index(drop=True)
    df = _df.reset_index(drop=True)
    df = (
        df.drop(["status", "pid", "name"], axis=1).groupby(["ts"], as_index=False).sum()
    )

    ram_max = df[df[constants.RAM] == df[constants.RAM].max()].drop(
        [constants.VIRT], axis=1
    )
    virt_max = df[df[constants.VIRT] == df[constants.VIRT].max()].drop(
        [constants.RAM], axis=1
    )

    # catch psutil and calculation failures for rss and vms cells and fill with failure markers
    # therefore we may see number of failures and ignore them in report csv file while analysing
    try:
        log.info(
            "Peak total ram memory consumption: "
            f"{bytes2human(ram_max[constants.RAM].values[0].astype(int))} at {ram_max['ts'].values[0]}"
        )
    except IndexError:
        ram_max = pd.DataFrame(columns=ram_max.columns, data=[[-1, pd.to_datetime(0)]])
    try:
        log.info(
            "Peak total virtual memory consumption: "
            f"{bytes2human(virt_max[constants.VIRT].values[0].astype(int))} at {virt_max['ts'].values[0]}"
        )
    except IndexError:
        virt_max = pd.DataFrame(
            columns=virt_max.columns, data=[[-1, pd.to_datetime(0)]]
        )

    return ram_max, virt_max


def catch_empty_mem_df(df: pd.DataFrame):
    """
    routine function to catch psutil failures and fill memory dataframe with failure markers,
    therefore we may see number of failures and ignore them on examination stage
    """
    if df.empty:
        log.debug("Dataframe is empty, reinitializing")
        global _columns_df
        df = pd.DataFrame(
            columns=_columns_df,
            data=[[0, "empty_name", pd.to_datetime(0), -1, -1, "empty_status"]],
        )
    return df
