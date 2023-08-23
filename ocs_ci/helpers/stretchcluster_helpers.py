import logging

from datetime import timedelta
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pod_logs

logger = logging.getLogger(__name__)


def check_for_read_pause(logreader_pods, start_time, end_time):
    """
    This checks for any read pause has occurred during the given
    window of start_time and end_time

    Args:
        logreader_pods (list): List of logreader pod objects
        start_time (datetime): datetime object representing the start time
        end_time (datetime): datetime object representing the end time

    Returns:
         Boolean : True if the pouase has occured else False

    """
    paused = False
    for pod in logreader_pods:
        pause_count = 0
        time_var = start_time
        pod_log = get_pod_logs(
            pod_name=pod.name, namespace=constants.STRETCH_CLUSTER_NAMESPACE
        )
        logger.info(f"Current pod: {pod.name}")
        while time_var <= (end_time + timedelta(minutes=1)):
            t_time = time_var.strftime("%H:%M")
            if f" {t_time}" not in pod_log:
                pause_count += 1
                logger.info(f"Read pause: {t_time}")
            else:
                logger.info(f"Read success: {t_time}")
            time_var = time_var + timedelta(minutes=1)
        if pause_count > 5:
            paused = True
            break
    return paused


def check_for_write_pause(logwriter_pod, log_files, start_time, end_time):
    """
    This checks for any read pause has occurred during the given
    window of start_time and end_time

    Args:
        logwriter_pod (Pod): Logwriter Pod object
        log_files (list): List representing the list of log files generated
        start_time (datetime): datetime object representing the start time
        end_time (datetime): datetime object representing the end time

    Returns:
         Boolean : True if the pouase has occured else False

    """
    paused = False
    for file_name in log_files:
        pause_count = 0
        file_log = logwriter_pod.exec_sh_cmd_on_pod(command=f"cat {file_name}")
        time_var = start_time
        logger.info(f"Current file: {file_name}")
        while time_var <= (end_time + timedelta(minutes=1)):
            t_time = time_var.strftime("%H:%M")
            if f"T{t_time}" not in file_log:
                pause_count += 1
                logger.info(f"Write pause: {t_time}")
            else:
                logger.info(f"Write success: {t_time}")
            time_var = time_var + timedelta(minutes=1)
        if pause_count > 5:
            paused = True
            break
    return paused
