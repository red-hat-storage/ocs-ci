import json
import os
import logging
import subprocess
import time
from datetime import datetime

import re

from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility import version

logger = logging.getLogger(__name__)
DATE_TIME_FORMAT = "%Y I%m%d %H:%M:%S.%f"

interface_data = {
    constants.CEPHBLOCKPOOL: {
        "prov": "csi-rbdplugin-provisioner",
        "csi_cnt": "csi-rbdplugin",
    },
    constants.CEPHFILESYSTEM: {
        "prov": "csi-cephfsplugin-provisioner",
        "csi_cnt": "csi-cephfsplugin",
    },
}


def write_fio_on_pod(pod_obj, file_size):
    """
    Writes IO of file_size size to a pod

    Args:
        pod_obj: pod object to write IO
        file_size: the size of the IO to be written opn pod

    """
    file_name = pod_obj.name
    logger.info(f"Starting IO on the POD {pod_obj.name}")
    now = datetime.now()

    pod_obj.fillup_fs(size=file_size, fio_filename=file_name)

    # Wait for fio to finish
    fio_result = pod_obj.get_fio_results(timeout=3600)
    err_count = fio_result.get("jobs")[0].get("error")
    assert err_count == 0, f"IO error on pod {pod_obj.name}. FIO result: {fio_result}."
    logger.info("IO on the PVC Finished")
    later = datetime.now()
    diff = int((later - now).total_seconds() / 60)
    logger.info(f"Writing of {file_size} took {diff} mins")

    # Verify presence of the file on pvc
    file_path = pod.get_file_path(pod_obj, file_name)
    logger.info(f"Actual file path on the pod is {file_path}.")
    assert pod.check_file_existence(
        pod_obj, file_path
    ), f"File {file_name} does not exist"
    logger.info(f"File {file_name} exists in {pod_obj.name}.")


def run_command(cmd, timeout=600, out_format="string", **kwargs):
    """
    Running command on the OS and return the STDOUT & STDERR outputs
    in case of argument is not string or list, return error message

    Args:
        cmd (str/list): the command to execute
        timeout (int): the command timeout in seconds, default is 10 Min.
        out_format (str): in which format to return the output: string / list
        kwargs (dict): dictionary of argument as subprocess get

    Returns:
        list or str : all STDOUT and STDERR output as list of lines, or one string separated by NewLine

    """

    if isinstance(cmd, str):
        command = cmd.split()
    elif isinstance(cmd, list):
        command = cmd
    else:
        return "Error in command"

    for key in ["stdout", "stderr", "stdin"]:
        kwargs[key] = subprocess.PIPE

    if "out_format" in kwargs:
        out_format = kwargs["out_format"]
        del kwargs["out_format"]

    logger.info(f"Going to format output as {out_format}")
    logger.info(f"Going to run {cmd} with timeout of {timeout}")
    cp = subprocess.run(command, timeout=timeout, **kwargs)
    output = cp.stdout.decode()
    err = cp.stderr.decode()
    # exit code is not zero
    if cp.returncode:
        logger.error(f"Command finished with non zero ({cp.returncode}): {err}")
        output += f"Error in command ({cp.returncode}): {err}"

    # TODO: adding more output_format types : json / yaml

    if out_format == "list":
        output = output.split("\n")  # convert output to list
        if len(output) > 1:
            output.pop()  # remove last empty element from the list
    return output


def run_oc_command(cmd, namespace=None):
    """
    Running an 'oc' command
    This function is needed in Performance tests in order to be able to run a separate command within the test
    without creating additional objects which increases memory consumed by the test.

    Args:
        cmd (str): the command to run
        namespace (str): the namespace where to run the command. If None
            is provided then value from config will be used.

    Returns:
        list : the results of the command as list of lines

    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]

    cluster_dir_kubeconfig = os.path.join(
        config.ENV_DATA["cluster_path"], config.RUN.get("kubeconfig_location")
    )
    if os.getenv("KUBECONFIG"):
        kubeconfig = os.getenv("KUBECONFIG")
    elif os.path.exists(cluster_dir_kubeconfig):
        kubeconfig = cluster_dir_kubeconfig
    else:
        kubeconfig = None

    command = f"oc --kubeconfig {kubeconfig} -n {namespace} {cmd}"
    return run_command(command, out_format="list")


def string_to_time(time_string):
    """
    Converting string which present a time stamp to a time object

    Args:
        time_string (str): the string to convert

    Return:
        datetime : a time object

    """
    return datetime.strptime(time_string, "%H:%M:%S.%f")


def get_logfile_names(interface, provisioning=True):
    """
    Finds names for log files pods in which logs for pvc creation are located

    Args:
        interface (str) : an interface (RBD or CephFS) to run on
        provisioning (bool): if True, look for the provisioner log pods

    Returns:
        log names (list) : names of the log files relevant for searching in

    """
    log_names = []

    num_of_tries = (
        5  # to overcome network glitches try a few times if the command fails
    )
    ns_name = config.ENV_DATA["cluster_namespace"]
    for i in range(num_of_tries):
        pods = run_oc_command(cmd="get pod", namespace=ns_name)

        if "Error in command" in pods or "Unable to connect" in pods:
            if i == num_of_tries - 1:
                raise Exception("Cannot get csi controller pod")
            else:
                time.sleep(3)
                continue

        break  # if we are here, no errors in command, exit the loop

    provisioning_name = interface_data[interface]["prov"]
    csi_name = interface_data[interface]["csi_cnt"]

    for line in pods:
        if provisioning:
            if provisioning_name in line:
                log_names.append(line.split()[0])
        else:
            if csi_name in line and provisioning_name not in line:
                log_names.append(line.split()[0])

    logger.info(f"The logs pods are : {log_names}")
    return log_names


def read_csi_logs(log_names, container_name, start_time):
    """
    Reading specific CSI logs starting on a specific time

    Args:
        log_names (list): list of pods to read log from them
        container_name (str): the name of the specific container in the pod
        start_time (time): the time stamp which will use as starting point in the log

    Returns:
        list : list of lines from all logs

    """
    ns_name = config.ENV_DATA["cluster_namespace"]
    logs = []
    for l in log_names:
        logs.append(
            run_oc_command(
                f"logs {l} -c {container_name} --since-time={start_time}",
                ns_name,
            )
        )
    return logs


# Sometimes, the logs are not available due to the connection issues, retry added
@retry(Exception, tries=6, delay=5, backoff=2)
def measure_pvc_creation_time(interface, pvc_name, start_time):
    """

    Measure PVC creation time, provided pvc name and time after which the PVC was created

    Args:
        interface (str) : an interface (RBD or CephFS) to run on
        pvc_name (str) : Name of the pvc for which we measure the time
        start_time (str): Formatted time from which and on to search the relevant logs

    Returns:
        (float) creation time for PVC in seconds

    """
    log_names = get_logfile_names(interface)
    logs = read_csi_logs(log_names, "csi-provisioner", start_time)

    st = None
    et = None
    # look for start time and end time of pvc creation. The start/end line may appear in log several times
    # in order to be on the safe side and measure the longest time difference (which is the actual pvc creation
    # time), the earliest start time and the latest end time are taken
    for sublog in logs:
        for line in sublog:
            if (
                st is None
                and "provision" in line
                and pvc_name in line
                and "started" in line
            ):
                st = string_to_time(line.split(" ")[1])
            elif "provision" in line and pvc_name in line and "succeeded" in line:
                et = string_to_time(line.split(" ")[1])
    del logs
    if st is None:
        logger.error(f"Cannot find start time of {pvc_name}")
        raise Exception(f"Cannot find start time of {pvc_name}")

    if et is None:
        logger.error(f"Cannot find end time of {pvc_name}")
        raise Exception(f"Cannot find end time of {pvc_name}")

    total_time = (et - st).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    logger.info(f"Creation time for pvc {pvc_name} is {total_time} seconds")
    return total_time


# Sometimes, the logs are not available due to the connection issues, retry added
@retry(Exception, tries=6, delay=5, backoff=2)
def csi_pvc_time_measure(interface, pvc_obj, operation, start_time):
    """

    Measure PVC time (create / delete) in the CSI driver

    Args:
        interface (str) : an interface (RBD or CephFS) to run on
        pvc_obj (PVC) : the PVC object which we want to mesure
        operation (str): which operation to mesure - 'create' / 'delete'
        start_time (str): Formatted time from which and on to search the relevant logs

    Returns:
        (float): time in seconds which took the CSI to hendale the PVC

    """

    pv_name = pvc_obj.backed_pv

    # Reading the CSI provisioner logs
    log_names = get_logfile_names(interface)
    logs = read_csi_logs(log_names, interface_data[interface]["csi_cnt"], start_time)

    st = None
    et = None
    for sublog in logs:
        for line in sublog:
            if (
                operation == "delete"
                and "generated volume id" in line.lower()
                and pv_name in line
            ):
                pv_name = line.split("(")[1].split(")")[0]
            if f"Req-ID: {pv_name} GRPC call:" in line:
                st = string_to_time(line.split(" ")[1])
            if f"Req-ID: {pv_name} GRPC response:" in line:
                et = string_to_time(line.split(" ")[1])
    del logs
    if st is None:
        err_msg = f"Cannot find CSI start time of {pvc_obj.name}"
        logger.error(err_msg)
        raise Exception(err_msg)

    if et is None:
        err_msg = f"Cannot find CSI end time of {pvc_obj.name}"
        logger.error(err_msg)
        raise Exception(err_msg)

    total_time = (et - st).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    logger.info(f"CSI time for pvc {pvc_obj.name} is {total_time} seconds")
    return total_time


def csi_bulk_pvc_time_measure(interface, pvc_objs, operation, start_time):
    """

    Measure PVC time (create / delete) in the CSI driver

    Args:
        interface (str) : an interface (RBD or CephFS) to run on
        pvc_objs (list) : list of the PVC objects which we want to mesure
        operation (str): which operation to mesure - 'create' / 'delete'
        start_time (str): Formatted time from which and on to search the relevant logs

    Returns:
        (float): time in seconds which took the CSI to hendale the PVC

    """

    st = []
    et = []

    cnt_names = {
        constants.CEPHFILESYSTEM: "csi-cephfsplugin",
        constants.CEPHBLOCKPOOL: "csi-rbdplugin",
    }

    # Reading the CSI provisioner logs
    log_names = get_logfile_names(interface)
    logs = read_csi_logs(log_names, cnt_names[interface], start_time)

    for pvc in pvc_objs:
        pv_name = pvc.backed_pv
        single_st = None
        single_et = None

        for sublog in logs:
            for line in sublog:
                if (
                    operation == "delete"
                    and "generated volume id" in line.lower()
                    and pv_name in line
                ):
                    pv_name = line.split("(")[1].split(")")[0]
                if f"Req-ID: {pv_name} GRPC call:" in line:
                    single_st = string_to_time(line.split(" ")[1])
                if f"Req-ID: {pv_name} GRPC response:" in line:
                    single_et = string_to_time(line.split(" ")[1])

        if single_st is None:
            err_msg = f"Cannot find CSI start time of {pvc.name}"
            logger.error(err_msg)
            raise Exception(err_msg)

        if single_et is None:
            err_msg = f"Cannot find CSI end time of {pvc.name}"
            logger.error(err_msg)
            raise Exception(err_msg)

        st.append(single_st)
        et.append(single_et)

    st.sort()
    et.sort()
    total_time = (et[-1] - st[0]).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    logger.info(
        f"CSI time for {operation} bulk of {len(pvc_objs)} pvcs is {total_time} seconds"
    )
    return total_time


def extruct_timestamp_from_log(line):
    """
    Excructing from the log line the timestamp of a message. adidng the current year
    since it is not exists in the log line.

    Args:

        line (str): a log line.

    Return:
         str: string of the timestamp from the log line.

    """
    this_year = str(datetime.now().year)
    mon_day = " ".join(line.split(" ")[0:2])
    results = f"{this_year} {mon_day}"
    logger.debug(f"The Results timestamp is : {results}")
    return results


def measure_total_snapshot_creation_time(snap_name, start_time):
    """
    Measure Snapshot creation time based on logs

    Args:
        snap_name (str): Name of the snapshot for creation time measurement
        start_time (str): start time, starting from which the logs are searched

    Returns:
        float: Creation time for the snapshot

    """
    start = get_snapshot_time(snap_name, "start", start_time)
    end = get_snapshot_time(snap_name, "end", start_time)

    if start and end:
        total = end - start
        return total.total_seconds()

    if start is None:
        err_msg = f"Cannot find start creation time of snapshot {snap_name}"
        logger.error(err_msg)
        raise Exception(err_msg)
    if end is None:
        err_msg = f"Cannot find end creation time of snapshot {snap_name}"
        logger.error(err_msg)
        raise Exception(err_msg)


def get_snapshot_time(snap_name, status, start_time):
    """
    Get the starting/ending creation time of a snapshot based on logs

    The time and date extraction code below has been modified to read
    the month and day data in the logs.  This fixes an error where negative
    time values are calculated when test runs cross midnight.  Also, previous
    calculations would not set the year, and so the calculations were done
    as if the year were 1900.  This is not a problem except that 1900 was
    not a leap year and so the next February 29th would throw ValueErrors
    for the whole day.  To avoid this problem, changes were made to also
    include the current year.

    Incorrect times will still be given for tests that cross over from
    December 31 to January 1.

    Args:
        pvc_name (str / list): Name of the PVC(s) for creation time
                               the list will be list of pvc objects
        status (str): the status that we want to get - Start / End
        start_time (str): start time, starting from which the logs are searched

    Returns:
        datetime object: Time of searched snapshot operation

    """

    def get_pattern_time(log, snapname, pattern):
        """
        Get the time of pattern in the log

        Args:
            log (list): list of all lines in the log file
            snapname (str): the name of the snapshot
            pattern (str): the pattern that need to be found in the log (start / bound)

        Returns:
            str: string of the pattern timestamp in the log, if not found None

        """
        this_year = str(datetime.now().year)
        for line in log:
            if re.search(snapname, line) and re.search(pattern, line):
                mon_day = " ".join(line.split(" ")[0:2])
                return f"{this_year} {mon_day}"
        return None

    pods = run_oc_command(cmd="get pod", namespace="openshift-cluster-storage-operator")

    if "Error in command" in pods:
        raise Exception("Cannot get csi controller pod")

    log_names = []
    for line in pods:
        if (
            "csi-snapshot-controller" in line
            and "csi-snapshot-controller-operator" not in line
        ):
            log_names.append(line.split()[0])

    log_lines = []
    for log in log_names:
        sublog_lines = run_oc_command(
            f"logs {log} --since-time={start_time}",
            "openshift-cluster-storage-operator",
        )
        for l in sublog_lines:
            log_lines.extend(l.split("\n"))

    if status.lower() == "start":
        pattern = "Creating content for snapshot"
    elif status.lower() == "end":
        pattern = "ready to use"
    else:
        logger.error(f"the status {status} is invalid.")
        return None

    time = get_pattern_time(log_lines, snap_name, pattern)
    if time:
        return datetime.strptime(time, DATE_TIME_FORMAT)
    else:
        return None


def measure_csi_snapshot_creation_time(interface, snapshot_id, start_time):
    """

    Measure PVC creation time, provided pvc name and time after which the PVC was created

    Args:
        interface (str) : an interface (RBD or CephFS) to run on
        snapshot_id (str) : Id of the snapshot which creation time is measured
        start_time (str): Formatted time from which and on to search the relevant logs

    Returns:
        (float) snapshot creation time in seconds

    """
    log_names = get_logfile_names(interface)
    logs = read_csi_logs(log_names, interface_data[interface]["csi_cnt"], start_time)

    st = None
    et = None
    time_format = "%H:%M:%S.%f"
    for sublog in logs:
        for line in sublog:
            if (
                "GRPC call: /csi.v1.Controller/CreateSnapshot" in line
                and snapshot_id in line
            ):
                st = line.split(" ")[1]
                st = datetime.strptime(st, time_format)
            elif "GRPC response" in line and snapshot_id in line:
                et = line.split(" ")[1]
                et = datetime.strptime(et, time_format)
    if st is None:
        logger.error(f"Cannot find start time of snapshot {snapshot_id}")
        raise Exception(f"Cannot find start time of snapshot {snapshot_id}")

    if et is None:
        logger.error(f"Cannot find end time of snapshot {snapshot_id}")
        raise Exception(f"Cannot find end time of snapshot {snapshot_id}")

    total_time = (et - st).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    return total_time


def calculate_operation_time(name, times):
    """
    Calculation the total time in seconds.

    Args:
        name (str): The name of object to calculate the time - for logging only
        times (dict): Dictioanry of {'start': datetime, 'end': datetime, 'total': int}

    Return:
        float: the number of seconds between start time to end time.
    """
    if times["start"] is None or times["end"] is None:
        err_msg = f"Start or End time for {name} didn't found in the log"
        logger.error(err_msg)
        raise Exception(err_msg)
    st = datetime.strptime(times["start"], DATE_TIME_FORMAT)
    logger.debug(f"Start time is {times['start']} - {st} seconds")
    et = datetime.strptime(times["end"], DATE_TIME_FORMAT)
    logger.debug(f"End time is {times['end']} - {et} seconds")

    # incase of start time is befor midnight and end time is after
    if et < st:
        et += 86400  # Total seconds in a day : 24H * 60Min * 60Sc.
    total_time = float("{:.3f}".format((et - st).total_seconds()))
    logger.debug(f"Total Time is : {total_time} Seconds")
    return total_time


def get_pvc_provision_times(interface, pvc_name, start_time, time_type="all", op="all"):
    """
    Get the starting/ending creation time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str / list): Name of the PVC(s) for creation time
                               the list will be list of pvc objects
        start_time (time): the starttime of the test to reduce log size reading
        time_type (str): the type of time to mesure : csi / total / all (csi & total)
        op (str) : the operation to mesure : create / delete / all (create & delete)

    Returns:
        dictioanry: all creation and deletion times for each pvc.

    """

    log_names = get_logfile_names(interface)

    if time_type.lower() in ["all", "total"]:
        logger.info("Reading the Provisioner logs")
        prov_logs = read_csi_logs(log_names, "csi-provisioner", start_time)
    if time_type.lower() in ["all", "csi"]:
        logger.info("Reading the CSI only logs")
        csi_logs = read_csi_logs(
            log_names, interface_data[interface]["csi_cnt"], start_time
        )

    # Initializing the results dictionary
    results = {}
    for i in range(0, len(pvc_name)):
        results[pvc_name[i].name] = {
            "create": {"start": None, "end": None, "time": None},
            "delete": {"start": None, "end": None, "time": None},
            "csi_create": {"start": None, "end": None, "time": None},
            "csi_delete": {"start": None, "end": None, "time": None},
        }
    # Getting times from Provisioner log - if needed
    if prov_logs:
        for sublog in prov_logs:
            for line in sublog:
                for i in range(0, len(pvc_name)):
                    name = pvc_name[i].name
                    pv_name = pvc_name[i].backed_pv
                    if op in ["all", "create"]:
                        if re.search(f"provision.*{name}.*started", line):
                            if results[name]["create"]["start"] is None:
                                results[name]["create"][
                                    "start"
                                ] = extruct_timestamp_from_log(line)
                        if re.search(f"provision.*{name}.*succeeded", line):
                            if results[name]["create"]["end"] is None:
                                results[name]["create"][
                                    "end"
                                ] = extruct_timestamp_from_log(line)
                                results[name]["create"][
                                    "time"
                                ] = calculate_operation_time(
                                    name, results[name]["create"]
                                )
                    if op in ["all", "delete"]:
                        if re.search(f'delete "{pv_name}": started', line):
                            if results[name]["delete"]["start"] is None:
                                results[name]["delete"][
                                    "start"
                                ] = extruct_timestamp_from_log(line)
                        if (
                            re.search(f'delete "{pv_name}": succeeded', line)
                            and (
                                version.get_semantic_ocs_version_from_config()
                                <= version.VERSION_4_13
                            )
                        ) or re.search(
                            f'delete "{pv_name}": persistentvolume deleted succeeded',
                            line,
                        ):
                            if results[name]["delete"]["end"] is None:
                                results[name]["delete"][
                                    "end"
                                ] = extruct_timestamp_from_log(line)
                                results[name]["delete"][
                                    "time"
                                ] = calculate_operation_time(
                                    name, results[name]["delete"]
                                )

    # Getting times from CSI log - if needed
    del_pv_names = []
    for i in range(0, len(pvc_name)):
        del_pv_names.append("")

    if csi_logs:
        for sublog in csi_logs:
            for line in sublog:
                for i in range(0, len(pvc_name)):
                    name = pvc_name[i].name
                    pv_name = pvc_name[i].backed_pv

                    if "generated volume id" in line.lower() and pv_name in line:
                        del_pv_names[i] = line.split("(")[1].split(")")[0]
                    if op in ["all", "create"]:
                        if f"Req-ID: {pv_name} GRPC call:" in line:
                            if results[name]["csi_create"]["start"] is None:
                                results[name]["csi_create"][
                                    "start"
                                ] = extruct_timestamp_from_log(line)
                        if f"Req-ID: {pv_name} GRPC response:" in line:
                            if results[name]["csi_create"]["end"] is None:
                                results[name]["csi_create"][
                                    "end"
                                ] = extruct_timestamp_from_log(line)
                                results[name]["csi_create"][
                                    "time"
                                ] = calculate_operation_time(
                                    name, results[name]["csi_create"]
                                )
                    if op in ["all", "delete"]:
                        if del_pv_names[i]:
                            if f"Req-ID: {del_pv_names[i]} GRPC call:" in line:
                                if results[name]["csi_delete"]["start"] is None:
                                    results[name]["csi_delete"][
                                        "start"
                                    ] = extruct_timestamp_from_log(line)
                            if f"Req-ID: {del_pv_names[i]} GRPC response:" in line:
                                if results[name]["csi_delete"]["end"] is None:
                                    results[name]["csi_delete"][
                                        "end"
                                    ] = extruct_timestamp_from_log(line)
                                    results[name]["csi_delete"][
                                        "time"
                                    ] = calculate_operation_time(
                                        name, results[name]["csi_delete"]
                                    )

    logger.debug(f"All results are : {json.dumps(results, indent=3)}")
    return results


def wait_for_resource_bulk_status(
    resource, resource_count, namespace, status, timeout=60, sleep_time=3
):
    """
    Waiting for bulk of resources (from the same type) to reach the desire status

    Args:
        resource (str): the resoure type to wait for
        resource_count (int):  the number of rusource to wait for - to wait for deleteion
            of resources, this should be '0'
        namespace (str): the namespace where the resources should be
        status (str): the status of the resources to be in.
        timeout (int): how much time to wait for the resources (in sec.)- default is 1 Minute
        sleep_time (int): how much time to wait between each iteration check - default is 3 sec.

    Return:
        bool : 'True' if all resources reach the desire state

    Raise:
        Exception : in case of not all resources reach the desire state.

    """
    while timeout >= 0:
        results = 0
        for line in run_oc_command(f"get {resource}", namespace=namespace):
            if status in line:
                results += 1
        if results == resource_count:
            return True
        else:
            logger.info(
                f"{results} {resource} out of {resource_count} are in {status} state !"
            )
            logger.info(f"wait {sleep_time} sec for next iteration")
            time.sleep(sleep_time)
            timeout -= sleep_time

    err_msg = f"{resource.upper()} failed reaching {status} on time"
    logger.error(err_msg)
    raise Exception(err_msg)


def pod_attach_csi_time(
    interface, pv_name, start_time, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    Get the pod start/attach csi time of a pod based on csi-rbdplugin container in csi-rbdplugin pods

    Args:
        interface (str): The interface backed the PVC
        pv_name (str): Name of the PV
        start_time (time): the start time of the test to reduce log size reading
        namespace (str): the tests namespace

    Return:
        float : Pod attachment csi time in seconds
        (time. time): Start time of node stage, End time of node publish

    Raise:
        Exception : in case that the expected time logs are not found

    """
    volume_handle = None
    for line in run_oc_command(f"describe pv {pv_name}", namespace=namespace):
        if "VolumeHandle:" in line:
            volume_handle = line.split()[1]
            break
    if volume_handle is None:
        logger.error(f"Cannot get volume handle for pv {pv_name}")
        raise Exception("Cannot get volume handle")

    log_names = get_logfile_names(interface, provisioning=False)
    logs = read_csi_logs(log_names, interface_data[interface]["csi_cnt"], start_time)

    logger.info(
        f"Looking for pod attach time for pv {pv_name} and volume handle {volume_handle}"
    )

    node_stage_st = None
    node_publish_st = None

    for sublog in logs:
        for line in sublog:
            if f"{volume_handle} GRPC call: /csi.v1.Node/NodeStageVolume" in line:
                node_stage_st = string_to_time(line.split()[1])
                node_stage_id = line.split()[5]
            if f"{volume_handle} GRPC call: /csi.v1.Node/NodePublishVolume" in line:
                node_publish_st = string_to_time(line.split()[1])
                node_publish_id = line.split()[5]
                node_publish_req_id = line.split()[7]

    if node_stage_st is None:
        logger.error("Cannot find node stage GRPC call")
        raise Exception("Cannot find node stage GRPC call")

    if node_publish_st is None:
        logger.error("Cannot find node publish GRPC call")
        raise Exception("Cannot find node publish GRPC call")

    logger.info(f"Node stage GRPC call start time is: {node_stage_st.time()}")
    logger.info(f"Node publish GRPC call start time is: {node_publish_st.time()}")

    node_stage_et = None
    node_publish_et = None
    for sublog in logs:
        for line in sublog:
            if "GRPC response:" in line and f"ID: {node_stage_id}" in line:
                node_stage_et = string_to_time(line.split(" ")[1])
            if (
                "GRPC response:" in line
                and f"ID: {node_publish_id}" in line
                and f"Req-ID: {node_publish_req_id}" in line
            ):
                node_publish_et = string_to_time(line.split(" ")[1])

    if node_stage_et is None:
        logger.error("Cannot find node stage GRPC response")
        raise Exception("Cannot find node stage GRPC response")

    if node_publish_et is None:
        logger.error("Cannot find node publish GRPC response")
        raise Exception("Cannot find node publish GRPC response")

    logger.info(f"Node stage GRPC response time is: {node_stage_et.time()}")
    logger.info(f"Node publish GRPC response time is: {node_publish_et.time()}")

    node_stage_time = (node_stage_et - node_stage_st).total_seconds()
    if node_stage_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        node_stage_time += 24 * 60 * 60

    logger.info(f"Node stage time is {node_stage_time} seconds")

    node_publish_time = (node_publish_et - node_publish_st).total_seconds()
    if node_publish_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        node_publish_time += 24 * 60 * 60

    logger.info(f"Node publish time is {node_publish_time} seconds")

    total_time = node_stage_time + node_publish_time
    logger.info(
        f"Total csi pod attach time (stage + publish) for pvc with volume handle {volume_handle} "
        f"is {total_time} seconds"
    )

    return total_time


def pod_bulk_attach_csi_time(interface, pvc_objs, csi_start_time, namespace):
    """

    Args:
        interface (str): The interface backed the PVC
        pvc_objs (list): List of PVC objects to which pods were attached
        csi_start_time (time): the start time of the test to reduce log size reading
        namespace (str): the tests namespace

    Returns:

    """

    pods_info = []

    for pvc in pvc_objs:
        pv_name = pvc.backed_pv
        volume_handle = None
        for line in run_oc_command(f"describe pv {pv_name}", namespace=namespace):
            if "VolumeHandle:" in line:
                volume_handle = line.split()[1]
                break
        if volume_handle is None:
            logger.error(f"Cannot get volume handle for pv {pv_name}")
            raise Exception("Cannot get volume handle")
        pods_info.append(
            {
                "pv": pv_name,
                "volume_handle": volume_handle,
                "node_stage_st": None,
                "node_publish_id": None,
                "node_publish_req_id": None,
                "node_publish_et": None,
            }
        )

    log_names = get_logfile_names(interface, provisioning=False)
    logs = read_csi_logs(
        log_names, interface_data[interface]["csi_cnt"], csi_start_time
    )

    for sublog in logs:
        for line in sublog:
            for pod_info in pods_info:
                if (
                    f"{pod_info['volume_handle']} GRPC call: /csi.v1.Node/NodeStageVolume"
                    in line
                ):
                    pod_info["node_stage_st"] = string_to_time(line.split()[1])
                if (
                    f"{pod_info['volume_handle']} GRPC call: /csi.v1.Node/NodePublishVolume"
                    in line
                ):
                    pod_info["node_publish_id"] = line.split()[5]
                    pod_info["node_publish_req_id"] = line.split()[7]
                if (
                    pod_info["node_publish_id"] is not None
                    and "GRPC response:" in line
                    and f"ID: {pod_info['node_publish_id']}" in line
                    and f"Req-ID: {pod_info['node_publish_req_id']}" in line
                ):
                    pod_info["node_publish_et"] = string_to_time(line.split(" ")[1])

    for pod_info in pods_info:
        if pod_info["node_stage_st"] is None:
            msg = (
                f"Cannot find node stage GRPC call for pv = {pod_info['pv']} "
                f"and volume handle = {pod_info['node_stage_st']}"
            )
            logger.error(msg)
            raise Exception("msg")
        if pod_info["node_publish_et"] is None:
            msg = (
                f"Cannot find node publish GRPC response for pv = {pod_info['pv']} "
                f"and volume handle = {pod_info['node_stage_st']}"
            )
            logger.error(msg)
            raise Exception("msg")

        logger.info(
            f"For pv {pv_name} : CSI start time = {pod_info['node_stage_st'].time()}, "
            f"csi end time = {pod_info['node_publish_et'].time()}"
        )

    node_stage_start_times = [pod_info["node_stage_st"] for pod_info in pods_info]
    node_stage_start_times.sort()
    publish_stage_end_times = [pod_info["node_publish_et"] for pod_info in pods_info]
    publish_stage_end_times.sort()

    logger.info(f"CSI bulk attach start time is = {node_stage_start_times[0].time()}")
    logger.info(f"CSI bulk attach end time = {publish_stage_end_times[-1].time()}")

    # total bulk_attach_csi_time is the delta between the last node publish time and the first node stage time
    total_time = (
        publish_stage_end_times[-1] - node_stage_start_times[0]
    ).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    logger.info(
        f"CSI time for bulk attach of {len(pvc_objs)} pvcs is {total_time} seconds"
    )

    return total_time


def wait_for_cronjobs(namespace, cronjobs_num, msg, timeout=60):
    """
    Runs 'oc get reclaimspacecronjob' with the TimeoutSampler

    Args:
        namespace(str): namespace in which cronjobs will be looked for
        cronjobs_num (int): the exact number of cronjobs that should exist
        msg (str): Error message to be printed if the desired condition is not reached
        timeout (int): Timeout
    Returns:

        list : Result of 'oc get reclaimspacecronjob' command

    """
    try:
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=run_oc_command,
            cmd="get reclaimspacecronjob",
            namespace=namespace,
        ):
            if (
                len(sample) == cronjobs_num + 1
            ):  # in the result one line is always a title
                return sample
    except TimeoutExpiredError:
        raise Exception(
            f"{msg} \n Only {len(sample) -1} cronjobs found.\n This is the full list: \n {sample}"
        )
