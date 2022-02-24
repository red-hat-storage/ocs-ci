import os
import logging
import subprocess
from datetime import datetime

from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


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
        output.pop()  # remove last empty element from the list
    return output


def run_oc_command(cmd, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
    """
    Running an 'oc' command
    This function is needed in Performance tests in order to be able to run a separate command within the test
    without creating additional objects which increases memory consumed by the test.

    Args:
        cmd (str): the command to run
        namespace (str): the namespace where to run the command

    Returns:
        list : the results of the command as list of lines

    """

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
    return datetime.strptime(time_string, "%H:%M:%S.%f")


def get_logfile_names(interface):
    """
    Finds names for log files pods in which logs for pvc creation are located
    For CephFS: 2 pods that start with "csi-cephfsplugin-provisioner" prefix
    For RBD: 2 pods that start with "csi-rbdplugin-provisioner" prefix

    Args:
        interface (str) : an interface (RBD or CephFS) to run on

    Returns:
        log names (list) : names of the log files relevant for searching in

    """
    log_names = []

    pods = run_oc_command(cmd="get pod", namespace="openshift-storage")

    if "Error in command" in pods:
        raise Exception("Can not get csi controller pod")

    provisioning_name = "csi-cephfsplugin-provisioner"
    if (
        interface == constants.CEPHBLOCKPOOL
        or interface == constants.CEPHBLOCKPOOL_THICK
    ):
        provisioning_name = "csi-rbdplugin-provisioner"

    for line in pods:
        if provisioning_name in line:
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
    logs = []
    for l in log_names:
        logs.append(
            run_oc_command(
                f"logs {l} -c {container_name} --since-time={start_time}",
                "openshift-storage",
            )
        )
    return logs


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

    if st is None:
        logger.error(f"Can not find start time of {pvc_name}")
        raise Exception(f"Can not find start time of {pvc_name}")

    if et is None:
        logger.error(f"Can not find end time of {pvc_name}")
        raise Exception(f"Can not find end time of {pvc_name}")

    total_time = (et - st).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    logger.info(f"Creation time for pvc {pvc_name} is {total_time} seconds")
    return total_time


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

    cnt_names = {
        constants.CEPHFILESYSTEM: "csi-cephfsplugin",
        constants.CEPHBLOCKPOOL: "csi-rbdplugin",
    }

    # Reading the CSI provisioner logs
    log_names = get_logfile_names(interface)
    logs = read_csi_logs(log_names, cnt_names[interface], start_time)

    st = None
    et = None
    for sublog in logs:
        for line in sublog:
            if (
                operation == "delete"
                and "generated Volume ID" in line
                and pv_name in line
            ):
                pv_name = line.split("(")[1].split(")")[0]
            if f"Req-ID: {pv_name} GRPC call:" in line:
                st = string_to_time(line.split(" ")[1])
            if f"Req-ID: {pv_name} GRPC response:" in line:
                et = string_to_time(line.split(" ")[1])

    if st is None:
        err_msg = f"Can not find CSI start time of {pvc_obj.name}"
        logger.error(err_msg)
        raise Exception(err_msg)

    if et is None:
        err_msg = f"Can not find CSI end time of {pvc_obj.name}"
        logger.error(err_msg)
        raise Exception(err_msg)

    total_time = (et - st).total_seconds()
    if total_time < 0:
        # for start-time > end-time (before / after midnigth) adding 24H to the time.
        total_time += 24 * 60 * 60

    logger.info(f"CSI time for pvc {pvc_obj.name} is {total_time} seconds")
    return total_time
