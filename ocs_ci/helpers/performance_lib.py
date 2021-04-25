import os
import logging
import subprocess
from datetime import datetime
import numpy

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
    if interface == constants.CEPHBLOCKPOOL:
        provisioning_name = "csi-rbdplugin-provisioner"

    for line in pods:
        if provisioning_name in line:
            log_names.append(line.split()[0])

    logger.info(f"The logs pods are : {log_names}")
    return log_names


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
    logs = []
    for l in log_names:
        logs.append(
            run_oc_command(
                f"logs {l} -c csi-provisioner --since-time={start_time}",
                "openshift-storage",
            )
        )

    format = "%H:%M:%S.%f"

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
                st = line.split(" ")[1]
                st = datetime.strptime(st, format)
            elif "provision" in line and pvc_name in line and "succeeded" in line:
                et = line.split(" ")[1]
                et = datetime.strptime(et, format)

    if st is None:
        logger.error(f"Can not find start time of {pvc_name}")
        raise Exception(f"Can not find start time of {pvc_name}")

    if et is None:
        logger.error(f"Can not find end time of {pvc_name}")
        raise Exception(f"Can not find end time of {pvc_name}")

    logger.info(
        f"Creation time (in seconds) for pvc {pvc_name} is {(et - st).total_seconds()}"
    )
    return (et - st).total_seconds()


def diff_check(first, second, diffs=10):
    """
    Function to check the differance between 2 numbers, and return true if the
    difference between them is more then expected

    Args:
        first (int) : the first number (usual the lowest)
        second (int) : the second number (usual the highest)
        diffs (int) : the acceptable difference between the number in percentage
           the default is 10%

    Return:
        bool : True if the difference is more then the acceptable, other False

    """
    try:
        # using the abs since the first number can be higher then the second
        # one, and in this case the results can be negative
        if abs((100 - (first * 100 / second))) > diffs:
            return True
    except ZeroDivisionError:
        pass
    return False


def cleanup_results_numbers_from_spikes(data, spike=10):
    """
    Function to cleanup list of results number from the highest and lowes numbers,
    usually, thous 2 numbers are 'noise' in the test.
    if the list of the number have less then 5 numbers, it will not do any cleanup
    the number of elements in the list must to be odd to prevent situation that we have half
    numbers are low and half numbers are high - which group of numbers are preferred ?

    This function going to handle few results scenarios :

        * 'sustain' results : [100, 99, 101, 100, 99] - do not need to be clean
        * 'bell' results : [100, 60, 101, 200, 99] - remove the highest/lowest results
        * 'half' results : [30, 100, 29, 31, 101] - return the largest group
        * 'mix' results : [100, 90, 70, 120, 80] - can not clean - bad results

    Args:
        data (list) : list of numbers - test results
        spike (int) : the acceptable percentage, default is 10% (each side - total of 20%)

    Returns:
        list : the list of the numbers without the Highest & Lowest numbers

    """

    # verify that the list of numbers is grater the 4, and with odd number of numbers
    if (len(data) < 5) or (len(data) % 2 == 0):
        logger.warning(
            "The list need to have more then 4 numbers and with odd number of numbers"
        )
        return data

    elements = numpy.array(data)

    # Getting the average of the numbers
    mean = numpy.mean(elements, axis=0)
    # Getting the standard deviation between the numbers
    sd = numpy.std(elements, axis=0)
    # calculating the percentage deviation between the numbers
    pct_dev = (sd / mean) * 100
    high = mean + sd  # Acceptable high number
    low = mean - sd  # Acceptable low number
    if pct_dev < spike * 2:
        # standard deviation is acceptable
        return data

    # remove the very high results
    logger.debug(f"removing results high then {high}")
    final_list = [x for x in data if (x < high)]
    # remove the very low results
    logger.debug(f"removing results low then {low}")
    final_list = [x for x in final_list if (x > low)]
    return final_list
