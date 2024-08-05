"""
Pod related functionalities and context info

Each pod in the openshift cluster will have a corresponding pod object
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import logging
import os
import re
import yaml
import tempfile
import time
import calendar
from threading import Thread
import base64
from semantic_version import Version

from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.ocp import get_images, OCP, verify_images_upgraded
from ocs_ci.helpers import helpers
from ocs_ci.helpers.proxy import update_container_with_proxy_env
from ocs_ci.ocs import constants, defaults, node, workload, ocp
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    CephToolBoxNotFoundException,
    CommandFailed,
    NotAllPodsHaveSameImagesError,
    NonUpgradedImagesFoundError,
    TimeoutExpiredError,
    UnavailableResourceException,
    ResourceNotFoundError,
    NotFoundError,
    TimeoutException,
    NoRunningCephToolBoxException,
)

from ocs_ci.ocs.utils import setup_ceph_toolbox, get_pod_name_by_pattern
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.job import get_job_obj, get_jobs_with_prefix
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    run_cmd,
    check_timeout_reached,
    TimeoutSampler,
    exec_cmd,
)
from ocs_ci.utility.utils import check_if_executable_in_path
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)
FIO_TIMEOUT = 600

TEXT_CONTENT = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
    "sed do eiusmod tempor incididunt ut labore et dolore magna "
    "aliqua. Ut enim ad minim veniam, quis nostrud exercitation "
    "ullamco laboris nisi ut aliquip ex ea commodo consequat. "
    "Duis aute irure dolor in reprehenderit in voluptate velit "
    "esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
    "occaecat cupidatat non proident, sunt in culpa qui officia "
    "deserunt mollit anim id est laborum."
)
TEST_FILE = "/var/lib/www/html/test"
FEDORA_TEST_FILE = "/mnt/test"


class Pod(OCS):
    """
    Handles per pod related context
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        kwargs:
            Copy of ocs/defaults.py::<some pod> dictionary
        """
        self.pod_data = kwargs
        # configure http[s]_proxy env variable, if applicable
        update_container_with_proxy_env(self.pod_data)
        super(Pod, self).__init__(**kwargs)

        with tempfile.NamedTemporaryFile(
            mode="w+", prefix="POD_", delete=False
        ) as temp_info:
            self.temp_yaml = temp_info.name
        self._name = self.pod_data.get("metadata").get("name")
        self._labels = self.get_labels()
        self._roles = []
        self.ocp = OCP(
            api_version=defaults.API_VERSION,
            kind=constants.POD,
            namespace=self.namespace,
        )
        self.fio_thread = None
        # TODO: get backend config !!

        self.wl_obj = None
        self.wl_setup_done = False

    @property
    def name(self):
        return self._name

    @property
    def namespace(self):
        return self._namespace

    @property
    def roles(self):
        return self._roles

    @property
    def labels(self):
        return self._labels

    @property
    def restart_count(self):
        return self.get().get("status").get("containerStatuses")[0].get("restartCount")

    def __setattr__(self, key, val):
        self.__dict__[key] = val

    def add_role(self, role):
        """
        Adds a new role for this pod

        Args:
            role (str): New role to be assigned for this pod
        """
        self._roles.append(role)

    def get_fio_results(self, timeout=FIO_TIMEOUT):
        """
        Get FIO execution results

        Returns:
            dict: Dictionary represents the FIO execution results

        Raises:
            Exception: In case of exception from FIO
        """
        logger.info(f"Waiting for FIO results from pod {self.name}")
        try:
            result = self.fio_thread.result(timeout)
            if result:
                return yaml.safe_load(result)
            raise CommandFailed(f"FIO execution results: {result}.")

        except CommandFailed as ex:
            logger.exception(f"FIO failed: {ex}")
            raise
        except Exception as ex:
            logger.exception(f"Found Exception: {ex}")
            raise

    def exec_cmd_on_pod(
        self,
        command,
        out_yaml_format=True,
        secrets=None,
        timeout=600,
        container_name=None,
        cluster_config=None,
        **kwargs,
    ):
        """
        Execute a command on a pod (e.g. oc rsh)

        Args:
            command (str): The command to execute on the given pod
            out_yaml_format (bool): whether to return yaml loaded python
                object OR to return raw output

            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(``**kwargs``)
            timeout (int): timeout for the exec_oc_cmd, defaults to 600 seconds
            container_name (str): The container name
            cluster_config (MultiClusterConfig): In case of multicluser scenario, this object will hold
                specific cluster's Config

        Returns:
            Munch Obj: This object represents a returned yaml file
        """
        if container_name:
            cmd = f"exec {self.name} -c {container_name} {command}"
        else:
            cmd = f"rsh {self.name} "
            cmd += command
        return self.ocp.exec_oc_cmd(
            cmd,
            out_yaml_format,
            secrets=secrets,
            timeout=timeout,
            cluster_config=cluster_config,
            **kwargs,
        )

    def exec_s3_cmd_on_pod(self, command, mcg_obj=None):
        """
        Execute an S3 command on a pod

        Args:
            mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
            command (str): The command to execute on the given pod

        Returns:
            Munch Obj: This object represents a returned yaml file
        """
        return self.exec_cmd_on_pod(
            craft_s3_command(command, mcg_obj),
            out_yaml_format=False,
            secrets=(
                [mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
                if mcg_obj
                else None
            ),
        )

    def copy_to_pod_rsync(self, src_path, target_path, container=None):
        """
        Copies to pod path from the local path

        Args:
            src_path (str): local path
            target_path (str): path within pod where you want to copy
            container (str): if multi-container pod you can specify the container name, by default its None

        Returns:
            str: stdout of the command

        """
        cmd = f"rsync {src_path} {self.name}:/{target_path}"
        if container:
            cmd = cmd + f" -c {container}"
        return self.ocp.exec_oc_cmd(cmd, out_yaml_format=False)

    def copy_to_pod_cat(self, src_path, target_path, timeout=60):
        """
        Copies to pod path from the local path file using standard output stream

        Args:
            src_path (str): local path
            target_path (str): path within pod where you want to copy
            timeout (int): timeout in seconds
        Returns:
            str: stdout of the command
        """
        cmd = f"cat {src_path} | oc exec -i {self.name} -n {self.namespace} -- sh -c 'cat > {target_path}'"
        return exec_cmd(cmd, timeout=timeout, shell=True)

    def copy_from_pod_oc_exec(
        self, target_path, src_path, timeout=600, chunk_size=2000
    ):
        """
        Copies to local path file from the pod using standard output stream via 'oc exec'. Good for log/json/yaml/text
        files, not good for large files/binaries with one-line plain string
        * Hand data from the pod over `oc exec cat`, other standard output ways will fail for the files > 1 Mb

        Args:
            target_path (str): local path
            src_path (str): path within pod what you want to copy
            timeout (int): timeout in seconds
                until size of the local file will not reach the initial file size.
                2000 lines is a maximum chunk size tested successfully
            chunk_size (int): file will be copied by chunks, by number of lines
        """
        file_size = int(self.exec_cmd_on_pod(f"stat -c %s {src_path}"))

        timestamp_end = datetime.now() + timedelta(seconds=timeout)

        start_line = 1
        cmd = f"oc exec -i {self.name} -n {self.namespace} -- bash -c "
        while os.path.exists(target_path) and datetime.now() <= timestamp_end:
            exec_cmd(
                cmd
                + f"\"tail -n '+{start_line}' {src_path}| head -n '{chunk_size}'\" >> {target_path}",
                timeout=timeout,
                shell=True,
            )
            start_line += chunk_size
            logger.info(f"size of target file = {os.stat(target_path).st_size}b")
            if os.stat(target_path).st_size == file_size:
                logger.info(f"file '{target_path}' copied")
                break
        else:
            raise TimeoutException(
                f"Failed to copy file from pod {self.name}:{src_path} to local path '{target_path}'\n"
                f"src file size = {src_path}b, target file size = {os.stat(target_path).st_size}b"
            )

    def exec_sh_cmd_on_pod(self, command, sh="bash", timeout=600, **kwargs):
        """
        Execute a pure bash command on a pod via oc exec where you can use
        bash syntaxt like &&, ||, ;, for loop and so on.

        Args:
            command (str): The command to execute on the given pod
            timeout (int): timeout for the exec_oc_cmd, defaults to 600 seconds

        Returns:
            str: stdout of the command
        """
        cmd = f'exec {self.name} -- {sh} -c "{command}"'
        return self.ocp.exec_oc_cmd(
            cmd, out_yaml_format=False, timeout=timeout, **kwargs
        )

    def get_labels(self):
        """
        Get labels from pod

        Raises:
            NotFoundError: If resource not found

        Returns:
            dict: All the openshift labels on a given pod
        """
        return self.pod_data.get("metadata").get("labels")

    def exec_ceph_cmd(
        self, ceph_cmd, format="json-pretty", out_yaml_format=True, timeout=600
    ):
        """
        Execute a Ceph command on the Ceph tools pod

        Args:
            ceph_cmd (str): The Ceph command to execute on the Ceph tools pod
            format (str): The returning output format of the Ceph command
            out_yaml_format (bool): whether to return yaml loaded python
                object OR to return raw output
            timeout (int): timeout for the exec_cmd_on_pod, defaults to 600 seconds

        Returns:
            dict: Ceph command output

        Raises:
            CommandFailed: In case the pod is not a toolbox pod
        """
        if "rook-ceph-tools" not in self.labels.values():
            raise CommandFailed("Ceph commands can be executed only on toolbox pod")
        ceph_cmd = ceph_cmd
        if format:
            ceph_cmd += f" --format {format}"
        out = self.exec_cmd_on_pod(
            ceph_cmd, out_yaml_format=out_yaml_format, timeout=timeout
        )

        # For some commands, like "ceph fs ls", the returned output is a list
        if isinstance(out, list):
            return [item for item in out if item]
        return out

    def get_storage_path(self, storage_type="fs"):
        """
        Get the pod volume mount path or device path

        Returns:
            str: The mount path of the volume on the pod (e.g. /var/lib/www/html/) if storage_type is fs
                 else device path of raw block pv
        """
        # TODO: Allow returning a path of a specified volume of a specified
        #  container
        if storage_type == "block":
            return (
                self.pod_data.get("spec")
                .get("containers")[0]
                .get("volumeDevices")[0]
                .get("devicePath")
            )

        return (
            self.pod_data.get("spec")
            .get("containers")[0]
            .get("volumeMounts")[0]
            .get("mountPath")
        )

    def workload_setup(self, storage_type, jobs=1, fio_installed=False):
        """
        Do setup on pod for running FIO

        Args:
            storage_type (str): 'fs' or 'block'
            jobs (int): Number of jobs to execute FIO
            fio_installed (bool): True if fio is already installed on the pod
        """
        work_load = "fio"
        name = f"test_workload_{work_load}"
        path = self.get_storage_path(storage_type)
        # few io parameters for Fio

        self.wl_obj = workload.WorkLoad(name, path, work_load, storage_type, self, jobs)
        if not (fio_installed and work_load == "fio"):
            assert self.wl_obj.setup(), f"Setup for FIO failed on pod {self.name}"
        self.wl_setup_done = True

    def run_io(
        self,
        storage_type,
        size,
        io_direction="rw",
        rw_ratio=75,
        jobs=1,
        runtime=60,
        depth=4,
        rate="1m",
        rate_process="poisson",
        fio_filename=None,
        bs="4K",
        end_fsync=0,
        invalidate=None,
        buffer_compress_percentage=None,
        buffer_pattern=None,
        readwrite=None,
        direct=0,
        verify=False,
        fio_installed=False,
        timeout=0,
    ):
        """
        Execute FIO on a pod
        This operation will run in background and will store the results in
        'self.thread.result()'.
        In order to wait for the output and not continue with the test until
        FIO is done, call self.thread.result() right after calling run_io.
        See tests/manage/test_pvc_deletion_during_io.py::test_run_io
        for usage of FIO

        Args:
            storage_type (str): 'fs' or 'block'
            size (str): Size in MB, e.g. '200M'
            io_direction (str): Determines the operation:
                'ro', 'wo', 'rw' (default: 'rw')
            rw_ratio (int): Determines the reads and writes using a
                <rw_ratio>%/100-<rw_ratio>%
                (e.g. the default is 75 which means it is 75%/25% which
                equivalent to 3 reads are performed for every 1 write)
            jobs (int): Number of jobs to execute FIO
            runtime (int): Number of seconds IO should run for
            depth (int): IO depth
            rate (str): rate of IO default 1m, e.g. 16k
            rate_process (str): kind of rate process default poisson, e.g. poisson
            fio_filename(str): Name of fio file created on app pod's mount point
            bs (str): Block size, e.g. 4K
            end_fsync (int): If 1, fio will sync file contents when a write
                stage has completed. Fio default is 0
            invalidate (bool): Invalidate the buffer/page cache parts of the files to be used prior to starting I/O
            buffer_compress_percentage (int): If this is set, then fio will attempt to provide I/O buffer
                content (on WRITEs) that compresses to the specified level
            buffer_pattern (str): fio will fill the I/O buffers with this pattern
            readwrite (str): Type of I/O pattern default is randrw from yaml
            direct(int): If value is 1, use non-buffered I/O. This is usually O_DIRECT. Fio default is 0.
            verify (bool): This method verifies file contents after each iteration of the job. e.g. crc32c, md5
            fio_installed (bool): True if fio is already installed on the pod
            timeout (int): The timeout in seconds to wait for fio to be completed

        """
        if not self.wl_setup_done:
            self.workload_setup(
                storage_type=storage_type, jobs=jobs, fio_installed=fio_installed
            )

        if io_direction == "rw":
            self.io_params = templating.load_yaml(constants.FIO_IO_RW_PARAMS_YAML)
            self.io_params["rwmixread"] = rw_ratio
        else:
            self.io_params = templating.load_yaml(constants.FIO_IO_PARAMS_YAML)
        if invalidate is not None:
            self.io_params["invalidate"] = invalidate
        elif storage_type == "block":
            self.io_params["invalidate"] = 0
        if runtime != 0:
            self.io_params["runtime"] = runtime
        else:
            del self.io_params["runtime"]
            del self.io_params["time_based"]
        size = size if isinstance(size, str) else f"{size}G"
        self.io_params["size"] = size
        if fio_filename:
            self.io_params["filename"] = fio_filename
        self.io_params["iodepth"] = depth
        self.io_params["rate"] = rate
        if rate_process is not None:
            self.io_params["rate_process"] = rate_process
        self.io_params["bs"] = bs
        self.io_params["direct"] = direct
        if buffer_compress_percentage:
            self.io_params["buffer_compress_percentage"] = buffer_compress_percentage
        if buffer_pattern:
            self.io_params["buffer_pattern"] = buffer_pattern
        if readwrite:
            self.io_params["readwrite"] = readwrite
        if end_fsync:
            self.io_params["end_fsync"] = end_fsync
        if verify:
            self.io_params["verify"] = config.RUN["io_verification_method"]
        if timeout != 0:
            self.io_params["timeout"] = timeout
        self.fio_thread = self.wl_obj.run(**self.io_params)

    def fillup_fs(self, size, fio_filename=None, performance_pod=False):
        """
        Execute FIO on a pod to fillup a file
        This will run sequential IO of 1MB block size to fill up the fill with data
        This operation will run in background and will store the results in
        'self.thread.result()'.
        In order to wait for the output and not continue with the test until
        FIO is done, call self.thread.result() right after calling run_io.
        See tests/manage/test_pvc_deletion_during_io.py::test_run_io
        for usage of FIO

        Args:
            size (str): Size in MB, e.g. '200M'
            fio_filename(str): Name of fio file created on app pod's mount point
            performance_pod (bool): True if this pod is the performance pod created with PERF_POD_YAML
        """

        if not self.wl_setup_done:
            self.workload_setup(
                storage_type="fs", jobs=1, fio_installed=performance_pod
            )

        self.io_params = templating.load_yaml(constants.FIO_IO_FILLUP_PARAMS_YAML)
        size = size if isinstance(size, str) else f"{size}M"
        self.io_params["size"] = size
        if fio_filename:
            self.io_params["filename"] = fio_filename
        self.fio_thread = self.wl_obj.run(**self.io_params)

    def run_git_clone(self, skip_install=True):
        """
        Execute git clone on a pod to simulate a Jenkins user

        Args:
            skip_install (bool): By default True, skips git package
                installation in pod

        """
        name = "test_workload"
        work_load = "jenkins"

        wl = workload.WorkLoad(
            name=name, work_load=work_load, pod=self, path=self.get_storage_path()
        )
        if not skip_install:
            assert wl.setup(), "Setup for git failed"
        wl.run()

    def install_packages(self, packages):
        """
        Install packages in a Pod

        Args:
            packages (list): List of packages to install

        """
        if isinstance(packages, list):
            packages = " ".join(packages)

        cmd = f"yum install {packages} -y"
        self.exec_cmd_on_pod(cmd, out_yaml_format=False)

    def copy_to_server(self, server, authkey, localpath, remotepath, user=None):
        """
        Upload a file from pod to server

        Args:
            server (str): Name of the server to upload
            authkey (str): Authentication file (.pem file)
            localpath (str): Local file/dir in pod to upload
            remotepath (str): Target path on the remote server
            user (str): User name to connect to server

        """
        if not user:
            user = "root"

        cmd = (
            f'scp -i {authkey} -o "StrictHostKeyChecking no"'
            f" -r {localpath} {user}@{server}:{remotepath}"
        )
        self.exec_cmd_on_pod(cmd, out_yaml_format=False)

    def exec_cmd_on_node(self, server, authkey, cmd, user=None):
        """
        Run command on a remote server from pod

        Args:
            server (str): Name of the server to run the command
            authkey (str): Authentication file (.pem file)
            cmd (str): command to run on server from pod
            user (str): User name to connect to server

        """
        if not user:
            user = "root"

        cmd = f'ssh -i {authkey} -o "StrictHostKeyChecking no" {user}@{server} {cmd}'
        self.exec_cmd_on_pod(cmd, out_yaml_format=False)

    def get_memory(self, container_name):
        """
        Get the pod memory size

        Args:
            container_name (str): The name of the container to look for

        Returns:
            str: The container memory size (e.g. '5Gi')

        """
        pod_containers = self.pod_data.get("spec").get("containers")
        matched_containers = [
            c for c in pod_containers if c.get("name") == container_name
        ]
        if len(matched_containers) > 1:
            logger.error(
                f"Multiple containers, of the same name, were found: {[c.get('name') for c in matched_containers]}"
            )
        container = matched_containers[0]
        return container.get("resources").get("limits").get("memory")

    def get_node(self):
        """
        Gets the node name

        Returns:
            str: Node name

        """
        if config.ENV_DATA.get(
            "platform", ""
        ).lower() == "aws" and config.DEPLOYMENT.get("local_storage"):
            return self.pod_data["spec"]["nodeSelector"]["kubernetes.io/hostname"]
        else:
            return self.pod_data["spec"]["nodeName"]

    def wait_for_pod_delete(self, resource_name=None, timeout=60):
        """
        Waits for the pod delete

        Args:
            resource_name(str): name of the pod
            timeout(int): time to wait in seconds
        Returns:
            bool: True

        """
        resource_name = resource_name if resource_name else self.name
        return self.ocp.wait_for_delete(resource_name, timeout=timeout)


# Helper functions for Pods


def get_all_pods(
    namespace=None,
    selector=None,
    selector_label="app",
    exclude_selector=False,
    wait=False,
    field_selector=None,
    cluster_kubeconfig="",
):
    """
    Get all pods in a namespace.

    Args:
        namespace (str): Name of the namespace
            If namespace is None - get all pods
        selector (list) : List of the resource selector to search with.
            Example: ['alertmanager','prometheus']
        selector_label (str): Label of selector (default: app).
        exclude_selector (bool): If list of the resource selector not to search with
        field_selector (str): Selector (field query) to filter on, supports
            '=', '==', and '!='. (e.g. status.phase=Running)
        wait (bool): True if you want to wait for the pods to be Running
        cluster_kubeconfig (str): Path to the kubeconfig file for the cluster

    Returns:
        list: List of Pod objects

    """

    ocp_pod_obj = OCP(
        kind=constants.POD,
        namespace=namespace,
        field_selector=field_selector,
        cluster_kubeconfig=cluster_kubeconfig,
    )
    # In case of >4 worker nodes node failures automatic failover of pods to
    # other nodes will happen.
    # So, we are waiting for the pods to come up on new node
    if wait:
        wait_time = 180
        logger.info(f"Waiting for {wait_time}s for the pods to stabilize")
        time.sleep(wait_time)
    pods = ocp_pod_obj.get()["items"]
    if selector:
        if exclude_selector:
            pods_new = [
                pod
                for pod in pods
                if pod["metadata"].get("labels", {}).get(selector_label) not in selector
            ]
        else:
            pods_new = [
                pod
                for pod in pods
                if pod["metadata"].get("labels", {}).get(selector_label) in selector
            ]
        pods = pods_new
    pod_objs = [Pod(**pod) for pod in pods]
    return pod_objs


def get_ceph_tools_pod(skip_creating_pod=False, wait=False, namespace=None):
    """
    Get the Ceph tools pod

    Args:
        skip_creating_pod (bool): True if user doesn't want to create new tool box
            if it doesn't exist
        wait (bool): True if you want to wait for the tool pods to be Running
        namespace: Namespace of OCS

    Returns:
        Pod object: The Ceph tools pod object

    Raises:
        ToolBoxNotFoundException: In case of tool box not found

    """
    from ocs_ci.ocs.managedservice import patch_consumer_toolbox

    if (
        config.multicluster
        and config.ENV_DATA.get("platform", "").lower()
        in constants.HCI_PC_OR_MS_PLATFORM
        and config.ENV_DATA.get("cluster_type", "").lower()
        in [constants.MS_CONSUMER_TYPE, constants.HCI_CLIENT]
    ):
        provider_kubeconfig = os.path.join(
            config.clusters[config.get_provider_index()].ENV_DATA["cluster_path"],
            config.clusters[config.get_provider_index()].RUN.get("kubeconfig_location"),
        )
        cluster_kubeconfig = provider_kubeconfig
    else:
        cluster_kubeconfig = config.ENV_DATA.get("provider_kubeconfig", "")

    if cluster_kubeconfig:
        namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
    else:
        namespace = namespace or config.ENV_DATA["cluster_namespace"]

    ocp_pod_obj = OCP(
        kind=constants.POD,
        namespace=namespace,
        selector=constants.TOOL_APP_LABEL,
        cluster_kubeconfig=cluster_kubeconfig,
    )
    ct_pod_items = ocp_pod_obj.data["items"]
    if not (ct_pod_items or skip_creating_pod):
        # setup ceph_toolbox pod if the cluster has been setup by some other CI
        setup_ceph_toolbox()
    namespace = namespace or config.ENV_DATA["cluster_namespace"]

    def _get_tools_pod_objs():
        """
        Method to get the Running ceph tool box pod objects

        Returns:
            List: of pod objects

        """
        ocp_pod_obj = OCP(
            kind=constants.POD,
            namespace=namespace,
            selector=constants.TOOL_APP_LABEL,
            cluster_kubeconfig=cluster_kubeconfig,
        )
        ct_pod_items = ocp_pod_obj.data["items"]
        logger.info(
            f"These are the ceph tool box pods: {[pod.get('metadata').get('name') for pod in ct_pod_items]}"
        )
        if not (ct_pod_items or skip_creating_pod):
            # setup ceph_toolbox pod if the cluster has been setup by some other CI
            setup_ceph_toolbox()
            ocp_pod_obj = OCP(
                kind=constants.POD,
                namespace=namespace,
                selector=constants.TOOL_APP_LABEL,
            )
            ct_pod_items = ocp_pod_obj.data["items"]

        if not ct_pod_items:
            raise CephToolBoxNotFoundException

        # In the case of node failure, the CT pod will be recreated with the old
        # one in status Terminated. Therefore, need to filter out the Terminated pod

        # Update the OCP pod object without the selector
        ocp_pod_obj = OCP(
            kind=constants.POD,
            namespace=namespace,
            cluster_kubeconfig=cluster_kubeconfig,
        )
        running_ct_pods = list()
        for pod in ct_pod_items:
            pod_status = ocp_pod_obj.get_resource_status(
                pod.get("metadata").get("name")
            )
            logger.info(f"Pod name: {pod.get('metadata').get('name')}")
            logger.info(f"Pod status: {pod_status}")
            if pod_status == constants.STATUS_RUNNING:
                running_ct_pods.append(pod)

        if not running_ct_pods:
            raise NoRunningCephToolBoxException
        return running_ct_pods

    if wait:
        running_ct_pods = retry(NoRunningCephToolBoxException, tries=10, delay=5)(
            _get_tools_pod_objs
        )()
    else:
        running_ct_pods = _get_tools_pod_objs()

    ceph_pod = Pod(**running_ct_pods[0])
    ceph_pod.ocp.cluster_kubeconfig = cluster_kubeconfig

    if (
        config.ENV_DATA.get("platform", "").lower() == constants.ROSA_PLATFORM
        and config.ENV_DATA.get("cluster_type", "").lower()
        == constants.MS_CONSUMER_TYPE
    ):
        # If the cluster is an MS consumer cluster, we need to patch the rook-ceph-tool box
        new_ceph_pod = patch_consumer_toolbox(consumer_tools_pod=ceph_pod)
        ceph_pod = new_ceph_pod or ceph_pod

    return ceph_pod


def get_csi_provisioner_pod(interface):
    """
    Get the provisioner pod based on interface
    Returns:
        Pod object: The provisioner pod object based on iterface
    """
    selector = (
        "app=csi-rbdplugin-provisioner"
        if (
            interface == constants.CEPHBLOCKPOOL
            or interface == constants.CEPHBLOCKPOOL_THICK
        )
        else "app=csi-cephfsplugin-provisioner"
    )
    ocp_pod_obj = OCP(
        kind=constants.POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=selector,
    )
    provision_pod_items = ocp_pod_obj.data["items"]
    assert provision_pod_items, f"No {interface} provisioner pod found"
    provisioner_pod = (
        Pod(**provision_pod_items[0]).name,
        Pod(**provision_pod_items[1]).name,
    )
    return provisioner_pod


def get_csi_snapshoter_pod():
    """
    Get the csi snapshot controller pod

    Returns:
        Pod object: csi snapshot controller pod

    """
    ocp_pod_obj = OCP(
        kind=constants.POD, namespace="openshift-cluster-storage-operator"
    )
    selector = "app=csi-snapshot-controller"
    snapshotner_pod = ocp_pod_obj.get(selector=selector)["items"]
    snapshotner_pod = Pod(**snapshotner_pod[0]).name
    return snapshotner_pod


def get_rgw_pods(rgw_label=constants.RGW_APP_LABEL, namespace=None):
    """
    Fetches info about rgw pods in the cluster

    Args:
        rgw_label (str): label associated with rgw pods
            (default: defaults.RGW_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: none)

    Returns:
        list: Pod objects of rgw pods

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    rgws = get_pods_having_label(rgw_label, namespace)
    return [Pod(**rgw) for rgw in rgws]


def get_ocs_operator_pod(ocs_label=constants.OCS_OPERATOR_LABEL, namespace=None):
    """
    Fetches info about rgw pods in the cluster

    Args:
        ocs_label (str): label associated with ocs_operator pod
            (default: defaults.OCS_OPERATOR_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: none)

    Returns:
        Pod object: ocs_operator pod object
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocs_operator = get_pods_having_label(ocs_label, namespace)
    ocs_operator_pod = Pod(**ocs_operator[0])
    return ocs_operator_pod


def get_noobaa_operator_pod(
    ocs_label=constants.NOOBAA_OPERATOR_POD_LABEL, namespace=None
):
    """
    Fetches info about noobaa operator pod in the cluster

    Args:
        ocs_label (str): label associated with noobaa_operator pod
            (default: defaults.NOOBAA_OPERATOR_POD_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: none)

    Returns:
        Pod object: noobaa_operator pod object

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    noobaa_operator = get_pods_having_label(ocs_label, namespace)
    noobaa_operator_pod = Pod(**noobaa_operator[0])
    return noobaa_operator_pod


def get_noobaa_db_pod():
    """
    Get noobaa db pod obj

    Returns:
        Pod object: Noobaa db pod object

    """
    nb_db = get_pods_having_label(
        label=constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    nb_db_pod = Pod(**nb_db[0])
    return nb_db_pod


def get_noobaa_core_pod():
    """
    Fetches Noobaa core pod details

    Returns:
        Pod object: Noobaa core pod object

    """
    noobaa_core = get_pods_having_label(
        label=constants.NOOBAA_CORE_POD_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    noobaa_core_pod = Pod(**noobaa_core[0])
    return noobaa_core_pod


def get_noobaa_endpoint_pods():
    """
    Fetches noobaa endpoint pod details

    Returns:
        List: List containing noobaa endpoint pod objects

    """
    noobaa_endpoints = get_pods_having_label(
        label=constants.NOOBAA_ENDPOINT_POD_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    noobaa_endpoint_pods = [Pod(**pod) for pod in noobaa_endpoints]
    return noobaa_endpoint_pods


def get_odf_operator_controller_manager(
    ocs_label=constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL, namespace=None
):
    """
    Fetches info about odf operator control manager pod in the cluster

    Args:
        ocs_label (str): label associated with ocs_operator pod
            (default: defaults.ODF_OPERATOR_CONTROL_MANAGER_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: none)

    Returns:
        Pod object: odf_operator_controller_manager pod object

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    odf_operator = get_pods_having_label(ocs_label, namespace)
    odf_operator_pod = Pod(**odf_operator[0])
    return odf_operator_pod


def get_alertmanager_managed_ocs_alertmanager_pods(
    label=constants.MANAGED_ALERTMANAGER_LABEL, namespace=None
):
    """
    Get alertmanager-managed-ocs-alertmanager pods in the cluster

    Args:
        label (str): Label associated with alertmanager-managed-ocs-alertmanager pods
        namespace (str): Namespace in which alertmanager-managed-ocs-alertmanager pods are residing

    Returns:
        list: Pod objects of alertmanager-managed-ocs-alertmanager pods

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    alertmanager_managed_pods = get_pods_having_label(label, namespace)
    return [
        Pod(**alertmanager_managed)
        for alertmanager_managed in alertmanager_managed_pods
    ]


def get_ocs_osd_controller_manager_pod(
    label=constants.MANAGED_CONTROLLER_LABEL, namespace=None
):
    """
    Get ocs-osd-controller-manager pod in the cluster

    Args:
        label (str): Label associated with ocs-osd-controller-manager pod
        namespace (str): Namespace in which ocs-osd-controller-manager pod is residing

    Returns:
        Pod: Pod object of ocs-osd-controller-manager pod

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    # odf-operator-controller-manager pod also have the same label. Select ocs-osd-controller-manager pod only.
    controller_manager_pods = [
        controller_manager
        for controller_manager in get_pods_having_label(label, namespace)
        if "ocs-osd-controller-manager" in controller_manager["metadata"]["name"]
    ]
    return Pod(**controller_manager_pods[0])


def get_prometheus_managed_ocs_prometheus_pod(
    label=constants.MANAGED_PROMETHEUS_LABEL, namespace=None
):
    """
    Get prometheus-managed-ocs-prometheus pod in the cluster

    Args:
        label (str): Label associated with prometheus-managed-ocs-prometheus pod
        namespace (str): Namespace in which prometheus-managed-ocs-prometheus pod is residing

    Returns:
        Pod: Pod object of prometheus-managed-ocs-prometheus pod

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    prometheus_managed_ocs_prometheus = get_pods_having_label(label, namespace)
    return Pod(**prometheus_managed_ocs_prometheus[0])


def get_prometheus_operator_pod(
    label=constants.PROMETHEUS_OPERATOR_LABEL, namespace=None
):
    """
    Get prometheus-operator pod in the cluster

    Args:
        label (str): Label associated with prometheus-operator pod
        namespace (str): Namespace in which prometheus-operator pod is residing

    Returns:
        Pod: Pod object of prometheus-operator pod

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    prometheus_operator = get_pods_having_label(label, namespace)
    return Pod(**prometheus_operator[0])


def get_ocs_provider_server_pod(label=constants.PROVIDER_SERVER_LABEL, namespace=None):
    """
    Get ocs-provider-server pod in the cluster

    Args:
        label (str): Label associated with ocs-provider-server pod
        namespace (str): Namespace in which ocs-provider-server pod is residing

    Returns:
        Pod: Pod object of ocs-provider-server pod

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocs_provider_server = get_pods_having_label(label, namespace)
    return Pod(**ocs_provider_server[0])


def get_lvm_vg_manager_pod(
    label=constants.LVMO_POD_LABEL["default"]["vg-manager_label"],
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Get vg manager pod in the lvm cluster

    Args:
        label (str): Label associated with vg manager pod
        namespace (str): Namespace in which vg manager pod is residing

    Returns:
        Pod: Pod object of vg manager pod

    """
    vg_manager = get_pods_having_label(label, namespace)
    return Pod(**vg_manager[0])


def get_lvm_operator_pod(
    label=constants.LVMO_POD_LABEL["default"]["controller_manager_label"],
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Get lvm operator controller manager pod in the lvm cluster

    Args:
        label (str): Label associated with lvm operator controller manager pod
        namespace (str): Namespace in which lvm operator controler manager pod is residing

    Returns:
        Pod: Pod object of lvm operator controller manager pod

    """
    lvm_operator = get_pods_having_label(label, namespace)
    return Pod(**lvm_operator[0])


def get_topolvm_controller_pod(
    label=constants.LVMO_POD_LABEL["default"]["topolvm-controller_label"],
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Get topolvm controller pod in the lvm cluster

    Args:
        label (str): Label associated with topolvm controller pod
        namespace (str): Namespace in which topolvm controler pod is residing

    Returns:
        Pod: Pod object of topolvm controller pod

    """
    topolvm_controller = get_pods_having_label(label, namespace)
    return Pod(**topolvm_controller[0])


def get_topolvm_node_pod(
    label=constants.LVMO_POD_LABEL["default"]["topolvm-node_label"],
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Get topolvm node pod in the lvm cluster

    Args:
        label (str): Label associated with topolvm node pod
        namespace (str): Namespace in which topolvm node pod is residing

    Returns:
        Pod: Pod object of topolvm node pod

    """
    topolvm_node_pod = get_pods_having_label(label, namespace)
    return Pod(**topolvm_node_pod[0])


def list_ceph_images(pool_name="rbd"):
    """
    Args:
        pool_name (str): Name of the pool to get the ceph images

    Returns (List): List of RBD images in the pool
    """
    ct_pod = get_ceph_tools_pod()
    return ct_pod.exec_ceph_cmd(ceph_cmd=f"rbd ls {pool_name}", format="json")


@retry(TypeError, tries=5, delay=2, backoff=1)
def check_file_existence(pod_obj, file_path):
    """
    Check if file exists inside the pod

    Args:
        pod_obj (Pod): The object of the pod
        file_path (str): The full path of the file to look for inside
            the pod

    Returns:
        bool: True if the file exist, False otherwise
    """
    try:
        check_if_executable_in_path(pod_obj.exec_cmd_on_pod("which find"))
    except CommandFailed:
        pod_obj.install_packages("findutils")
    ret = pod_obj.exec_cmd_on_pod(f'bash -c "find {file_path}"')
    if re.search(file_path, ret):
        return True
    return False


def get_file_path(pod_obj, file_name):
    """
    Get the full path of the file

    Args:
        pod_obj (Pod): The object of the pod
        file_name (str): The name of the file for which path to get

    Returns:
        str: The full path of the file
    """
    path = (
        pod_obj.get()
        .get("spec")
        .get("containers")[0]
        .get("volumeMounts")[0]
        .get("mountPath")
    )
    file_path = os.path.join(path, file_name)
    return file_path


def get_device_path(pod_obj):
    """
    get device path from pod in block mode
    Args:
         pod_obj (Pod): The object of the pod
    Returns:
          str: device path
    """

    return (
        pod_obj.get()
        .get("spec")
        .get("containers")[0]
        .get("volumeDevices")[0]
        .get("devicePath")
    )


def cal_md5sum(pod_obj, file_name, block=False, raw_path=False):
    """
    Calculates the md5sum of the file

    Args:
        pod_obj (Pod): The object of the pod
        file_name (str): The name of the file for which md5sum to be calculated
        block (bool): True if the volume mode of PVC used on pod is 'Block'.
            file_name will be the devicePath in this case.
        raw_path (bool): True if file_name includes the filepath and should be used as-is
                         i.e. - a pod without a PVC attached to it

    Returns:
        str: The md5sum of the file
    """
    if raw_path is False:
        file_path = (
            get_device_path(pod_obj) if block else get_file_path(pod_obj, file_name)
        )
    else:
        file_path = file_name

    md5sum_cmd_out = pod_obj.ocp.exec_oc_cmd(
        command=f"exec {pod_obj.name} -- md5sum {file_path}"
    )
    md5sum = md5sum_cmd_out.split()[0]

    logger.info(f"md5sum of file {file_name}: {md5sum}")
    return md5sum


def verify_data_integrity(pod_obj, file_name, original_md5sum, block=False):
    """
    Verifies existence and md5sum of file created from first pod

    Args:
        pod_obj (Pod): The object of the pod
        file_name (str): The name of the file for which md5sum to be calculated
        original_md5sum (str): The original md5sum of the file
        block (bool): True if the volume mode of PVC used on pod is 'Block'.
            file_name will be the devicePath in this case.

    Returns:
        bool: True if the file exists and md5sum matches

    Raises:
        AssertionError: If file doesn't exist or md5sum mismatch
    """
    file_path = file_name if block else get_file_path(pod_obj, file_name)
    assert check_file_existence(pod_obj, file_path), f"File {file_name} doesn't exists"
    current_md5sum = cal_md5sum(pod_obj, file_name, block)
    logger.info(f"Original md5sum of file: {original_md5sum}")
    logger.info(f"Current md5sum of file: {current_md5sum}")
    assert current_md5sum == original_md5sum, "Data corruption found"
    logger.info(f"File {file_name} exists and md5sum matches")
    return True


def verify_data_integrity_for_multi_pvc_objs(pod_objs, pvc_objs, file_name):
    """
    Verifies existence and md5sum of file created during IO, for all the pods.

    Args:
        pod_objs (list) : List of POD objects for which existence and md5sum of file created during IO needs to be
                            verified.
        pvc_objs (list) : List of original PVC objects.
        file_name (str) : The name of the file for which md5sum is to be calculated.

    Raises:
        AssertionError : Raises an exception if current md5sum does not match the original md5sum.

    """
    for pod_no in range(len(pod_objs)):
        pod_obj = pod_objs[pod_no]
        is_block = (
            True
            if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
            else False
        )
        file_name_pod = (
            file_name
            if not is_block
            else pod_obj.get_storage_path(storage_type="block")
        )
        logger.info(f"Verifying md5sum of {file_name_pod} " f"on pod {pod_obj.name}")
        verify_data_integrity(pod_obj, file_name_pod, pvc_objs[pod_no].md5sum)
        logger.info(
            f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
            f"matches with the original md5sum"
        )


def verify_data_integrity_after_expansion_for_block_pvc(pod_obj, pvc_obj, fio_size):
    """
    Verifies data integrity the block PVC obj, by comparing the md5sum of data written using FIO before
    expansion and after expansion.

    Args:
        pod_obj (Pod) : POD object for which md5sum of data written during FIO needs to be verified.
        pvc_obj (PVC) : Original PVC object before expansion.
        fio_size (int) : Size in MB of FIO.

    Raises:
        AssertionError : Raises an exception if current md5sum does not match the original md5sum.

    """
    logger.info(f"Verifying md5sum on pod {pod_obj.name}")
    # Read IO from given block PVCs using dd and calculate md5sum.
    # This dd command reads the data from the device, writes it to stdout, and reads md5sum from stdin.
    current_md5sum = pod_obj.exec_sh_cmd_on_pod(
        command=(
            f"dd iflag=direct if={pod_obj.get_storage_path(storage_type='block')} bs=10M "
            f"count={fio_size // 10} | md5sum"
        )
    )
    logger.info(f"Original md5sum of file: {pvc_obj.md5sum}")
    logger.info(f"Current md5sum of file: {current_md5sum}")
    assert current_md5sum == pvc_obj.md5sum, "Data corruption found"
    logger.info("md5sum matches")

    logger.info(
        f"Verified: md5sum of {pod_obj.get_storage_path(storage_type='block')} on pod {pod_obj.name} "
        f"matches with the original md5sum"
    )


def get_fio_rw_iops(pod_obj):
    """
    Execute FIO on a pod

    Args:
        pod_obj (Pod): The object of the pod
    """
    fio_result = pod_obj.get_fio_results()
    logger.info(f"FIO output: {fio_result}")
    logger.info("IOPs after FIO:")
    logger.info(f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}")
    logger.info(f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}")


def run_io_in_bg(pod_obj, expect_to_fail=False, fedora_dc=False):
    """
    Run I/O in the background

    Args:
        pod_obj (Pod): The object of the pod
        expect_to_fail (bool): True for the command to be expected to fail
            (disruptive operations), False otherwise
        fedora_dc (bool): set to False by default. If set to True, it runs IO in
            background on a fedora dc pod.

    Returns:
        Thread: A thread of the I/O execution
    """
    logger.info(f"Running I/O on pod {pod_obj.name}")

    def exec_run_io_cmd(pod_obj, expect_to_fail, fedora_dc):
        """
        Execute I/O
        """
        try:
            # Writing content to a new file every 0.01 seconds.
            # Without sleep, the device will run out of space very quickly -
            # 5-10 seconds for a 5GB device
            if fedora_dc:
                FILE = FEDORA_TEST_FILE
            else:
                FILE = TEST_FILE
            pod_obj.exec_cmd_on_pod(
                command=f'bash -c "let i=0; while true; do echo '
                f'{TEXT_CONTENT} >> {FILE}$i; let i++; sleep 0.01; done"',
                timeout=2400,
            )
        # Once the pod gets deleted, the I/O execution will get terminated.
        # Hence, catching this exception
        except CommandFailed as ex:
            if expect_to_fail:
                if re.search("code 137", str(ex)) or (re.search("code 143", str(ex))):
                    logger.info("I/O command got terminated as expected")
                    return
            raise ex

    thread = Thread(target=exec_run_io_cmd, args=(pod_obj, expect_to_fail, fedora_dc))
    thread.start()
    time.sleep(2)

    # Checking file existence
    if fedora_dc:
        FILE = FEDORA_TEST_FILE
    else:
        FILE = TEST_FILE
    test_file = FILE + "1"

    # Check I/O started
    try:
        for sample in TimeoutSampler(
            timeout=20,
            sleep=1,
            func=check_file_existence,
            pod_obj=pod_obj,
            file_path=test_file,
        ):
            if sample:
                break
            logger.info(f"Waiting for I/O to start inside {pod_obj.name}")
    except TimeoutExpiredError:
        logger.error(
            f"Wait timeout: I/O failed to start inside {pod_obj.name}. "
            "Collect file list."
        )
        parent_dir = os.path.join(TEST_FILE, os.pardir)
        pod_obj.exec_cmd_on_pod(
            command=f"ls -l {os.path.abspath(parent_dir)}", out_yaml_format=False
        )
        raise TimeoutExpiredError(f"I/O failed to start inside {pod_obj.name}")
    return thread


def get_admin_key_from_ceph_tools():
    """
    Fetches admin key secret from ceph
    Returns:
            admin keyring encoded with base64 as a string
    """
    tools_pod = get_ceph_tools_pod()
    out = tools_pod.exec_ceph_cmd(ceph_cmd="ceph auth get-key client.admin")
    base64_output = base64.b64encode(out["key"].encode()).decode()
    return base64_output


def run_io_and_verify_mount_point(pod_obj, bs="10M", count="950"):
    """
    Run I/O on mount point


    Args:
        pod_obj (Pod): The object of the pod
        bs (str): Read and write up to bytes at a time
        count (str): Copy only N input blocks

    Returns:
         used_percentage (str): Used percentage on mount point
    """
    pod_obj.exec_cmd_on_pod(
        command=f"dd if=/dev/urandom of=/var/lib/www/html/dd_a bs={bs} count={count}"
    )

    # Verify data's are written to mount-point
    mount_point = pod_obj.exec_cmd_on_pod(command="df -kh")
    mount_point = mount_point.split()
    used_percentage = mount_point[mount_point.index("/var/lib/www/html") - 1]
    return used_percentage


def get_pods_having_label(
    label,
    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    retry=0,
    cluster_config=None,
    statuses=None,
):
    """
    Fetches pod resources with given label in given namespace

    Args:
        label (str): label which pods might have
        namespace (str): Namespace in which to be looked up
        cluster_config (MultiClusterConfig): In case of multicluster, this object will hold
            specif cluster config
        statuses (list): List of pod statuses. Fetch only pods in any of the status mentioned
            in the statuses list
    Return:
        list: of pods info

    """
    ocp_pod = OCP(kind=constants.POD, namespace=namespace)
    pods = ocp_pod.get(selector=label, retry=retry, cluster_config=cluster_config).get(
        "items"
    )
    if statuses:
        for pod in pods:
            if pod["status"]["phase"] not in statuses:
                pods.remove(pod)
    return pods


def get_deployments_having_label(label, namespace):
    """
    Fetches deployment resources with given label in given namespace

    Args:
        label (str): label which deployments might have
        namespace (str): Namespace in which to be looked up

    Return:
        list: deployment OCP instances
    """
    ocp_deployment = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
    pods = ocp_deployment.get(selector=label).get("items")
    return pods


def get_mds_pods(mds_label=constants.MDS_APP_LABEL, namespace=None):
    """
    Fetches info about mds pods in the cluster

    Args:
        mds_label (str): label associated with mds pods
            (default: defaults.MDS_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : of mds pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    mdss = get_pods_having_label(mds_label, namespace)
    mds_pods = [Pod(**mds) for mds in mdss]
    return mds_pods


def get_mon_pods(mon_label=constants.MON_APP_LABEL, namespace=None):
    """
    Fetches info about mon pods in the cluster

    Args:
        mon_label (str): label associated with mon pods
            (default: defaults.MON_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : of mon pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    mons = get_pods_having_label(mon_label, namespace)
    mon_pods = [Pod(**mon) for mon in mons]
    return mon_pods


def get_mgr_pods(mgr_label=constants.MGR_APP_LABEL, namespace=None):
    """
    Fetches info about mgr pods in the cluster

    Args:
        mgr_label (str): label associated with mgr pods
            (default: defaults.MGR_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : of mgr pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    mgrs = get_pods_having_label(mgr_label, namespace)
    mgr_pods = [Pod(**mgr) for mgr in mgrs]
    return mgr_pods


def get_osd_pods(osd_label=constants.OSD_APP_LABEL, namespace=None):
    """
    Fetches info about osd pods in the cluster

    Args:
        osd_label (str): label associated with osd pods
            (default: defaults.OSD_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : of osd pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    osds = get_pods_having_label(osd_label, namespace)
    osd_pods = [Pod(**osd) for osd in osds]
    return osd_pods


def get_osd_prepare_pods(
    osd_prepare_label=constants.OSD_PREPARE_APP_LABEL,
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Fetches info about osd prepare pods in the cluster

    Args:
        osd_prepare_label (str): label associated with osd prepare pods
            (default: constants.OSD_PREPARE_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list: OSD prepare pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    osds = get_pods_having_label(osd_prepare_label, namespace)
    osd_pods = [Pod(**osd) for osd in osds]
    return osd_pods


def get_osd_deployments(osd_label=constants.OSD_APP_LABEL, namespace=None):
    """
    Fetches info about osd deployments in the cluster

    Args:
        osd_label (str): label associated with osd deployments
            (default: defaults.OSD_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list: OSD deployment OCS instances
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    osds = get_deployments_having_label(osd_label, namespace)
    osd_deployments = [OCS(**osd) for osd in osds]
    return osd_deployments


def get_pod_count(label, namespace=None):
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pods = get_pods_having_label(label=label, namespace=namespace)
    return len(pods)


def get_cephfsplugin_provisioner_pods(
    cephfsplugin_provisioner_label=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
    namespace=None,
):
    """
    Fetches info about CSI Cephfs plugin provisioner pods in the cluster

    Args:
        cephfsplugin_provisioner_label (str): label associated with cephfs
            provisioner pods
            (default: defaults.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : csi-cephfsplugin-provisioner Pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pods = get_pods_having_label(cephfsplugin_provisioner_label, namespace)
    fs_plugin_pods = [Pod(**pod) for pod in pods]
    return fs_plugin_pods


def get_rbdfsplugin_provisioner_pods(
    rbdplugin_provisioner_label=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
    namespace=None,
):
    """
    Fetches info about CSI Cephfs plugin provisioner pods in the cluster

    Args:
        rbdplugin_provisioner_label (str): label associated with RBD
            provisioner pods
            (default: defaults.CSI_RBDPLUGIN_PROVISIONER_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : csi-rbdplugin-provisioner Pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    pods = get_pods_having_label(rbdplugin_provisioner_label, namespace)
    ebd_plugin_pods = [Pod(**pod) for pod in pods]
    return ebd_plugin_pods


def get_pod_obj(name, namespace=None):
    """
    Returns the pod obj for the given pod

    Args:
        name (str): Name of the resources

    Returns:
        obj : A pod object
    """
    ocp_obj = OCP(api_version="v1", kind=constants.POD, namespace=namespace)
    ocp_dict = ocp_obj.get(resource_name=name)
    pod_obj = Pod(**ocp_dict)
    return pod_obj


def get_pod_logs(
    pod_name,
    container=None,
    namespace=config.ENV_DATA["cluster_namespace"],
    previous=False,
    all_containers=False,
    since=None,
):
    """
    Get logs from a given pod

    pod_name (str): Name of the pod
    container (str): Name of the container
    namespace (str): Namespace of the pod
    previous (bool): True, if pod previous log required. False otherwise.
    all_containers (bool): fetch logs from all containers of the resource
    since (str): only return logs newer than a relative duration like 5s, 2m, or 3h.

    Returns:
        str: Output from 'oc get logs <pod_name> command

    """
    pod = OCP(kind=constants.POD, namespace=namespace)
    cmd = f"logs {pod_name}"
    if container:
        cmd += f" -c {container}"
    if previous:
        cmd += " --previous"
    if all_containers:
        cmd += " --all-containers=true"
    if since:
        cmd += f" --since={since}"

    return pod.exec_oc_cmd(cmd, out_yaml_format=False)


def get_pod_node(pod_obj):
    """
    Get the node that the pod is running on

    Args:
        pod_obj (OCS): The pod object

    Returns:
        ocs_ci.ocs.ocp.OCP: The node object

    Raises:
        NotFoundError when the node name is not found

    """
    node_name = pod_obj.get().get("spec").get("nodeName")
    if not node_name:
        raise NotFoundError(f"Node name not found for the pod {pod_obj.name}")
    return node.get_node_objs(node_names=node_name)[0]


def delete_pods(pod_objs, wait=True):
    """
    Deletes list of the pod objects

    Args:
        pod_objs (list): List of the pod objects to be deleted
        wait (bool): Determines if the delete command should wait for
            completion

    """
    for pod in pod_objs:
        pod.delete(wait=wait)


def validate_pods_are_respinned_and_running_state(pod_objs_list):
    """
    Verifies the list of the pods are respinned and in running state

    Args:
        pod_objs_list (list): List of the pods obj

    Returns:
         bool : True if the pods are respinned and running, False otherwise

    Raises:
        ResourceWrongStatusException: In case the resources hasn't
            reached the Running state

    """
    for pod in pod_objs_list:
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING, timeout=180)

    for pod in pod_objs_list:
        pod_obj = pod.get()
        start_time = pod_obj["status"]["startTime"]
        ts = time.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        ts = calendar.timegm(ts)
        current_time_utc = time.time()
        sec = current_time_utc - ts
        if (sec / 3600) >= 1:
            logger.error(
                f"Pod {pod.name} is not respinned, the age of the pod is {start_time}"
            )
            return False

    return True


def verify_node_name(pod_obj, node_name):
    """
    Verifies that the pod is running on a particular node

    Args:
        pod_obj (Pod): The pod object
        node_name (str): The name of node to check

    Returns:
        bool: True if the pod is running on a particular node, False otherwise
    """

    logger.info(
        f"Checking whether the pod {pod_obj.name} is running on " f"node {node_name}"
    )
    actual_node = pod_obj.get().get("spec").get("nodeName")
    if actual_node == node_name:
        logger.info(
            f"The pod {pod_obj.name} is running on the specified node " f"{actual_node}"
        )
        return True
    else:
        logger.info(
            f"The pod {pod_obj.name} is not running on the specified node "
            f"specified node: {node_name}, actual node: {actual_node}"
        )
        return False


def get_pvc_name(pod_obj):
    """
    Function to get pvc_name from pod_obj

    Args:
        pod_obj (str): The pod object

    Returns:
        str: The pvc name of a given pod_obj,

    Raises:
        UnavailableResourceException: If no pvc attached

    """
    pvc = pod_obj.get().get("spec").get("volumes")[0].get("persistentVolumeClaim")
    if not pvc:
        raise UnavailableResourceException
    return pvc.get("claimName")


def get_used_space_on_mount_point(pod_obj):
    """
    Get the used space on a mount point

    Args:
        pod_obj (POD): The pod object

    Returns:
        int: Percentage represent the used space on the mount point

    """
    # Verify data's are written to mount-point
    mount_point = pod_obj.exec_cmd_on_pod(command="df -kh")
    mount_point = mount_point.split()
    used_percentage = mount_point[mount_point.index(constants.MOUNT_POINT) - 1]
    return used_percentage


def get_plugin_pods(interface, namespace=None):
    """
    Fetches info of csi-cephfsplugin pods or csi-rbdplugin pods

    Args:
        interface (str): Interface type. eg: CephBlockPool, CephFileSystem
        namespace (str): Name of cluster namespace

    Returns:
        list : csi-cephfsplugin pod objects or csi-rbdplugin pod objects
    """
    if interface == constants.CEPHFILESYSTEM:
        plugin_label = constants.CSI_CEPHFSPLUGIN_LABEL
    if interface == constants.CEPHBLOCKPOOL:
        plugin_label = constants.CSI_RBDPLUGIN_LABEL
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    plugins_info = get_pods_having_label(plugin_label, namespace)
    plugin_pods = [Pod(**plugin) for plugin in plugins_info]
    return plugin_pods


def get_plugin_provisioner_leader(interface, namespace=None, leader_type="provisioner"):
    """
    Get csi-cephfsplugin-provisioner or csi-rbdplugin-provisioner leader pod

    Args:
        interface (str): Interface type. eg: CephBlockPool, CephFileSystem
        namespace (str): Name of cluster namespace
        leader_type (str): Parameter to check the lease. eg: 'snapshotter' to
            select external-snapshotter leader holder

    Returns:
        Pod: csi-cephfsplugin-provisioner or csi-rbdplugin-provisioner leader
            pod

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    leader_types = {
        "provisioner": "csi-provisioner",
        "snapshotter": "csi-snapshotter",
        "resizer": "csi-resizer",
        "attacher": "csi-attacher",
    }

    non_leader_msg = "failed to acquire lease"
    lease_acq_msg = "successfully acquired lease"
    lease_renew_msg = "successfully renewed lease"
    leader_pod = ""

    if interface == constants.CEPHBLOCKPOOL:
        pods = get_rbdfsplugin_provisioner_pods(namespace=namespace)

    elif interface == constants.CEPHFILESYSTEM:
        pods = get_cephfsplugin_provisioner_pods(namespace=namespace)

    pods_log = {}
    for pod in pods:
        pods_log[pod] = get_pod_logs(
            pod_name=pod.name, container=leader_types[leader_type], namespace=namespace
        ).split("\n")

    for pod, log_list in pods_log.items():
        log_list.reverse()
        for log_msg in log_list:
            # Check for last occurrence of leader message
            # This will be the first occurrence in reversed list.
            if (lease_renew_msg in log_msg) or (lease_acq_msg in log_msg):
                curr_index = log_list.index(log_msg)
                # Ensure that there is no non leader message logged after
                # the last occurrence of leader message
                if not any(non_leader_msg in msg for msg in log_list[:curr_index]):
                    leader_pod = pod
                break
    assert leader_pod, "Couldn't identify plugin provisioner leader pod."
    logger.info(f"Plugin provisioner leader pod is {leader_pod.name}")
    return leader_pod


def get_operator_pods(operator_label=constants.OPERATOR_LABEL, namespace=None):
    """
    Fetches info about rook-ceph-operator pods in the cluster

    Args:
        operator_label (str): Label associated with rook-ceph-operator pod
        namespace (str): Namespace in which ceph cluster lives

    Returns:
        list : of rook-ceph-operator pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    operators = get_pods_having_label(operator_label, namespace)
    operator_pods = [Pod(**operator) for operator in operators]
    return operator_pods


def upload(pod_name, localpath, remotepath, namespace=None):
    """
    Upload a file to pod

    Args:
        pod_name (str): Name of the pod
        localpath (str): Local file to upload
        remotepath (str): Target path on the pod

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE
    cmd = (
        f"oc -n {namespace} cp {os.path.expanduser(localpath)} {pod_name}:{remotepath}"
    )
    run_cmd(cmd)


def download_file_from_pod(
    pod_name, remotepath, localpath, namespace=None, cluster_config=None
):
    """
    Download a file from a pod

    Args:
        pod_name (str): Name of the pod
        remotepath (str): Target path on the pod
        localpath (str): Local file to upload
        namespace (str): The namespace of the pod

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE
    kubeconfig = None
    cmd = "oc"
    if cluster_config:
        kubeconfig = cluster_config.RUN.get("kubeconfig")
        cmd = cmd + f" --kubeconfig {kubeconfig}"
    cmd = f"{cmd} -n {namespace} cp {pod_name}:{remotepath} {os.path.expanduser(localpath)}"
    run_cmd(cmd, cluster_config=cluster_config)


def wait_for_storage_pods(timeout=200):
    """
    Check all OCS pods status, they should be in Running or Completed state

    Args:
        timeout (int): Number of seconds to wait for pods to get into correct
            state

    """
    all_pod_obj = get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])

    # Ignoring detect version pods
    labels_to_ignore = [
        constants.ROOK_CEPH_DETECT_VERSION_LABEL,
        constants.CEPH_FILE_CONTROLLER_DETECT_VERSION_LABEL,
        constants.CEPH_OBJECT_CONTROLLER_DETECT_VERSION_LABEL,
    ]
    all_pod_obj = [
        pod
        for pod in all_pod_obj
        if pod.get_labels()
        and all(
            label[4:] not in pod.get_labels().values() for label in labels_to_ignore
        )
    ]

    for pod_obj in all_pod_obj:
        state = constants.STATUS_RUNNING
        if any(i in pod_obj.name for i in ["-1-deploy", "osd-prepare"]):
            state = constants.STATUS_COMPLETED
        helpers.wait_for_resource_state(resource=pod_obj, state=state, timeout=timeout)


def wait_for_noobaa_pods_running(timeout=300, sleep=10):
    """
    Wait until all the noobaa pods have reached status RUNNING

    Args:
        timeout (int): Timeout in seconds

    """

    def _check_nb_pods_status():
        nb_pod_labels = [
            constants.NOOBAA_CORE_POD_LABEL,
            constants.NOOBAA_ENDPOINT_POD_LABEL,
            constants.NOOBAA_OPERATOR_POD_LABEL,
            constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
        ]
        if config.ENV_DATA.get("noobaa_external_pgsql"):
            nb_pod_labels.remove(constants.NOOBAA_DB_LABEL_47_AND_ABOVE)
        nb_pods_running = list()
        for pod_label in nb_pod_labels:
            pods = get_pods_having_label(pod_label, statuses=[constants.STATUS_RUNNING])
            if len(pods) == 0:
                return False
            nb_pods_running.append(pod_label)
        return set(nb_pod_labels) == set(nb_pods_running)

    sampler = TimeoutSampler(timeout=timeout, sleep=10, func=_check_nb_pods_status)
    sampler.wait_for_func_status(True)


def verify_pods_upgraded(old_images, selector, count=1, timeout=720):
    """
    Verify that all pods do not have old image.

    Args:
       old_images (set): Set with old images.
       selector (str): Selector (e.g. app=ocs-osd)
       count (int): Number of resources for selector.
       timeout (int): Timeout in seconds to wait for pods to be upgraded.

    Raises:
        TimeoutException: If the pods didn't get upgraded till the timeout.

    """

    namespace = config.ENV_DATA["cluster_namespace"]
    info_message = (
        f"Waiting for {count} pods with selector: {selector} to be running "
        f"and upgraded."
    )
    logger.info(info_message)
    start_time = time.time()
    selector_label, selector_value = selector.split("=")
    while True:
        pod_count = 0
        pod_images = {}
        try:
            pods = get_all_pods(namespace, [selector_value], selector_label)
            pods_len = len(pods)
            logger.info(f"Found {pods_len} pod(s) for selector: {selector}")
            if pods_len != count:
                logger.warning(
                    f"Number of found pods {pods_len} is not as expected: " f"{count}"
                )
            for pod in pods:
                pod_obj = pod.get()
                verify_images_upgraded(old_images, pod_obj)
                current_pod_images = get_images(pod_obj)
                for container_name, container_image in current_pod_images.items():
                    if container_name not in pod_images:
                        pod_images[container_name] = container_image
                    else:
                        if pod_images[container_name] != container_image:
                            raise NotAllPodsHaveSameImagesError(
                                f"Not all the pods with the selector: {selector} have the same "
                                f"images! Image for container {container_name} has image {container_image} "
                                f"which doesn't match with: {pod_images} differ! This means "
                                "that upgrade hasn't finished to restart all the pods yet! "
                                "Or it's caused by other discrepancy which needs to be investigated!"
                            )
                pod_count += 1
        except CommandFailed as ex:
            logger.warning(
                f"Failed when getting pods with selector {selector}." f"Error: {ex}"
            )
        except (NonUpgradedImagesFoundError, NotAllPodsHaveSameImagesError) as ex:
            logger.warning(ex)
        check_timeout_reached(start_time, timeout, info_message)
        if pods_len != count:
            logger.error(f"Found pods: {pods_len} but expected: {count}!")
        elif pod_count == count:
            return


def get_noobaa_pods(noobaa_label=constants.NOOBAA_APP_LABEL, namespace=None):
    """
    Fetches info about noobaa pods in the cluster

    Args:
        noobaa_label (str): label associated with osd pods
            (default: defaults.NOOBAA_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : of noobaa pod objects
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    noobaas = get_pods_having_label(noobaa_label, namespace)
    noobaa_pods = [Pod(**noobaa) for noobaa in noobaas]

    return noobaa_pods


def wait_for_dc_app_pods_to_reach_running_state(
    dc_pod_obj, timeout=120, exclude_state=None
):
    """
    Wait for DC app pods to reach running state

    Args:
        dc_pod_obj (list): list of dc app pod objects
        timeout (int): Timeout in seconds to wait for pods to be in Running
            state.
        exclude_state (str): A resource state to ignore

    """
    for pod_obj in dc_pod_obj:
        name = pod_obj.get_labels().get("name")
        dpod_list = get_all_pods(selector_label=f"name={name}", wait=True)
        for dpod in dpod_list:
            if "-1-deploy" not in dpod.name and dpod.status != exclude_state:
                helpers.wait_for_resource_state(
                    dpod, constants.STATUS_RUNNING, timeout=timeout
                )


def delete_deploymentconfig_pods(pod_obj):
    """
    Delete a DeploymentConfig pod and all the pods that are controlled by it

    Args:
         pod_obj (Pod): Pod object

    """
    dc_ocp_obj = ocp.OCP(kind=constants.DEPLOYMENTCONFIG, namespace=pod_obj.namespace)
    pod_data_list = dc_ocp_obj.get().get("items")
    if pod_data_list:
        for pod_data in pod_data_list:
            if pod_obj.get_labels().get("name") == pod_data.get("metadata").get("name"):
                dc_ocp_obj.delete(resource_name=pod_obj.get_labels().get("name"))
                dc_ocp_obj.wait_for_delete(
                    resource_name=pod_obj.get_labels().get("name")
                )


def wait_for_new_osd_pods_to_come_up(number_of_osd_pods_before):
    status_options = ["Init:1/4", "Init:2/4", "Init:3/4", "PodInitializing", "Running"]
    try:
        for osd_pods in TimeoutSampler(timeout=180, sleep=3, func=get_osd_pods):
            # Check if the new osd pods has started to come up
            new_osd_pods = osd_pods[number_of_osd_pods_before:]
            new_osd_pods_come_up = [
                pod.status() in status_options for pod in new_osd_pods
            ]
            if any(new_osd_pods_come_up):
                logger.info("One or more of the new osd pods has started to come up")
                break
    except TimeoutExpiredError:
        logger.warning("None of the new osd pods reached the desired status")


def get_pod_restarts_count(
    namespace=config.ENV_DATA["cluster_namespace"], label=None, list_of_pods=None
):
    """
    Gets the dictionary of pod and its restart count for all the pods in a given namespace

    Returns:
        dict: dictionary of pod name and its corresponding restart count

    """
    if label:
        selector = label.split("=")[1]
        selector_label = label.split("=")[0]
    else:
        selector = None
        selector_label = None

    if not list_of_pods:
        list_of_pods = get_all_pods(
            namespace=namespace, selector=[selector], selector_label=selector_label
        )

    restart_dict = {}
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # we don't want to compare osd-prepare and canary pods as they get created freshly when an osd need to be added.
        if (
            "rook-ceph-osd-prepare" not in p.name
            and "rook-ceph-drain-canary" not in p.name
        ):
            pod_count = ocp_pod_obj.get_resource(p.name, "RESTARTS")
            restart_dict[p.name] = int(pod_count.split()[0])
    logger.info(f"get_pod_restarts_count: restarts dict = {restart_dict}")
    return restart_dict


def check_pods_in_running_state(
    namespace=config.ENV_DATA["cluster_namespace"],
    pod_names=None,
    raise_pod_not_found_error=False,
    skip_for_status=None,
    cluster_kubeconfig="",
):
    """
    Checks whether the pods in a given namespace are in Running state or not.
    The pods which are in 'Completed' state will be skipped when checking for all pods in the
    namespace openshift-storage. 'Completed' will be the expected state of such pods.

    Args:
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])
        pod_names (list): List of the pod names to check.
            If not provided, it will check all the pods in the given namespace
        raise_pod_not_found_error (bool): If True, it raises an exception, if one of the pods
            in the pod names are not found. If False, it ignores the case of pod not found and
            returns the pod objects of the rest of the pod names. The default value is False
        skip_for_status(list): List of pod status that should be skipped. If the status of a pod is in the given list,
            the check for 'Running' status of that particular pod will be skipped.
            eg: ["Pending", "Completed"]
        cluster_kubeconfig (str): The kubeconfig file to use for the oc command
    Returns:
        Boolean: True, if all pods in Running state. False, otherwise

    """
    ret_val = True

    if pod_names:
        list_of_pods = get_pod_objs(
            pod_names, raise_pod_not_found_error, cluster_kubeconfig=cluster_kubeconfig
        )
    else:
        list_of_pods = get_all_pods(namespace, cluster_kubeconfig=cluster_kubeconfig)

    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=namespace, cluster_kubeconfig=cluster_kubeconfig
    )
    for p in list_of_pods:
        # we don't want to compare osd-prepare and canary pods as they get created freshly when an osd need to be added.
        if (
            "rook-ceph-osd-prepare" not in p.name
            and "rook-ceph-drain-canary" not in p.name
        ):
            status = ocp_pod_obj.get_resource(p.name, "STATUS")
        if (
            ("rook-ceph-osd-prepare" not in p.name)
            and ("rook-ceph-drain-canary" not in p.name)
            and ("debug" not in p.name)
            and (constants.REPORT_STATUS_TO_PROVIDER_POD not in p.name)
        ):
            status = ocp_pod_obj.get_resource(p.name, "STATUS")
            if skip_for_status:
                if status in skip_for_status:
                    continue
            # Skip the pods which are in 'Completed' state when checking for all pods in the
            # namespace openshift-storage. 'Completed' will be the expected state of such pods.
            if (
                (status == constants.STATUS_COMPLETED)
                and (not pod_names)
                and (namespace == config.ENV_DATA["cluster_namespace"])
            ):
                logger.warning(
                    f"The pod {p.name} is not in {constants.STATUS_RUNNING} state, "
                    f"but in {constants.STATUS_COMPLETED} state."
                )
                continue
            if status not in "Running":
                logger.error(
                    f"The pod {p.name} is in {status} state. Expected = Running"
                )
                ret_val = False
    return ret_val


def get_running_state_pods(
    namespace=config.ENV_DATA["cluster_namespace"], ignore_selector=None
):
    """
    Checks the running state pods in a given namespace.

    Args:
        namespace (str): Cluster namespace where pods are running
        ignore_selector (list): List of pod selectors to ignore ( eg: ["rook-ceph-mgr", "rook-ceph-detect-version"] )

    Returns:
        List: all the pod objects that are in running state only

    """
    if ignore_selector:
        list_of_pods = get_all_pods(
            namespace, selector=ignore_selector, exclude_selector=True
        )
    else:
        list_of_pods = get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    running_pods_object = list()
    for pod in list_of_pods:
        status = ocp_pod_obj.get_resource(pod.name, "STATUS")
        if "Running" in status:
            running_pods_object.append(pod)

    return running_pods_object


def get_not_running_pods(selector=None, namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Get all the non-running pods in a given namespace
    and give selector

    Returns:
        List: all the pods that are not Running

    """

    if selector is not None:
        pod_objs = [
            Pod(**pod_info) for pod_info in get_pods_having_label(selector, namespace)
        ]
    else:
        pod_objs = get_all_pods(namespace)

    pods_not_running = list()
    for pod in pod_objs:
        status = pod.status()
        if status != constants.STATUS_RUNNING:
            pods_not_running.append(pod)

    return pods_not_running


def wait_for_pods_to_be_running(
    namespace=config.ENV_DATA["cluster_namespace"],
    pod_names=None,
    raise_pod_not_found_error=False,
    timeout=200,
    sleep=10,
    cluster_kubeconfig="",
):
    """
    Wait for all the pods in a specific namespace to be running.

    Args:
        namespace (str): the namespace ot the pods
        pod_names (list): List of the pod names to check.
            If not provided, it will check all the pods in the given namespace
        raise_pod_not_found_error (bool): If True, it raises an exception(in the function
            'check_pods_in_running_state'), if one of the pods in the pod names are not found.
            If False, it ignores the case of pod not found and returns the pod objects of
            the rest of the pod names. The default value is False
        timeout (int): time to wait for pods to be running
        sleep (int): Time in seconds to sleep between attempts
        cluster_kubeconfig (str): The kubeconfig file to use for the oc command

    Returns:
         bool: True, if all pods in Running state. False, otherwise

    """
    try:
        for pods_running in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=check_pods_in_running_state,
            namespace=namespace,
            pod_names=pod_names,
            raise_pod_not_found_error=raise_pod_not_found_error,
            cluster_kubeconfig=cluster_kubeconfig,
        ):
            # Check if all the pods in running state
            if pods_running:
                logger.info("All the pods reached status running!")
                return True

    except TimeoutExpiredError:
        logger.warning(
            f"Not all the pods reached status running " f"after {timeout} seconds"
        )
        return False


def wait_for_pods_by_label_count(
    label,
    exptected_count,
    namespace=config.ENV_DATA["cluster_namespace"],
    timeout=200,
    sleep=10,
):
    """

    Wait for the expected number of pods with the given selector.

    Args:
        selector (str): The resource selector to search with
        exptected_count (int): The expected number of pods with the given selector
        namespace (str): the namespace ot the pods
        timeout (int): time to wait for pods to be running
        sleep (int): Time in seconds to sleep between attempts

    """

    try:
        for pods_count in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_pod_count,
            label=label,
            namespace=namespace,
        ):
            # Check if the expected number of pods with the given selector is met
            if pods_count == exptected_count:
                logger.info(f"Found {exptected_count} pods with selector {label}")
                return True
    except TimeoutExpiredError:
        logger.warning(
            f"The expected number of pods was not met" f"after {timeout} seconds"
        )
        return False


def list_of_nodes_running_pods(
    selector, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    The function returns the list of nodes for the given selector

    Args:
        selector (str): The resource selector to search with

    Returns:
        list: a list of nodes that runs the given selector pods

    """
    pod_obj_list = get_all_pods(namespace=namespace, selector=[selector])
    pods_running_nodes = [get_pod_node(pod) for pod in pod_obj_list]
    logger.info(f"{selector} running on nodes {pods_running_nodes}")
    return list(set(pods_running_nodes))


def get_osd_removal_pod_name(timeout=60):
    """
    Get the osd removal pod name

    Args:
        timeout (int): The time to wait for getting the osd removal pod name

    Returns:
        str: The osd removal pod name

    """
    pattern = "ocs-osd-removal-job"
    try:
        for osd_removal_pod_names in TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_pod_name_by_pattern,
            pattern=pattern,
        ):
            if osd_removal_pod_names:
                osd_removal_pod_name = osd_removal_pod_names[0]
                logger.info(f"Found pod {osd_removal_pod_name}")
                return osd_removal_pod_name

    except TimeoutExpiredError:
        logger.warning(f"Failed to get pod by the pattern {pattern}")
        return None


def check_toleration_on_pods(toleration_key=constants.TOLERATION_KEY):
    """
    Function to check toleration on pods

    Args:
        toleration_key (str): The toleration key to check

    """

    pod_objs = get_all_pods(
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=[constants.TOOL_APP_LABEL],
        exclude_selector=True,
    )
    flag = False
    for pod_obj in pod_objs:
        resource_name = pod_obj.name
        tolerations = pod_obj.get().get("spec").get("tolerations")
        for key in tolerations:
            if key["key"] == toleration_key:
                flag = True
        if flag:
            logger.info(f"The Toleration {toleration_key} exists on {resource_name}")
        else:
            logger.error(
                f"The pod {resource_name} does not have toleration {toleration_key}"
            )


def run_osd_removal_job(osd_ids=None):
    """
    Run the ocs-osd-removal job

    Args:
        osd_ids (list): The osd IDs.

    Returns:
        ocs_ci.ocs.resources.ocs.OCS: The ocs-osd-removal job object

    """
    osd_ids_str = ",".join(map(str, osd_ids))

    cmd_params = "-p FORCE_OSD_REMOVAL=true"

    cmd_params += f" -p FAILED_OSD_IDS={osd_ids_str}"

    logger.info(f"Executing OSD removal job on OSD ids: {osd_ids_str}")
    ocp_obj = ocp.OCP(namespace=config.ENV_DATA["cluster_namespace"])
    osd_removal_job_yaml = ocp_obj.exec_oc_cmd(
        f"process ocs-osd-removal {cmd_params} -o yaml"
    )
    # Add the namespace param, so that the ocs-osd-removal job will be created in the correct namespace
    osd_removal_job_yaml["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    osd_removal_job = OCS(**osd_removal_job_yaml)
    osd_removal_job.create(do_reload=False)

    return osd_removal_job


def check_safe_to_destroy_status(osd_id):
    """
    check if it is safe to destroy the osd

    Args:
        osd_id (str): osd id

    Return:
        bool: True, if it is safe to destroy the osd. False, otherwise

    """
    try:
        pod_tool = get_ceph_tools_pod()
        out = pod_tool.exec_cmd_on_pod(
            command=f"ceph osd safe-to-destroy {osd_id}",
            out_yaml_format=False,
        )
    except Exception as e:
        logger.error(e)
        return False
    return "are safe to destroy without reducing data durability" in out


def verify_osd_removal_job_completed_successfully(osd_id):
    """
    Verify that the ocs-osd-removal job completed successfully

    Args:
        osd_id (str): The osd id

    Returns:
        bool: True, if the ocs-osd-removal job completed successfully. False, otherwise

    """
    logger.info("Getting the ocs-osd-removal pod name")
    osd_removal_pod_name = get_osd_removal_pod_name()
    osd_removal_pod_obj = get_pod_obj(
        osd_removal_pod_name, namespace=config.ENV_DATA["cluster_namespace"]
    )

    timeout = 300
    try:
        is_completed = osd_removal_pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=osd_removal_pod_name,
            sleep=20,
            timeout=timeout,
        )
    # Don't failed the test yet if the ocs-osd-removal pod job is not completed
    except TimeoutExpiredError:
        is_completed = False

    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
    )
    osd_removal_pod_status = ocp_pod_obj.get_resource_status(osd_removal_pod_name)

    # Check if 'osd_removal_pod' is in status 'completed'
    if not is_completed and osd_removal_pod_status != constants.STATUS_COMPLETED:
        if osd_removal_pod_status != constants.STATUS_RUNNING:
            logger.info(
                f"ocs-osd-removal pod job did not reach status '{constants.STATUS_COMPLETED}' "
                f"or '{constants.STATUS_RUNNING}' after {timeout} seconds"
            )
            return False
        else:
            logger.info(
                f"ocs-osd-removal pod job reached status '{constants.STATUS_RUNNING}',"
                f" but we were waiting for status '{constants.STATUS_COMPLETED}' "
            )

            new_timeout = 900
            logger.info(
                f"Wait more {new_timeout} seconds for ocs-osd-removal pod job to be completed"
            )
            is_completed = osd_removal_pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_COMPLETED,
                resource_name=osd_removal_pod_name,
                sleep=30,
                timeout=new_timeout,
            )
            if not is_completed:
                logger.info(
                    f"ocs-osd-removal pod job did not complete after {new_timeout} seconds"
                )
                return False

    # Verify OSD removal from the ocs-osd-removal pod logs
    logger.info(f"Verifying removal of OSD from {osd_removal_pod_name} pod logs")
    logs = get_pod_logs(osd_removal_pod_name)
    patterns = [
        f"purged osd.{osd_id}",
        f"purge osd.{osd_id}",
        f"completed removal of OSD {osd_id}",
    ]

    is_osd_pattern_in_logs = any([re.search(pattern, logs) for pattern in patterns])
    if not is_osd_pattern_in_logs:
        logger.warning(
            f"Didn't find the removal of OSD from {osd_removal_pod_name} pod logs"
        )
        return False

    return True


def delete_osd_removal_job(osd_id):
    """
    Delete the ocs-osd-removal job.

    Args:
        osd_id (str): The osd id

    Returns:
        bool: True, if the ocs-osd-removal job deleted successfully. False, otherwise

    """
    ocs_version = config.ENV_DATA["ocs_version"]
    if Version.coerce(ocs_version) >= Version.coerce("4.7"):
        job_name = "ocs-osd-removal-job"
    else:
        job_name = f"ocs-osd-removal-{osd_id}"

    osd_removal_job = get_job_obj(
        job_name, namespace=config.ENV_DATA["cluster_namespace"]
    )
    osd_removal_job.delete()
    try:
        osd_removal_job.ocp.wait_for_delete(resource_name=job_name)
    except TimeoutError:
        logger.warning(f"{job_name} job did not get deleted successfully")
        return False

    return True


def get_deployment_name(pod_name):
    """
    Get the deployment of the pod.

    Args:
        pod_name (str): The pod's name.

    Returns:
        The deployment of the specific pod name

    """
    return "-".join(pod_name.split("-")[:-2])


def get_osd_pod_id(osd_pod):
    """
    Get the osd pod id

    Args:
        osd_pod (ocs_ci.ocs.resources.pod.Pod): The osd pod object

    Returns:
        str: The osd pod id

    """
    return osd_pod.get().get("metadata").get("labels").get("ceph-osd-id")


def get_pods_in_statuses(
    status_options,
    namespace=config.ENV_DATA["cluster_namespace"],
    exclude_pod_name_prefixes=None,
):
    """
    Get all the pods in specific statuses

    Args:
        status_options (list): The list of the status options.
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])
        exclude_pod_name_prefixes (list): The list of the pod name prefixes to exclude from the pods to get

    Returns:
        list: All the pods that their status in the 'status_options' list.

    """
    pods = get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    pods_in_status_options = list()
    for p in pods:
        try:
            pod_status = ocp_pod_obj.get_resource_status(p.name)
        except CommandFailed as e:
            logger.info(f"Can't get the pod status due to the error: {str(e)}")
            pod_status = ""

        if pod_status in status_options:
            pods_in_status_options.append(p)

    if exclude_pod_name_prefixes:
        exclude_pod_name_prefixes = tuple(exclude_pod_name_prefixes)
        pods_in_status_options = [
            p
            for p in pods_in_status_options
            if not p.name.startswith(exclude_pod_name_prefixes)
        ]

    return pods_in_status_options


def get_pod_ceph_daemon_type(pod_obj):
    """
    Get the ceph daemon type of the pod object

    Args:
        pod_obj (Pod): the pod object

    Returns:
        str: The pod's ceph daemon type

    """
    return pod_obj.get_labels().get("ceph_daemon_type")


def check_pods_after_node_replacement():
    """
    Check the pods status after the node replacement process.

    Returns:
        bool: True if all the pods are running after a specific time. False otherwise.

    """
    # Import here to avoid circular loop
    from ocs_ci.ocs.cluster import is_ms_provider_cluster

    are_pods_running = wait_for_pods_to_be_running(timeout=240)
    if are_pods_running:
        return True

    # This is a workaround due to the BZ https://bugzilla.redhat.com/show_bug.cgi?id=2142461
    failed_pod_statuses = [
        "Init:0/1",
        constants.STATUS_CONTAINER_CREATING,
        constants.STATUS_CLBO,
    ]
    restart_pods_in_statuses(failed_pod_statuses)

    not_ready_statuses = [
        constants.STATUS_ERROR,
        constants.STATUS_PENDING,
        constants.STATUS_CLBO,
        constants.STATUS_TERMINATING,
    ]

    pods_not_ready = get_pods_in_statuses(
        status_options=not_ready_statuses,
        exclude_pod_name_prefixes=[constants.REPORT_STATUS_TO_PROVIDER_POD],
    )
    if len(pods_not_ready) == 0:
        logger.info("All the pods are running")
        return True

    if len(pods_not_ready) > 1:
        logger.warning("More than one pod is not running")
        return False

    # if len(pods_not_ready) == 1
    pod_not_ready = pods_not_ready[0]
    pod_daemon_type = get_pod_ceph_daemon_type(pod_not_ready)
    if pod_daemon_type == constants.MON_DAEMON:
        logger.info(
            f"One of the '{pod_daemon_type}' pods is not running, "
            f"but all the other pods are running"
        )
        timeout = 1800 if is_ms_provider_cluster() else 1500
        logger.info(
            f"waiting another {timeout} seconds for all the pods to be running..."
        )

        expected_statuses = [constants.STATUS_RUNNING, constants.STATUS_COMPLETED]
        are_pods_running = wait_for_pods_to_be_in_statuses(
            expected_statuses=expected_statuses,
            timeout=timeout,
            sleep=30,
            exclude_pod_name_prefixes=constants.REPORT_STATUS_TO_PROVIDER_POD,
        )
        if are_pods_running:
            logger.info("All the pods are running")
            return True
        else:
            logger.warning(
                f"Not all the pods are in a running state after {timeout} seconds"
            )
            return False

    else:
        logger.warning(f"One of the '{pod_daemon_type}' pods is not running")
        return False


def get_osd_pods_having_ids(osd_ids):
    """
    Get the osd pods having specific ids

    Args:
        osd_ids (list): The list of the osd ids

    Returns:
        list: The osd pods having the osd ids

    """
    # Convert it to set to reduce complexity
    osd_ids_set = set(osd_ids)
    osd_pods_having_ids = []

    osd_pods = get_osd_pods()
    for osd_pod in osd_pods:
        if get_osd_pod_id(osd_pod) in osd_ids_set:
            osd_pods_having_ids.append(osd_pod)

    return osd_pods_having_ids


def get_pod_objs(
    pod_names,
    raise_pod_not_found_error=False,
    namespace=config.ENV_DATA["cluster_namespace"],
    cluster_kubeconfig="",
):
    """
    Get the pod objects of the specified pod names

    Args:
        pod_names (list): The list of the pod names to get their pod objects
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])
        raise_pod_not_found_error (bool): If True, it raises an exception, if one of the pods
            in the pod names are not found. If False, it ignores the case of pod not found and
            returns the pod objects of the rest of the pod names. The default value is False
        cluster_kubeconfig (str): The kubeconfig file to use for the oc command

    Returns:
        list: The pod objects of the specified pod names

    Raises:
        ResourceNotFoundError: If 'raise_pod_not_found_error' is True,
            and not all the pod names were found

    """
    # Convert it to set to reduce complexity
    pod_names_set = set(pod_names)
    pods = get_all_pods(namespace=namespace, cluster_kubeconfig=cluster_kubeconfig)
    pod_objs_found = [p for p in pods if p.name in pod_names_set]

    if len(pod_names) > len(pod_objs_found):
        pod_names_found_set = {p.name for p in pod_objs_found}
        pod_names_not_found = list(pod_names_set - pod_names_found_set)
        error_message = f"Did not find the following pod names: {pod_names_not_found}"
        if raise_pod_not_found_error:
            raise ResourceNotFoundError(error_message)
        else:
            logger.info(error_message)

    return pod_objs_found


def wait_for_change_in_pods_statuses(
    pod_names,
    current_statuses=None,
    namespace=config.ENV_DATA["cluster_namespace"],
    timeout=300,
    sleep=20,
):
    """
    Wait for the pod statuses in a specific namespace to change.

    Args:
        pod_names (list): List of the pod names to check if their status changed.
        namespace (str): the namespace ot the pods
        current_statuses (list): The current pod statuses. These are the pod statuses
            to check if they changed during each iteration.
        timeout (int): time to wait for pod statuses to change
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        bool: True, if the pod statuses have changed. False, otherwise

    """
    if current_statuses is None:
        # If 'current_statuses' is None the default value will be the ready statues
        current_statuses = [constants.STATUS_RUNNING, constants.STATUS_COMPLETED]

    try:
        for pod_objs in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_pod_objs,
            namespace=namespace,
            pod_names=pod_names,
        ):
            ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
            if len(pod_objs) < len(pod_names):
                pod_names_found_set = {p.name for p in pod_objs}
                pod_names_not_found = list(set(pod_names) - pod_names_found_set)
                logger.info(f"Some of the pods have not found: {pod_names_not_found}")
                return True

            for p in pod_objs:
                try:
                    pod_status = ocp_pod_obj.get_resource_status(p.name)
                except CommandFailed as ex:
                    logger.info(
                        f"Can't get the status of the pod {p.name} due to the error: {ex}"
                    )
                    continue

                if pod_status not in current_statuses:
                    logger.info(
                        f"The status of the pod '{p.name}' has changed to '{pod_status}'"
                    )
                    return True
    except TimeoutExpiredError:
        logger.info(f"The status of the pods did not change after {timeout} seconds")
        return False


def get_rook_ceph_pod_names():
    """
    Get all the rook ceph pod names

    Returns:
        list: List of the rook ceph pod names

    """
    rook_ceph_pod_names = get_pod_name_by_pattern("rook-ceph-")
    # Exclude the rook ceph pod tools because it creates by OCS and not rook ceph operator
    return [
        pod_name
        for pod_name in rook_ceph_pod_names
        if not pod_name.startswith("rook-ceph-tools-")
    ]


def get_mon_pod_id(mon_pod):
    """
    Get the mon pod id

    Args:
        mon_pod (ocs_ci.ocs.resources.pod.Pod): The mon pod object

    Returns:
        str: The mon pod id

    """
    return mon_pod.get().get("metadata").get("labels").get("ceph_daemon_id")


def delete_all_osd_removal_jobs(namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Delete all the osd removal jobs in a specific namespace

    Args:
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])

    Returns:
        bool: True, if all the jobs deleted successfully. False, otherwise

    """
    result = True
    osd_removal_jobs = get_jobs_with_prefix("ocs-osd-removal-", namespace=namespace)
    for osd_removal_job in osd_removal_jobs:
        osd_removal_job.delete()
        try:
            osd_removal_job.ocp.wait_for_delete(resource_name=osd_removal_job.name)
        except TimeoutError:
            logger.warning(
                f"{osd_removal_job.name} job did not get deleted successfully"
            )
            result = False

    return result


def get_crashcollector_pods(
    crashcollector_label=constants.CRASHCOLLECTOR_APP_LABEL, namespace=None
):
    """
    Fetches info about crashcollector pods in the cluster

    Args:
        crashcollector_label (str): label associated with mon pods
            (default: defaults.CRASHCOLLECTOR_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list : of crashcollector pod objects

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    crashcollectors = get_pods_having_label(crashcollector_label, namespace)
    return [Pod(**crashcollector) for crashcollector in crashcollectors]


def check_pods_in_statuses(
    expected_statuses,
    pod_names=None,
    namespace=config.ENV_DATA["cluster_namespace"],
    raise_pod_not_found_error=False,
    exclude_pod_name_prefixes=None,
):
    """
    checks whether the pods in a given namespace are in the expected statuses or not

    Args:
        expected_statuses (list): The expected statuses of the pods
        pod_names (list): List of the pod names to check.
            If not provided, it will check all the pods in the given namespace
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])
        raise_pod_not_found_error (bool): If True, it raises an exception, if one of the pods
            in the pod names are not found. If False, it ignores the case of pod not found and
            check the pod objects of the rest of the pod names. The default value is False
        exclude_pod_name_prefixes (list): The list of the pod name prefixes to exclude from the pods to check

    Returns:
        Boolean: True, if the pods are in the expected statuses. False, otherwise

    """
    if pod_names:
        list_of_pods = get_pod_objs(
            pod_names=pod_names,
            raise_pod_not_found_error=raise_pod_not_found_error,
            namespace=namespace,
        )
    else:
        list_of_pods = get_all_pods(namespace)

    if exclude_pod_name_prefixes:
        exclude_pod_name_prefixes = tuple(exclude_pod_name_prefixes)
        list_of_pods = [
            p for p in list_of_pods if not p.name.startswith(exclude_pod_name_prefixes)
        ]

    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        try:
            status = ocp_pod_obj.get_resource(p.name, "STATUS")
        except CommandFailed as e:
            logger.info(f"Can't get the pod status due to the error: {str(e)}")
            status = ""

        if status not in expected_statuses:
            logger.warning(
                f"The pod {p.name} is in {status} state, and not in the expected statuses {expected_statuses}"
            )
            return False

    logger.info(f"All the pods reached the expected statuses {expected_statuses}")
    return True


def wait_for_pods_to_be_in_statuses(
    expected_statuses,
    pod_names=None,
    namespace=config.ENV_DATA["cluster_namespace"],
    raise_pod_not_found_error=False,
    exclude_pod_name_prefixes=None,
    timeout=180,
    sleep=10,
):
    """
    Wait for the pods in a given namespace to be in the expected statuses

    Args:
        expected_statuses (list): The expected statuses of the pods
        pod_names (list): List of the pod names to check.
            If not provided, it will check all the pods in the given namespace
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])
        raise_pod_not_found_error (bool): If True, it raises an exception, if one of the pods
            in the pod names are not found. If False, it ignores the case of pod not found and
            check the pod objects of the rest of the pod names. The default value is False
        exclude_pod_name_prefixes (list): The list of the pod name prefixes to exclude from the pods to check
        timeout (int): time to wait for the pods to be in the expected statuses
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        Boolean: True, if all pods are in the expected statuses. False, otherwise
    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=check_pods_in_statuses,
        expected_statuses=expected_statuses,
        pod_names=pod_names,
        namespace=namespace,
        raise_pod_not_found_error=raise_pod_not_found_error,
        exclude_pod_name_prefixes=exclude_pod_name_prefixes,
    )
    return sample.wait_for_func_status(result=True)


def wait_for_pods_to_be_in_statuses_concurrently(
    app_selectors_to_resource_count_list,
    namespace,
    timeout=1200,
    status=constants.STATUS_RUNNING,
    cluster_kubeconfig="",
):
    """
    Verify pods are running in the namespace using app selectors. This method is using concurrent futures to
    speed up execution and will be blocking until all pods are running or timeout is reached

    Args:
        app_selectors_to_resource_count_list:
        namespace: namespace of the pods expected to run
        timeout: time to wait for the pods to be running in seconds
        status: status of the pods to wait for
        cluster_kubeconfig: The kubeconfig file to use for the oc command

    Returns:
        bool: True if all pods are running, False otherwise
    """
    pod = OCP(
        kind=constants.POD, namespace=namespace, cluster_kubeconfig=cluster_kubeconfig
    )
    results = dict()

    def check_pod_status(app_selector, resource_count):
        results[app_selector] = pod.wait_for_resource(
            condition=status,
            selector=app_selector,
            resource_count=resource_count,
            timeout=timeout,
        )

    with ThreadPoolExecutor(
        max_workers=len(app_selectors_to_resource_count_list)
    ) as executor:
        futures = []
        for item in app_selectors_to_resource_count_list:
            for app_selector, resource_count in item.items():
                futures.append(
                    executor.submit(check_pod_status, app_selector, resource_count)
                )

        [future.result() for future in futures]

    return all(value for value in results.values())


def get_pod_ip(pod_obj):
    """
    Get the pod ip

    Args:
        pod_obj (Pod): The pod object

    Returns:
        str: The pod ip

    """
    return pod_obj.get().get("status").get("podIP")


def wait_for_osd_pods_having_ids(osd_ids, timeout=180, sleep=10):
    """
    Wait for the osd pods having specific ids

    Args:
        osd_ids (list): The list of the osd ids
        timeout (int): Time to wait for the osd pods having the specified ids
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        list: The osd pods having the specified ids

    Raise:
        TimeoutExpiredError: In case it didn't find all the osd pods with the specified ids

    """
    for osd_pods in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_osd_pods_having_ids,
        osd_ids=osd_ids,
    ):
        if len(osd_pods) == len(osd_ids):
            logger.info(f"Found all the osd pods with the ids: {osd_ids}")
            return osd_pods


def pod_resource_utilization_raw_output_from_adm_top(
    namespace=config.ENV_DATA["cluster_namespace"],
):
    """
    Gets the pod's memory utilization using adm top command.

    Args:
        namespace (str) : The pod's namespace where the adm top command has to be run

    Returns:
        str : Raw output of adm top pods command

    """
    obj = ocp.OCP()
    resource_utilization_all_pods = obj.exec_oc_cmd(
        command=f"adm top pods -n {namespace}", out_yaml_format=False
    )
    logger.info("Command RAW output of adm top pods")
    logger.info(f"{resource_utilization_all_pods}")
    return resource_utilization_all_pods


def get_mon_label(mon_pod_obj):
    """
    Gets the mon pod label

    Args:
        mon_pod_obj (Pod): The pod object

    Returns:
        str: The mon pod label (eg: a)

    """
    return mon_pod_obj.get().get("metadata").get("labels").get("mon")


def set_osd_maintenance_mode(osd_deployment):
    """
    Set osd in maintenance mode for running ceph-objectstore commands

    Args:
        osd_deployment (OCS): List of OSD deployment OCS instances

    """
    logger.info("Entering into the OSD maintenance mode")
    # Scale down rook-ceph-operator and ocs-operator deployments to zero
    helpers.modify_deployment_replica_count(
        deployment_name=constants.ROOK_CEPH_OPERATOR, replica_count=0
    )
    helpers.modify_deployment_replica_count(
        deployment_name=constants.OCS_CSV_PREFIX, replica_count=0
    )
    # Set ceph osd noout and pause flags
    ct_pod = get_ceph_tools_pod()
    logger.info("Setting osd noout flag")
    ct_pod.exec_ceph_cmd("ceph osd set noout")
    logger.info("Setting osd pause flag")
    ct_pod.exec_ceph_cmd("ceph osd set pause")
    # Take a backup of the list of osd deployments
    logger.info("Backup OSD deployments")
    for deployment in osd_deployment:
        deployment_get = deployment.get()
        templating.dump_data_to_temp_yaml(
            deployment_get, f"backup_{deployment.name}.yaml"
        )
        patch_cmd = (
            '{"spec": {"template": {"spec":{"containers": [{"name": "osd", '
            '"command": ["sleep", "infinity"], "args":[]}]}}}}'
        )
        deployment.ocp.patch(resource_name=deployment.name, params=patch_cmd)
    # Sleep for 60 sec for the OSD pods to respin
    logger.info("Sleeping for 60s for the osd pods to stabilize")
    time.sleep(60)
    for pod in get_osd_pods():
        helpers.wait_for_resource_state(resource=pod, state=constants.STATUS_RUNNING)


def exit_osd_maintenance_mode(osd_deployment):
    """
    Exit from osd maintenance mode

    Args:
        osd_deployment (OCS): List of OSD deployment OCS instances

    """
    logger.info("Exiting the OSD from maintenance mode")
    # Replace OSD deployment with the backup took while setting the osd in maintenance mode
    for deployment in osd_deployment:
        if os.path.isfile(f"backup_{deployment.name}.yaml"):
            logger.info(f"Replacing the backup for {deployment.name}")
            cmd = f"oc replace --force -f backup_{deployment.name}.yaml"
            run_cmd(cmd=cmd)
    # Scale up rook-ceph-operator and ocs-operator deployments to 1
    helpers.modify_deployment_replica_count(
        deployment_name=constants.ROOK_CEPH_OPERATOR, replica_count=1
    )
    helpers.modify_deployment_replica_count(
        deployment_name=constants.OCS_CSV_PREFIX, replica_count=1
    )
    # Sleep for 60 sec for the OSD pods to respin
    logger.info("Sleeping for 60s for the osd pods to stabilize")
    time.sleep(60)
    for pod in get_osd_pods():
        helpers.wait_for_resource_state(resource=pod, state=constants.STATUS_RUNNING)
    ct_pod = get_ceph_tools_pod()
    # UnSet ceph osd noout and pause flags
    logger.info("UnSet osd noout flag")
    ct_pod.exec_ceph_cmd("ceph osd unset noout")
    logger.info("UnSet osd pause flag")
    ct_pod.exec_ceph_cmd("ceph osd unset pause")
    # Remove the backup files
    for deployment in osd_deployment:
        if os.path.isfile(f"backup_{deployment.name}.yaml"):
            os.remove(f"backup_{deployment.name}.yaml")


def restart_pods_having_label(label, namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Restart the pods having particular label

    Args:
        label (str): Label of the pod
        namespace (str): namespace where the pods are running

    """
    pods_to_restart = [
        Pod(**pod_data)
        for pod_data in get_pods_having_label(label, namespace=namespace)
    ]
    delete_pods(pods_to_restart, wait=True)
    logger.info(f"Deleted all the pods with label {label} and in namespace {namespace}")


def restart_pods_in_statuses(
    status_options, namespace=config.ENV_DATA["cluster_namespace"], wait=True
):
    """
    Restart all the pods in specific statuses

    Args:
        status_options (list): The list of the status options.
        namespace (str): Name of cluster namespace(default: config.ENV_DATA["cluster_namespace"])
        wait (bool): Determines if the delete command should wait for
            completion

    Returns:
        list: Restart all the pods that their status in the 'status_options' list.

    """
    logger.info(f"Get the pods in the statuses: {status_options}")
    pods_to_restart = get_pods_in_statuses(status_options, namespace)
    logger.info(
        f"The pods {pods_to_restart} are in the statuses {status_options}. Restarting the pods..."
    )
    delete_pods(pods_to_restart, wait=wait)
    logger.info("Finish restarting the pods")


def wait_for_ceph_cmd_execute_successfully(timeout=300):
    """
    Wait for a Ceph command to execute successfully

    Args:
        timeout (int): The time to wait for a Ceph command to execute successfully

    Returns:
        bool: True, if the Ceph command executed successfully. False, otherwise

    """
    try:
        for res in TimeoutSampler(
            timeout=timeout, sleep=10, func=check_ceph_cmd_execute_successfully
        ):
            if res:
                return True
    except TimeoutExpiredError:
        logger.warning(f"Failed to execute the ceph command after {timeout} seconds")
        return False


def check_ceph_cmd_execute_successfully():
    """
    Check that a Ceph command executes successfully

    Returns:
        bool: True, if the Ceph command executed successfully. False, otherwise

    """
    # Import here to avoid circular loop
    from ocs_ci.ocs.cluster import is_ms_consumer_cluster
    from ocs_ci.ocs.managedservice import patch_consumer_toolbox

    try:
        tool_pod = get_ceph_tools_pod()
        res = tool_pod.exec_cmd_on_pod("ceph -s --format json-pretty", timeout=60)
        if res:
            logger.info("The Ceph command executed successfully")
            return True
    except CommandFailed as ex:
        if "RADOS permission error" in str(ex) and is_ms_consumer_cluster():
            logger.info("Patch the consumer rook-ceph-tools deployment")
            patch_consumer_toolbox()

        logger.warning(f"Failed to execute the ceph command due to the error {str(ex)}")
        return False


def search_pattern_in_pod_logs(
    pod_name,
    pattern,
    namespace=config.ENV_DATA["cluster_namespace"],
    container=None,
    all_containers=False,
):
    """
    Searches for the given regular expression pattern in the logs of a pod and returns all matching lines.

    Args:
        pod_name (str): The name of the pod.
        pattern (str): The regular expression pattern to search for.
        namespace (str, optional): The namespace of the pod. Defaults to None.
        container (str, optional): The name of the container to search logs for. Defaults to None.
        all_containers (bool, optional): Whether to search logs for all containers in the pod. Defaults to False.

    Returns:
        A list of matched lines with the pattern.
    """
    pod_logs = get_pod_logs(
        pod_name=pod_name,
        namespace=namespace,
        container=container,
        all_containers=all_containers,
    )

    matched_lines = [line for line in pod_logs.split("\n") if re.search(pattern, line)]

    return matched_lines


def get_containers_names_by_pod(pod: OCP) -> set:
    """
    Gets the names of all containers in given pod or pods

    Args:
        pod (ocp.OCP): instance of OCP object that represents a pod (kind=POD)

    Returns:
        set: hash set of names of all containers in given pod or pods

    """
    items = pod.data.get("items")
    if not isinstance(items, list):
        items = [items]

    container_names = list()
    for item in items:
        containers = item.get("spec").get("containers")
        container_names += [c.get("name") for c in containers]

    logger.debug(f"Containers: {container_names}")

    return set(container_names)


def get_ceph_daemon_id(pod_obj):
    """
    Get Ceph Daemon ID of osd, mds, mon, rgw, mgr

    Args:
       pod_obj (POD Obj): pod object

    Returns:
        str: ceph_daemon_id
    """
    return pod_obj.get("labels").get("metadata").get("labels").get("ceph_daemon_id")


def get_mon_pod_by_pvc_name(pvc_name: str):
    """
    Function to get monitor pod by pvc_name label

    Args:
        pvc_name (str): name of the pvc the monitor pod is related to
    """
    mon_pod_ocp = (
        ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=f"pvc_name={pvc_name}",
        )
        .get()
        .get("items")[0]
    )
    return Pod(**mon_pod_ocp)


def get_debug_pods(debug_nodes, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
    """
    Get debug pods created for the nodes in debug

    Args:
        debug_nodes (list): List of nodes in debug mode
        namespace (str): By default 'openshift-storage' namespace

    Returns:
        List: of Pod objects

    """
    debug_pods = []
    for node_name in debug_nodes:
        debug_pods.extend(
            get_pod_obj(pod, namespace=namespace)
            for pod in get_pod_name_by_pattern(
                f"{node_name}-debug", namespace=namespace
            )
        )

    return debug_pods


def wait_for_pods_deletion(
    label, timeout=120, sleep=5, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
):
    """
    Wait for the pods with particular label to be deleted
    until the given timeout

    Args:
        label (str): Pod label
        timeout (int): Timeout
        namespace (str): Namespace in which pods are running

    Raises:
        TimeoutExpiredError

    """

    def _check_if_pod_deleted(label, namespace):
        return len(get_pods_having_label(label, namespace)) == 0

    sampler = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=_check_if_pod_deleted,
        label=label,
        namespace=namespace,
    )
    sampler.wait_for_func_status(True)


def calculate_md5sum_of_pod_files(pods_for_integrity_check, pod_file_name):
    """
    Calculate the md5sum of the pod files, and save it in the pod objects

    Args:
        pods_for_integrity_check (list): The list of the pod objects to calculate the md5sum
        pod_file_name (str): The pod file name to save the md5sum

    """
    # Wait for IO to finish
    logger.info("Wait for IO to finish on pods")
    for pod_obj in pods_for_integrity_check:
        pod_obj.get_fio_results()
        logger.info(f"IO finished on pod {pod_obj.name}")
        # Calculate md5sum
        pod_file_name = (
            pod_file_name
            if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
            else pod_obj.get_storage_path(storage_type="block")
        )
        logger.info(
            f"Calculate the md5sum of the file {pod_file_name} in the pod {pod_obj.name}"
        )
        pod_obj.pvc.md5sum = cal_md5sum(
            pod_obj,
            pod_file_name,
            pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
        )


def verify_md5sum_on_pod_files(pods_for_integrity_check, pod_file_name):
    """
    Verify the md5sum of the pod files

    Args:
        pods_for_integrity_check (list): The list of the pod objects to verify the md5sum
        pod_file_name (str): The pod file name to verify its md5sum

    Raises:
        AssertionError: If file doesn't exist or md5sum mismatch

    """
    for pod_obj in pods_for_integrity_check:
        pod_file_name = (
            pod_obj.get_storage_path(storage_type="block")
            if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
            else pod_file_name
        )
        verify_data_integrity(
            pod_obj,
            pod_file_name,
            pod_obj.pvc.md5sum,
            pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
        )
        logger.info(
            f"Verified: md5sum of {pod_file_name} on pod {pod_obj.name} "
            f"matches with the original md5sum"
        )
    logger.info("Data integrity check passed on all pods")


def fetch_rgw_pod_restart_count(namespace=config.ENV_DATA["cluster_namespace"]):
    """
    This method fetches the restart count of rgw pod

    Arg:
        namespace(str): namespace where rgw pd is running. default value is,
        config.ENV_DATA["cluster_namespace"]

    Return:
        rgw_pod_restart_count: restart count for rgw pod

    """
    list_of_rgw_pods = get_rgw_pods(namespace=namespace)
    rgw_pod_obj = list_of_rgw_pods[0]
    restart_count_for_rgw_pod = get_pod_restarts_count(
        list_of_pods=list_of_rgw_pods,
        namespace=namespace,
    )
    rgw_pod_restart_count = restart_count_for_rgw_pod[rgw_pod_obj.name]
    logger.info(f"restart count for rgw pod is: {rgw_pod_restart_count}")
    return rgw_pod_restart_count
