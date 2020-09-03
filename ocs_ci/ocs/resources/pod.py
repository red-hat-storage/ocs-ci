"""
Pod related functionalities and context info

Each pod in the openshift cluster will have a corresponding pod object
"""
import logging
import os
import re
import yaml
import tempfile
import time
import calendar
from threading import Thread
import base64

from ocs_ci.ocs.ocp import OCP, verify_images_upgraded
from tests import helpers
from ocs_ci.ocs import constants, defaults, node, workload, ocp
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    CommandFailed, NonUpgradedImagesFoundError, ResourceWrongStatusException,
    TimeoutExpiredError, UnavailableResourceException
)
from ocs_ci.ocs.utils import setup_ceph_toolbox
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, check_timeout_reached, TimeoutSampler
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
TEST_FILE = '/var/lib/www/html/test'
FEDORA_TEST_FILE = '/mnt/test'


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
        super(Pod, self).__init__(**kwargs)

        with tempfile.NamedTemporaryFile(
            mode='w+', prefix='POD_', delete=False
        ) as temp_info:
            self.temp_yaml = temp_info.name
        self._name = self.pod_data.get('metadata').get('name')
        self._labels = self.get_labels()
        self._roles = []
        self.ocp = OCP(
            api_version=defaults.API_VERSION, kind=constants.POD,
            namespace=self.namespace
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
        return self.get().get('status').get('containerStatuses')[0].get('restartCount')

    def __setattr__(self, key, val):
        self.__dict__[key] = val

    def add_role(self, role):
        """
        Adds a new role for this pod

        Args:
            role (str): New role to be assigned for this pod
        """
        self._roles.append(role)

    def get_fio_results(self):
        """
        Get FIO execution results

        Returns:
            dict: Dictionary represents the FIO execution results

        Raises:
            Exception: In case of exception from FIO
        """
        logger.info(f"Waiting for FIO results from pod {self.name}")
        try:
            result = self.fio_thread.result(FIO_TIMEOUT)
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
        self, command, out_yaml_format=True, secrets=None, timeout=600, **kwargs
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

        Returns:
            Munch Obj: This object represents a returned yaml file
        """
        rsh_cmd = f"rsh {self.name} "
        rsh_cmd += command
        return self.ocp.exec_oc_cmd(
            rsh_cmd, out_yaml_format, secrets=secrets, timeout=timeout, **kwargs
        )

    def exec_sh_cmd_on_pod(self, command, sh="bash"):
        """
        Execute a pure bash command on a pod via oc exec where you can use
        bash syntaxt like &&, ||, ;, for loop and so on.

        Args:
            command (str): The command to execute on the given pod

        Returns:
            str: stdout of the command
        """
        cmd = f'exec {self.name} -- {sh} -c "{command}"'
        return self.ocp.exec_oc_cmd(cmd, out_yaml_format=False)

    def get_labels(self):
        """
        Get labels from pod

        Raises:
            NotFoundError: If resource not found

        Returns:
            dict: All the openshift labels on a given pod
        """
        return self.pod_data.get('metadata').get('labels')

    def exec_ceph_cmd(self, ceph_cmd, format='json-pretty'):
        """
        Execute a Ceph command on the Ceph tools pod

        Args:
            ceph_cmd (str): The Ceph command to execute on the Ceph tools pod
            format (str): The returning output format of the Ceph command

        Returns:
            dict: Ceph command output

        Raises:
            CommandFailed: In case the pod is not a toolbox pod
        """
        if 'rook-ceph-tools' not in self.labels.values():
            raise CommandFailed(
                "Ceph commands can be executed only on toolbox pod"
            )
        ceph_cmd = ceph_cmd
        if format:
            ceph_cmd += f" --format {format}"
        out = self.exec_cmd_on_pod(ceph_cmd)

        # For some commands, like "ceph fs ls", the returned output is a list
        if isinstance(out, list):
            return [item for item in out if item]
        return out

    def get_storage_path(self, storage_type='fs'):
        """
        Get the pod volume mount path or device path

        Returns:
            str: The mount path of the volume on the pod (e.g. /var/lib/www/html/) if storage_type is fs
                 else device path of raw block pv
        """
        # TODO: Allow returning a path of a specified volume of a specified
        #  container
        if storage_type == 'block':
            return self.pod_data.get('spec').get('containers')[0].get(
                'volumeDevices')[0].get('devicePath')

        return (
            self.pod_data.get(
                'spec'
            ).get('containers')[0].get('volumeMounts')[0].get('mountPath')
        )

    def workload_setup(self, storage_type, jobs=1):
        """
        Do setup on pod for running FIO

        Args:
            storage_type (str): 'fs' or 'block'
            jobs (int): Number of jobs to execute FIO
        """
        work_load = 'fio'
        name = f'test_workload_{work_load}'
        path = self.get_storage_path(storage_type)
        # few io parameters for Fio

        self.wl_obj = workload.WorkLoad(
            name, path, work_load, storage_type, self, jobs
        )
        assert self.wl_obj.setup(), f"Setup for FIO failed on pod {self.name}"
        self.wl_setup_done = True

    def run_io(
        self, storage_type, size, io_direction='rw', rw_ratio=75,
        jobs=1, runtime=60, depth=4, rate='1m', rate_process='poisson', fio_filename=None, bs='4K'
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
        """
        if not self.wl_setup_done:
            self.workload_setup(storage_type=storage_type, jobs=jobs)

        if io_direction == 'rw':
            self.io_params = templating.load_yaml(
                constants.FIO_IO_RW_PARAMS_YAML
            )
            self.io_params['rwmixread'] = rw_ratio
        else:
            self.io_params = templating.load_yaml(
                constants.FIO_IO_PARAMS_YAML
            )
        self.io_params['runtime'] = runtime
        size = size if isinstance(size, str) else f"{size}G"
        self.io_params['size'] = size
        if fio_filename:
            self.io_params['filename'] = fio_filename
        self.io_params['iodepth'] = depth
        self.io_params['rate'] = rate
        self.io_params['rate_process'] = rate_process
        self.io_params['bs'] = bs
        self.fio_thread = self.wl_obj.run(**self.io_params)

    def run_git_clone(self):
        """
        Execute git clone on a pod to simulate a Jenkins user
        """
        name = 'test_workload'
        work_load = 'jenkins'

        wl = workload.WorkLoad(
            name=name,
            work_load=work_load,
            pod=self,
            path=self.get_storage_path()
        )
        assert wl.setup(), "Setup up for git failed"
        wl.run()

    def install_packages(self, packages):
        """
        Install packages in a Pod

        Args:
            packages (list): List of packages to install

        """
        if isinstance(packages, list):
            packages = ' '.join(packages)

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
            f"scp -i {authkey} -o \"StrictHostKeyChecking no\""
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

        cmd = f"ssh -i {authkey} -o \"StrictHostKeyChecking no\" {user}@{server} {cmd}"
        self.exec_cmd_on_pod(cmd, out_yaml_format=False)

    def get_memory(self):
        """
        Get the pod memory size

        Returns:
            dict: The names of the pod's containers (str) as keys and their memory
                size (str) as values

        """
        containers = self.pod_data.get('spec').get('containers')
        container_names_and_memory = {
            container.get('name'): container.get('resources')
            .get('limits').get('memory') for container in containers
        }
        return container_names_and_memory


# Helper functions for Pods

def get_all_pods(
        namespace=None, selector=None, selector_label='app',
        exclude_selector=False, wait=False
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

    Returns:
        list: List of Pod objects

    """
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    # In case of >4 worker nodes node failures automatic failover of pods to
    # other nodes will happen.
    # So, we are waiting for the pods to come up on new node
    if wait:
        wait_time = 180
        logger.info(f"Waiting for {wait_time}s for the pods to stabilize")
        time.sleep(wait_time)
    pods = ocp_pod_obj.get()['items']
    if selector:
        if exclude_selector:
            pods_new = [
                pod for pod in pods if
                pod['metadata'].get(
                    'labels', {}
                ).get(selector_label) not in selector
            ]
        else:
            pods_new = [
                pod for pod in pods if
                pod['metadata'].get(
                    'labels', {}
                ).get(selector_label) in selector
            ]
        pods = pods_new
    pod_objs = [Pod(**pod) for pod in pods]
    return pod_objs


def get_ceph_tools_pod():
    """
    Get the Ceph tools pod

    Returns:
        Pod object: The Ceph tools pod object
    """
    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
    )
    ct_pod_items = ocp_pod_obj.get(
        selector='app=rook-ceph-tools'
    )['items']
    if not ct_pod_items:
        # setup ceph_toolbox pod if the cluster has been setup by some other CI
        setup_ceph_toolbox()
        ct_pod_items = ocp_pod_obj.get(
            selector='app=rook-ceph-tools'
        )['items']

    assert ct_pod_items, "No Ceph tools pod found"

    # In the case of node failure, the CT pod will be recreated with the old
    # one in status Terminated. Therefore, need to filter out the Terminated pod
    running_ct_pods = list()
    for pod in ct_pod_items:
        if ocp_pod_obj.get_resource_status(
            pod.get('metadata').get('name')
        ) == constants.STATUS_RUNNING:
            running_ct_pods.append(pod)

    assert running_ct_pods, "No running Ceph tools pod found"
    ceph_pod = Pod(**running_ct_pods[0])
    return ceph_pod


def get_csi_provisioner_pod(interface):
    """
    Get the provisioner pod based on interface
    Returns:
        Pod object: The provisioner pod object based on iterface
    """
    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
    )
    selector = 'app=csi-rbdplugin-provisioner' if (
        interface == constants.CEPHBLOCKPOOL
    ) else 'app=csi-cephfsplugin-provisioner'
    provision_pod_items = ocp_pod_obj.get(
        selector=selector
    )['items']
    assert provision_pod_items, f"No {interface} provisioner pod found"
    provisioner_pod = (
        Pod(**provision_pod_items[0]).name,
        Pod(**provision_pod_items[1]).name
    )
    return provisioner_pod


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
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    ocs_operator = get_pods_having_label(ocs_label, namespace)
    ocs_operator_pod = Pod(**ocs_operator[0])
    return ocs_operator_pod


def list_ceph_images(pool_name='rbd'):
    """
    Args:
        pool_name (str): Name of the pool to get the ceph images

    Returns (List): List of RBD images in the pool
    """
    ct_pod = get_ceph_tools_pod()
    return ct_pod.exec_ceph_cmd(ceph_cmd=f"rbd ls {pool_name}", format='json')


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
    ret = pod_obj.exec_cmd_on_pod(f"bash -c \"find {file_path}\"")
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
        pod_obj.get().get('spec').get('containers')[0].get(
            'volumeMounts')[0].get('mountPath')
    )
    file_path = os.path.join(path, file_name)
    return file_path


def cal_md5sum(pod_obj, file_name):
    """
    Calculates the md5sum of the file

    Args:
        pod_obj (Pod): The object of the pod
        file_name (str): The name of the file for which md5sum to be calculated

    Returns:
        str: The md5sum of the file
    """
    file_path = get_file_path(pod_obj, file_name)
    md5sum_cmd_out = pod_obj.exec_cmd_on_pod(
        command=f"bash -c \"md5sum {file_path}\"", out_yaml_format=False
    )
    md5sum = md5sum_cmd_out.split()[0]
    logger.info(f"md5sum of file {file_name}: {md5sum}")
    return md5sum


def verify_data_integrity(pod_obj, file_name, original_md5sum):
    """
    Verifies existence and md5sum of file created from first pod

    Args:
        pod_obj (Pod): The object of the pod
        file_name (str): The name of the file for which md5sum to be calculated
        original_md5sum (str): The original md5sum of the file

    Returns:
        bool: True if the file exists and md5sum matches

    Raises:
        AssertionError: If file doesn't exist or md5sum mismatch
    """
    file_path = get_file_path(pod_obj, file_name)
    assert check_file_existence(pod_obj, file_path), (
        f"File {file_name} doesn't exists"
    )
    current_md5sum = cal_md5sum(pod_obj, file_name)
    logger.info(f"Original md5sum of file: {original_md5sum}")
    logger.info(f"Current md5sum of file: {current_md5sum}")
    assert current_md5sum == original_md5sum, (
        'Data corruption found'
    )
    logger.info(f"File {file_name} exists and md5sum matches")
    return True


def get_fio_rw_iops(pod_obj):
    """
    Execute FIO on a pod

    Args:
        pod_obj (Pod): The object of the pod
    """
    fio_result = pod_obj.get_fio_results()
    logging.info(f"FIO output: {fio_result}")
    logging.info("IOPs after FIO:")
    logging.info(
        f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
    )
    logging.info(
        f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
    )


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
                command=f"bash -c \"let i=0; while true; do echo "
                f"{TEXT_CONTENT} >> {FILE}$i; let i++; sleep 0.01; done\"",
                timeout=2400
            )
        # Once the pod gets deleted, the I/O execution will get terminated.
        # Hence, catching this exception
        except CommandFailed as ex:
            if expect_to_fail:
                if re.search("code 137", str(ex)) or (
                    re.search("code 143", str(ex))
                ):
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
            timeout=20, sleep=1, func=check_file_existence,
            pod_obj=pod_obj, file_path=test_file
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
            command=f'ls -l {os.path.abspath(parent_dir)}',
            out_yaml_format=False
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
    out = tools_pod.exec_ceph_cmd(ceph_cmd='ceph auth get-key client.admin')
    base64_output = base64.b64encode(out['key'].encode()).decode()
    return base64_output


def run_io_and_verify_mount_point(pod_obj, bs='10M', count='950'):
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
    used_percentage = mount_point[mount_point.index('/var/lib/www/html') - 1]
    return used_percentage


def get_pods_having_label(label, namespace):
    """
    Fetches pod resources with given label in given namespace

    Args:
        label (str): label which pods might have
        namespace (str): Namespace in which to be looked up

    Return:
        list: of pods info

    """
    ocp_pod = OCP(kind=constants.POD, namespace=namespace)
    pods = ocp_pod.get(selector=label).get('items')
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
    pods = ocp_deployment.get(selector=label).get('items')
    return pods


def get_mds_pods(mds_label=constants.MDS_APP_LABEL, namespace=None):
    """
    Fetches info about mds pods in the cluster

    Args:
        mds_label (str): label associated with mds pods
            (default: defaults.MDS_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : of mds pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : of mon pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : of mgr pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : of osd pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    osds = get_pods_having_label(osd_label, namespace)
    osd_pods = [Pod(**osd) for osd in osds]
    return osd_pods


def get_osd_prepare_pods(
    osd_prepare_label=constants.OSD_PREPARE_APP_LABEL, namespace=defaults.ROOK_CLUSTER_NAMESPACE
):
    """
    Fetches info about osd prepare pods in the cluster

    Args:
        osd_prepare_label (str): label associated with osd prepare pods
            (default: constants.OSD_PREPARE_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list: OSD prepare pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list: OSD deployment OCS instances
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    osds = get_deployments_having_label(osd_label, namespace)
    osd_deployments = [OCS(**osd) for osd in osds]
    return osd_deployments


def get_pod_count(label, namespace=None):
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    pods = get_pods_having_label(label=label, namespace=namespace)
    return len(pods)


def get_cephfsplugin_provisioner_pods(
    cephfsplugin_provisioner_label=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
    namespace=None
):
    """
    Fetches info about CSI Cephfs plugin provisioner pods in the cluster

    Args:
        cephfsplugin_provisioner_label (str): label associated with cephfs
            provisioner pods
            (default: defaults.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : csi-cephfsplugin-provisioner Pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    pods = get_pods_having_label(cephfsplugin_provisioner_label, namespace)
    fs_plugin_pods = [Pod(**pod) for pod in pods]
    return fs_plugin_pods


def get_rbdfsplugin_provisioner_pods(
    rbdplugin_provisioner_label=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
    namespace=None
):
    """
    Fetches info about CSI Cephfs plugin provisioner pods in the cluster

    Args:
        rbdplugin_provisioner_label (str): label associated with RBD
            provisioner pods
            (default: defaults.CSI_RBDPLUGIN_PROVISIONER_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : csi-rbdplugin-provisioner Pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
    ocp_obj = OCP(api_version='v1', kind=constants.POD, namespace=namespace)
    ocp_dict = ocp_obj.get(resource_name=name)
    pod_obj = Pod(**ocp_dict)
    return pod_obj


def get_pod_logs(pod_name, container=None, namespace=defaults.ROOK_CLUSTER_NAMESPACE, previous=False):
    """
    Get logs from a given pod

    pod_name (str): Name of the pod
    container (str): Name of the container
    namespace (str): Namespace of the pod
    previous (bool): True, if pod previous log required. False otherwise.

    Returns:
        str: Output from 'oc get logs <pod_name> command
    """
    pod = OCP(
        kind=constants.POD, namespace=namespace
    )
    cmd = f"logs {pod_name}"
    if container:
        cmd += f" -c {container}"
    if previous:
        cmd += " --previous"
    return pod.exec_oc_cmd(cmd, out_yaml_format=False)


def get_pod_node(pod_obj):
    """
    Get the node that the pod is running on

    Args:
        pod_obj (OCS): The pod object

    Returns:
        ocs_ci.ocs.ocp.OCP: The node object

    """
    node_name = pod_obj.get().get('spec').get('nodeName')
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
        start_time = pod_obj['status']['startTime']
        ts = time.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        ts = calendar.timegm(ts)
        current_time_utc = time.time()
        sec = current_time_utc - ts
        if (sec / 3600) >= 1:
            logger.error(
                f'Pod {pod.name} is not respinned, the age of the pod is {start_time}'
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
        f"Checking whether the pod {pod_obj.name} is running on "
        f"node {node_name}"
    )
    actual_node = pod_obj.get().get('spec').get('nodeName')
    if actual_node == node_name:
        logger.info(
            f"The pod {pod_obj.name} is running on the specified node "
            f"{actual_node}"
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
    pvc = pod_obj.get().get('spec').get('volumes')[0].get('persistentVolumeClaim')
    if not pvc:
        raise UnavailableResourceException
    return pvc.get('claimName')


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
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    plugins_info = get_pods_having_label(plugin_label, namespace)
    plugin_pods = [Pod(**plugin) for plugin in plugins_info]
    return plugin_pods


def plugin_provisioner_leader(interface, namespace=None):
    """
    Find csi-cephfsplugin-provisioner or csi-rbdplugin-provisioner leader pod

    Args:
        interface (str): Interface type. eg: CephBlockPool, CephFileSystem
        namespace (str): Name of cluster namespace

    Returns:
        Pod: csi-cephfsplugin-provisioner or csi-rbdplugin-provisioner leader
            pod

    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    if interface == constants.CEPHBLOCKPOOL:
        lease_cmd = "get leases openshift-storage-rbd-csi-ceph-com -o yaml"
    if interface == constants.CEPHFILESYSTEM:
        lease_cmd = "get leases openshift-storage-cephfs-csi-ceph-com -o yaml"

    ocp_obj = ocp.OCP(kind=constants.POD, namespace=namespace)
    lease = ocp_obj.exec_oc_cmd(command=lease_cmd)
    leader = lease.get('spec').get('holderIdentity').strip()
    assert leader, "Couldn't identify plugin provisioner leader pod."
    logger.info(f"Plugin provisioner leader pod is {leader}")

    ocp_obj._resource_name = leader
    leader_pod = Pod(**ocp_obj.get())
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
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
    cmd = f"oc -n {namespace} cp {os.path.expanduser(localpath)} {pod_name}:{remotepath}"
    run_cmd(cmd)


def download_file_from_pod(pod_name, remotepath, localpath, namespace=None):
    """
    Download a file from a pod

    Args:
        pod_name (str): Name of the pod
        remotepath (str): Target path on the pod
        localpath (str): Local file to upload
        namespace (str): The namespace of the pod

    """
    namespace = namespace or constants.DEFAULT_NAMESPACE
    cmd = f"oc -n {namespace} cp {pod_name}:{remotepath} {os.path.expanduser(localpath)}"
    run_cmd(cmd)


def wait_for_storage_pods(timeout=200):
    """
    Check all OCS pods status, they should be in Running or Completed state

    Args:
        timeout (int): Number of seconds to wait for pods to get into correct
            state

    """
    all_pod_obj = get_all_pods(
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    for pod_obj in all_pod_obj:
        state = constants.STATUS_RUNNING
        if any(i in pod_obj.name for i in ['-1-deploy', 'ocs-deviceset']):
            state = constants.STATUS_COMPLETED

        try:
            helpers.wait_for_resource_state(
                resource=pod_obj,
                state=state,
                timeout=timeout
            )
        except ResourceWrongStatusException:
            # 'rook-ceph-crashcollector' on the failed node stucks at
            # pending state. BZ 1810014 tracks it.
            # Ignoring 'rook-ceph-crashcollector' pod health check as
            # WA and deleting its deployment so that the pod
            # disappears. Will revert this WA once the BZ is fixed
            if 'rook-ceph-crashcollector' in pod_obj.name:
                ocp_obj = ocp.OCP(
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE
                )
                pod_name = pod_obj.name
                deployment_name = '-'.join(pod_name.split("-")[:-2])
                command = f"delete deployment {deployment_name}"
                ocp_obj.exec_oc_cmd(command=command)
                logger.info(f"Deleted deployment for pod {pod_obj.name}")
            else:
                raise


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

    namespace = config.ENV_DATA['cluster_namespace']
    pod = OCP(
        kind=constants.POD, namespace=namespace,
    )
    info_message = (
        f"Waiting for {count} pods with selector: {selector} to be running "
        f"and upgraded."
    )
    logger.info(info_message)
    start_time = time.time()
    selector_label, selector_value = selector.split('=')
    while True:
        pod_count = 0
        try:
            pods = get_all_pods(namespace, [selector_value], selector_label)
            pods_len = len(pods)
            logger.info(f"Found {pods_len} pod(s) for selector: {selector}")
            if pods_len != count:
                logger.warning(
                    f"Number of found pods {pods_len} is not as expected: "
                    f"{count}"
                )
            for pod in pods:
                verify_images_upgraded(old_images, pod.get())
                pod_count += 1
        except CommandFailed as ex:
            logger.warning(
                f"Failed when getting pods with selector {selector}."
                f"Error: {ex}"
            )
        except NonUpgradedImagesFoundError as ex:
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
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list : of noobaa pod objects
    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
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
        name = pod_obj.get_labels().get('name')
        dpod_list = get_all_pods(selector_label=f"name={name}", wait=True)
        for dpod in dpod_list:
            if '-1-deploy' not in dpod.name and dpod.status != exclude_state:
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
    pod_data_list = dc_ocp_obj.get().get('items')
    if pod_data_list:
        for pod_data in pod_data_list:
            if pod_obj.get_labels().get('name') == pod_data.get('metadata').get('name'):
                dc_ocp_obj.delete(resource_name=pod_obj.get_labels().get('name'))
                dc_ocp_obj.wait_for_delete(
                    resource_name=pod_obj.get_labels().get('name')
                )


def wait_for_new_osd_pods_to_come_up(number_of_osd_pods_before):
    status_options = ['Init:1/4', 'Init:2/4', 'Init:3/4', 'PodInitializing', 'Running']
    try:
        for osd_pods in TimeoutSampler(
            timeout=180, sleep=3, func=get_osd_pods
        ):
            # Check if the new osd pods has started to come up
            new_osd_pods = osd_pods[number_of_osd_pods_before:]
            new_osd_pods_come_up = [pod.status() in status_options for pod in new_osd_pods]
            if any(new_osd_pods_come_up):
                logging.info("One or more of the new osd pods has started to come up")
                break
    except TimeoutExpiredError:
        logging.warning(
            "None of the new osd pods reached the desired status"
        )


def get_pod_restarts_count(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Gets the dictionary of pod and its restart count for all the pods in a given namespace

    Returns:
        dict: dictionary of pod name and its corresponding restart count

    """
    list_of_pods = get_all_pods(namespace)
    restart_dict = {}
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # we don't want to compare osd-prepare and canary pods as they get created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name and "rook-ceph-drain-canary" not in p.name:
            restart_dict[p.name] = int(ocp_pod_obj.get_resource(p.name, 'RESTARTS'))
    logging.info(f"get_pod_restarts_count: restarts dict = {restart_dict}")
    return restart_dict


def check_pods_in_running_state(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    checks whether all the pods in a given namespace are in Running state or not

    Returns:
        Boolean: True, if all pods in Running state. False, otherwise

    """
    ret_val = True
    list_of_pods = get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # we don't want to compare osd-prepare and canary pods as they get created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name and "rook-ceph-drain-canary" not in p.name:
            status = ocp_pod_obj.get_resource(p.name, 'STATUS')
            if status not in "Running":
                logging.error(f"The pod {p.name} is in {status} state. Expected = Running")
                ret_val = False
    return ret_val


def get_running_state_pods(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Checks the running state pods in a given namespace.

        Returns:
            List: all the pod objects that are in running state only

    """
    list_of_pods = get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    running_pods_object = list()
    for pod in list_of_pods:
        status = ocp_pod_obj.get_resource(pod.name, 'STATUS')
        if "Running" in status:
            running_pods_object.append(pod)

    return running_pods_object
