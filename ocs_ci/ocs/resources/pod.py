"""
Pod related functionalities and context info

Each pod in the openshift cluster will have a corresponding pod object
"""
import logging
import os
import re
import yaml
import tempfile
from time import sleep
from threading import Thread
import base64

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import workload
from ocs_ci.ocs import constants, defaults, node
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

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

        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='POD_', delete=False
        )
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
        try:
            if self.fio_thread and self.fio_thread.done():
                return yaml.safe_load(self.fio_thread.result())
            elif self.fio_thread.running():
                for sample in TimeoutSampler(
                    timeout=FIO_TIMEOUT, sleep=3, func=self.fio_thread.done
                ):
                    if sample:
                        return yaml.safe_load(self.fio_thread.result())

        except CommandFailed as ex:
            logger.exception(f"FIO failed: {ex}")
            raise
        except Exception as ex:
            logger.exception(f"Found Exception: {ex}")
            raise

    def exec_cmd_on_pod(self, command, out_yaml_format=True, secrets=None, **kwargs):
        """
        Execute a command on a pod (e.g. oc rsh)

        Args:
            command (str): The command to execute on the given pod
            out_yaml_format (bool): whether to return yaml loaded python
                object OR to return raw output

            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(**kwargs)

        Returns:
            Munch Obj: This object represents a returned yaml file
        """
        rsh_cmd = f"rsh {self.name} "
        rsh_cmd += command
        return self.ocp.exec_oc_cmd(rsh_cmd, out_yaml_format, secrets=secrets, **kwargs)

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

    def get_mount_path(self):
        """
        Get the pod volume mount path

        Returns:
            str: The mount path of the volume on the pod (e.g. /var/lib/www/html/)
        """
        # TODO: Allow returning a path of a specified volume of a specified
        #  container
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
        path = self.get_mount_path()
        # few io parameters for Fio

        self.wl_obj = workload.WorkLoad(
            name, path, work_load, storage_type, self, jobs
        )
        assert self.wl_obj.setup(), f"Setup for FIO failed on pod {self.name}"
        self.wl_setup_done = True

    def run_io(
        self, storage_type, size, io_direction='rw', rw_ratio=75,
        jobs=1, runtime=60, depth=4, fio_filename=None
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
            fio_filename(str): Name of fio file created on app pod's mount point
        """
        if not self.wl_setup_done:
            self.workload_setup(storage_type=storage_type, jobs=jobs)

        if io_direction == 'rw':
            self.io_params = templating.load_yaml_to_dict(
                constants.FIO_IO_RW_PARAMS_YAML
            )
            self.io_params['rwmixread'] = rw_ratio
        else:
            self.io_params = templating.load_yaml_to_dict(
                constants.FIO_IO_PARAMS_YAML
            )
        self.io_params['runtime'] = runtime
        self.io_params['size'] = size
        if fio_filename:
            self.io_params['filename'] = fio_filename
        self.io_params['iodepth'] = depth
        self.fio_thread = self.wl_obj.run(**self.io_params)

    def run_git_clone(self):
        """
        Execute git clone on a pod to simulate a Jenkins user
        """
        name = 'test_workload'
        work_load = 'jenkins'

        wl = workload.WorkLoad(
            name=name, work_load=work_load, pod=self
        )
        assert wl.setup(), "Setup up for git failed"
        wl.run()


# Helper functions for Pods

def get_all_pods(namespace=None):
    """
    Get all pods in a namespace.
    If namespace is None - get all pods

    Returns:
        list: List of Pod objects
    """
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    pods = ocp_pod_obj.get()['items']
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
    assert ct_pod_items, "No Ceph tools pod found"
    ceph_pod = Pod(**ct_pod_items[0])
    return ceph_pod


def get_rbd_provisioner_pod():
    """
    Get the RBD provisioner pod

    Returns:
        Pod object: The RBD provisioner pod object
    """
    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
    )
    rbd_provision_pod_items = ocp_pod_obj.get(
        selector='app=csi-rbdplugin-provisioner'
    )['items']
    assert rbd_provision_pod_items, "No RBD provisioner pod found"
    ceph_pod = Pod(**rbd_provision_pod_items[0])
    return ceph_pod


def get_cephfs_provisioner_pod():
    """
    Get the cephfs provisioner pod

    Returns:
        Pod object: The cephfs provisioner pod object
    """
    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
    )
    cephfs_provision_pod_items = ocp_pod_obj.get(
        selector='app=csi-cephfsplugin-provisioner'
    )['items']
    assert cephfs_provision_pod_items, "No cephfs provisioner pod found"
    ceph_pod = Pod(**cephfs_provision_pod_items[0])
    return ceph_pod


def list_ceph_images(pool_name='rbd'):
    """
    Args:
        pool_name (str): Name of the pool to get the ceph images

    Returns (List): List of RBD images in the pool
    """
    ct_pod = get_ceph_tools_pod()
    return ct_pod.exec_ceph_cmd(ceph_cmd=f"rbd ls {pool_name}", format='json')


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
    logging.info(f"Waiting for IO results from pod {pod_obj.name}")
    fio_result = pod_obj.get_fio_results()
    logging.info("IOPs after FIO:")
    logging.info(
        f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
    )
    logging.info(
        f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
    )


def run_io_in_bg(pod_obj, expect_to_fail=False):
    """
    Run I/O in the background

    Args:
        pod_obj (Pod): The object of the pod
        expect_to_fail (bool): True for the command to be expected to fail
            (disruptive operations), False otherwise

    Returns:
        Thread: A thread of the I/O execution
    """
    logger.info(f"Running I/O on pod {pod_obj.name}")

    def exec_run_io_cmd(pod_obj, expect_to_fail):
        """
        Execute I/O
        """
        try:
            # Writing content to a new file every 0.01 seconds.
            # Without sleep, the device will run out of space very quickly -
            # 5-10 seconds for a 5GB device
            pod_obj.exec_cmd_on_pod(
                f"bash -c \"let i=0; while true; do echo {TEXT_CONTENT} "
                f">> {TEST_FILE}$i; let i++; sleep 0.01; done\""
            )
        # Once the pod gets deleted, the I/O execution will get terminated.
        # Hence, catching this exception
        except CommandFailed as ex:
            if expect_to_fail:
                if re.search("code 137", str(ex)):
                    logger.info("I/O command got terminated as expected")
                    return
            raise ex

    thread = Thread(target=exec_run_io_cmd, args=(pod_obj, expect_to_fail,))
    thread.start()
    sleep(2)

    # Checking file existence
    test_file = TEST_FILE + "1"
    assert check_file_existence(pod_obj, test_file), (
        f"I/O failed to start inside {pod_obj.name}"
    )

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
        dict: of pod info
    """
    ocp_pod = OCP(kind=constants.POD, namespace=namespace)
    pods = ocp_pod.get(selector=label).get('items')
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


def get_pod_logs(pod_name, container=None):
    """
    Get logs from a given pod

    pod_name (str): Name of the pod
    container (str): Name of the container

    Returns:
        str: Output from 'oc get logs <pod_name> command
    """
    pod = OCP(
        kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    cmd = f"logs {pod_name}"
    if container:
        cmd += f" -c {container}"
    return pod.exec_oc_cmd(cmd, out_yaml_format=False)


def get_pod_node(pod_obj):
    """
    Get the node that the pod is running on

    Args:
        pod_obj (OCS): The pod object

    Returns:
        OCP: The node object

    """
    node_name = pod_obj.get().get('spec').get('nodeName')
    return node.get_node_objs(node_names=node_name)[0]


def delete_pods(pod_objs):
    """
    Deletes list of the pod objects

    Args:
        pod_objs (list): List of the pod objects to be deleted

    Returns:
        bool: True if deletion is successful

    """
    for pod in pod_objs:
        pod.delete()
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
