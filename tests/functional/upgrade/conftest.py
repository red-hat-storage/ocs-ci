import copy
import logging
import textwrap

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.pod import Pod, cal_md5sum
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.utility.utils import run_ceph_health_cmd, TimeoutSampler
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_cluster,
    StorageCluster,
)


log = logging.getLogger(__name__)


def create_fio_pod(
    project,
    interface,
    pvc_factory,
    storageclass,
    access_mode,
    fio_job_dict,
    fio_configmap_dict,
    tmp_path,
    volume_mode=None,
    pvc_size=10,
):
    """
    Create pods for upgrade testing.

    Args:
        project (obj): Project in which to create resources
        interface (str): CephBlockPool or CephFileSystem
        pvc_factory (function): Function for creating PVCs
        storageclass (obj): Storageclass to use
        access_mode (str): ReadWriteOnce, ReadOnlyMany or ReadWriteMany.
            This decides the access mode to be used for the PVC
        fio_job_dict (dict): fio job dictionary to use
        fio_configmap_dict (dict): fio configmap dictionary to use
        tmp_path (obj): reference to tmp_path fixture object
        volume_mode (str): Volume mode for rbd RWO PVC
        pvc_size (int): Size of PVC in GiB

    Return:
        list: List of generated pods

    """
    log.info(
        f"Creating pod via {interface} using {access_mode}"
        f" access mode, {volume_mode} volume mode and {storageclass.name}"
        f" storageclass"
    )
    pvc = pvc_factory(
        project=project,
        storageclass=storageclass,
        access_mode=access_mode,
        volume_mode=volume_mode,
        size=pvc_size,
        status=None,
    )
    helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND, timeout=600)

    job_volume = fio_job_dict["spec"]["template"]["spec"]["volumes"][0]
    job_volume["persistentVolumeClaim"]["claimName"] = pvc.name
    fio_objs = [fio_configmap_dict, fio_job_dict]
    job_file = ObjectConfFile("fio_continuous", fio_objs, project, tmp_path)

    # deploy the Job to the cluster and start it
    job_file.create()

    ocp_pod_obj = ocp.OCP(kind=constants.POD, namespace=project.namespace)
    pods = ocp_pod_obj.get()["items"]
    for pod in pods:
        pod_volume = pod["spec"]["volumes"][0]
        if pod_volume["persistentVolumeClaim"]["claimName"] == pvc.name:
            pod_data = pod
            break

    return Pod(**pod_data)


def set_fio_dicts(job_name, fio_job_dict, fio_configmap_dict, mode="fs"):
    """
    Set correct names for jobs, targets, configs and volumes in fio
    dictionaries.

    Args:
        job_name (str): fio job name
        fio_job_dict (dict): instance of fio_job_dict fixture
        fio_configmap_dict (dict): instance of fio_configmap_dict fixture
        mode (str): block or fs

    Returns:
        tupple: Edited fio_job_dict and fio_configmap_dict

    """
    config_name = f"{job_name}-config"
    volume_name = f"{config_name}-vol"
    target_name = f"{job_name}-target"

    fio_configmap_dict["metadata"]["name"] = config_name
    fio_job_dict["metadata"]["name"] = job_name
    fio_job_dict["spec"]["template"]["metadata"]["name"] = job_name

    job_spec = fio_job_dict["spec"]["template"]["spec"]
    job_spec["volumes"][0]["name"] = target_name
    job_spec["volumes"][0]["persistentVolumeClaim"]["claimName"] = target_name
    job_spec["volumes"][1]["name"] = volume_name
    job_spec["volumes"][1]["configMap"]["name"] = config_name

    job_spec["containers"][0]["volumeMounts"][0]["name"] = target_name
    job_spec["containers"][0]["volumeMounts"][1]["name"] = volume_name

    if mode == "block":
        # set correct path for fio volumes
        fio_job_dict_block = copy.deepcopy(fio_job_dict)
        job_spec = fio_job_dict_block["spec"]["template"]["spec"]
        job_spec["containers"][0]["volumeDevices"] = []
        job_spec["containers"][0]["volumeDevices"].append(
            job_spec["containers"][0]["volumeMounts"].pop(0)
        )
        block_path = "/dev/rbdblock"
        # set correct path for fio volumes
        job_spec["containers"][0]["volumeDevices"][0]["devicePath"] = block_path
        try:
            job_spec["containers"][0]["volumeDevices"][0].pop("mountPath")
        except KeyError:
            # mountPath key might be missing from previous update of fio_job_dict
            pass

        return fio_job_dict_block, fio_configmap_dict

    return fio_job_dict, fio_configmap_dict


@pytest.fixture(scope="session")
def tmp_path(tmp_path_factory):
    """
    Path for fio related artefacts

    """
    return tmp_path_factory.mktemp("fio")


@pytest.fixture(scope="session")
def fio_project(project_factory_session):
    """
    This project is used by standard workload job generated by fio.
    There shouldn't be created other pods then from upgrade pod fixtures.

    """
    log.info("Creating project for fio jobs")
    return project_factory_session()


@pytest.fixture(scope="session")
def fio_project_mcg(project_factory_session):
    """
    This project is used by MCG workload job generated by fio.

    """
    log.info("Creating project for MCG fio job")
    return project_factory_session()


@pytest.fixture(scope="session")
def fio_conf_fs():
    """
    Basic fio configuration for upgrade utilization for fs based pvcs

    """
    # TODO(fbalak): handle better fio size
    fio_size = 1
    return textwrap.dedent(
        f"""
        [readwrite]
        readwrite=randrw
        buffered=1
        blocksize=4k
        ioengine=libaio
        directory=/mnt/target
        size={fio_size}G
        time_based
        runtime=24h
        numjobs=10
        """
    )


@pytest.fixture(scope="session")
def fio_conf_block():
    """
    Basic fio configuration for upgrade utilization for block based pvcs

    """
    # TODO(fbalak): handle better fio size
    fio_size = 1
    return textwrap.dedent(
        f"""
        [readwrite]
        readwrite=randrw
        buffered=1
        blocksize=4k
        ioengine=libaio
        filename=/dev/rbdblock
        size={fio_size}G
        time_based
        runtime=24h
        numjobs=10
        """
    )


@pytest.fixture(scope="session")
def pre_upgrade_filesystem_pods(
    request,
    pvc_factory_session,
    default_storageclasses,
    fio_job_dict_session,
    fio_configmap_dict_session,
    fio_conf_fs,
    fio_project,
    tmp_path,
):
    """
    Generate RBD and CephFS pods for tests before upgrade is executed.
    These pods use filesystem volume type.

    Returns:
        list: List of pods with RBD and CephFs interface

    """
    pods = []
    pvc_size = 10
    fio_configmap_dict_session["data"]["workload.fio"] = fio_conf_fs
    # By setting ``backoffLimit`` to 1, fio job would be restarted if it fails.
    # One such restart may be necessary to make sure the job is running IO
    # during whole upgrade.
    # See also:
    # https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
    fio_job_dict_session["spec"]["backoffLimit"] = 1

    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN,
    ):
        job_name = f"{reclaim_policy}-rbd-rwo-fs".lower()
        fio_job_dict, fio_configmap_dict = set_fio_dicts(
            job_name, fio_job_dict_session, fio_configmap_dict_session
        )
        rbd_pod = create_fio_pod(
            project=fio_project,
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory_session,
            storageclass=default_storageclasses.get(reclaim_policy)[0],
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
            fio_job_dict=fio_job_dict,
            fio_configmap_dict=fio_configmap_dict,
            pvc_size=pvc_size,
            tmp_path=tmp_path,
        )
        pods.append(rbd_pod)

        for access_mode in (constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX):
            job_name = f"{reclaim_policy}-cephfs-{access_mode}-fs".lower()
            fio_job_dict, fio_configmap_dict = set_fio_dicts(
                job_name, fio_job_dict_session, fio_configmap_dict_session
            )
            cephfs_pod = create_fio_pod(
                project=fio_project,
                interface=constants.CEPHFILESYSTEM,
                pvc_factory=pvc_factory_session,
                storageclass=default_storageclasses.get(reclaim_policy)[1],
                access_mode=access_mode,
                fio_job_dict=fio_job_dict,
                fio_configmap_dict=fio_configmap_dict,
                pvc_size=pvc_size,
                tmp_path=tmp_path,
            )
            pods.append(cephfs_pod)

    def teardown():
        for pod in pods:
            try:
                pod.delete()
            except CommandFailed as ex:
                log.info(
                    f"Command for pod deletion failed but it was probably "
                    f"deleted: {ex}"
                )

    request.addfinalizer(teardown)

    return pods


@pytest.fixture(scope="session")
def pre_upgrade_block_pods(
    request,
    pvc_factory_session,
    default_storageclasses,
    fio_job_dict_session,
    fio_configmap_dict_session,
    fio_conf_block,
    fio_project,
    tmp_path,
):
    """
    Generate RBD pods for tests before upgrade is executed.
    These pods use block volume type.

    Returns:
        list: List of pods with RBD interface

    """
    pods = []

    pvc_size = 10
    fio_configmap_dict_session["data"]["workload.fio"] = fio_conf_block
    # By setting ``backoffLimit`` to 1, fio job would be restarted if it fails.
    # One such restart may be necessary to make sure the job is running IO
    # during whole upgrade.
    fio_job_dict_session["spec"]["backoffLimit"] = 1

    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN,
    ):
        for access_mode in (constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO):
            job_name = f"{reclaim_policy}-rbd-{access_mode}-block".lower()
            fio_job_dict_block, fio_configmap_dict = set_fio_dicts(
                job_name, fio_job_dict_session, fio_configmap_dict_session, mode="block"
            )

            rbd_pod = create_fio_pod(
                project=fio_project,
                interface=constants.CEPHBLOCKPOOL,
                pvc_factory=pvc_factory_session,
                storageclass=default_storageclasses.get(reclaim_policy)[0],
                access_mode=access_mode,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                fio_job_dict=fio_job_dict_block,
                fio_configmap_dict=fio_configmap_dict,
                pvc_size=pvc_size,
                tmp_path=tmp_path,
            )
            pods.append(rbd_pod)

    def teardown():
        for pod in pods:
            try:
                pod.delete()
            except CommandFailed as ex:
                log.info(
                    f"Command for pod deletion failed but it was probably "
                    f"deleted: {ex}"
                )

    request.addfinalizer(teardown)

    return pods


@pytest.fixture
def post_upgrade_filesystem_pods(
    request,
    pvc_factory,
    default_storageclasses,
    fio_job_dict,
    fio_configmap_dict,
    fio_conf_fs,
    fio_project,
    tmp_path,
):
    """
    Generate RBD and CephFS pods for tests after upgrade is executed.
    These pods use filesystem volume type.

    Returns:
        list: List of pods with RBD and CephFS interface

    """
    pods = []

    pvc_size = 10
    fio_configmap_dict["data"]["workload.fio"] = fio_conf_fs

    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN,
    ):
        job_name = f"{reclaim_policy}-rbd-rwo-fs-post".lower()
        fio_job_dict, fio_configmap_dict = set_fio_dicts(
            job_name, fio_job_dict, fio_configmap_dict
        )
        rbd_pod = create_fio_pod(
            project=fio_project,
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory,
            storageclass=default_storageclasses.get(reclaim_policy)[0],
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
            fio_job_dict=fio_job_dict,
            fio_configmap_dict=fio_configmap_dict,
            pvc_size=pvc_size,
            tmp_path=tmp_path,
        )
        pods.append(rbd_pod)

        for access_mode in (constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX):
            job_name = f"{reclaim_policy}-cephfs-{access_mode}-fs-post".lower()
            fio_job_dict, fio_configmap_dict = set_fio_dicts(
                job_name, fio_job_dict, fio_configmap_dict
            )
            cephfs_pod = create_fio_pod(
                project=fio_project,
                interface=constants.CEPHFILESYSTEM,
                pvc_factory=pvc_factory,
                storageclass=default_storageclasses.get(reclaim_policy)[1],
                access_mode=access_mode,
                fio_job_dict=fio_job_dict,
                fio_configmap_dict=fio_configmap_dict,
                pvc_size=pvc_size,
                tmp_path=tmp_path,
            )
            pods.append(cephfs_pod)

    def teardown():
        for pod in pods:
            try:
                pod.delete()
            except CommandFailed as ex:
                log.info(
                    f"Command for pod deletion failed but it was probably "
                    f"deleted: {ex}"
                )

    request.addfinalizer(teardown)

    return pods


@pytest.fixture
def post_upgrade_block_pods(
    request,
    pvc_factory,
    default_storageclasses,
    fio_job_dict,
    fio_configmap_dict,
    fio_conf_block,
    fio_project,
    tmp_path,
):
    """
    Generate RBD pods for tests after upgrade is executed.
    These pods use block volume type.

    Returns:
        list: List of pods with RBD interface

    """
    pods = []

    pvc_size = 10
    fio_configmap_dict["data"]["workload.fio"] = fio_conf_block

    for reclaim_policy in (
        constants.RECLAIM_POLICY_DELETE,
        constants.RECLAIM_POLICY_RETAIN,
    ):
        for access_mode in (constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO):
            job_name = f"{reclaim_policy}-rbd-{access_mode}-fs-post".lower()
            fio_job_dict_block, fio_configmap_dict = set_fio_dicts(
                job_name, fio_job_dict, fio_configmap_dict, mode="block"
            )

            rbd_pod = create_fio_pod(
                project=fio_project,
                interface=constants.CEPHBLOCKPOOL,
                pvc_factory=pvc_factory,
                storageclass=default_storageclasses.get(reclaim_policy)[0],
                access_mode=access_mode,
                volume_mode=constants.VOLUME_MODE_BLOCK,
                fio_job_dict=fio_job_dict_block,
                fio_configmap_dict=fio_configmap_dict,
                pvc_size=pvc_size,
                tmp_path=tmp_path,
            )
            pods.append(rbd_pod)

    def teardown():
        for pod in pods:
            try:
                pod.delete()
            except CommandFailed as ex:
                log.info(
                    f"Command for pod deletion failed but it was probably "
                    f"deleted: {ex}"
                )

    request.addfinalizer(teardown)

    return pods


@pytest.fixture(scope="session")
def pre_upgrade_pods_running_io(
    pre_upgrade_filesystem_pods,
    pre_upgrade_block_pods,
):
    return pre_upgrade_filesystem_pods + pre_upgrade_block_pods


@pytest.fixture(scope="session")
def mcg_workload_job(
    fio_project_mcg,
    mcg_job_factory_session,
):
    """
    Creates kubernetes job that should utilize MCG during upgrade.

    Returns:
        object: Job object

    """
    return mcg_job_factory_session(job_name="mcg-workload", project=fio_project_mcg)


@pytest.fixture(scope="session")
def upgrade_buckets(bucket_factory_session, awscli_pod_session, mcg_obj_session):
    """
    Additional NooBaa buckets that are created for upgrade testing. First
    bucket is populated with data and quota to 1 PB is set.

    Returns:
        list: list of buckets that should survive OCS and OCP upgrade.
            First one has bucket quota set to 1 PB and is populated
            with 3.5 GB.

    """
    buckets = bucket_factory_session(amount=3)

    # add quota to the first bucket

    update_quota_payload = {
        "name": buckets[0].name,
        "quota": {
            "size": {
                "unit": "P",
                # The size value here refers to how many Petabytes are allowed
                "value": 1,
            },
            # The quantity value here refers to max number of objects
            "quantity": {"value": 1000},
        },
    }

    mcg_obj_session.send_rpc_query(
        "bucket_api",
        "update_bucket",
        update_quota_payload,
    )

    # add some data to the first pod
    awscli_pod_session.exec_cmd_on_pod(
        "dd if=/dev/urandom of=/tmp/testfile bs=1M count=500"
    )
    for i in range(1, 7):
        awscli_pod_session.exec_cmd_on_pod(
            craft_s3_command(
                f"cp /tmp/testfile s3://{buckets[0].name}/testfile{i}", mcg_obj_session
            ),
            out_yaml_format=False,
            secrets=[
                mcg_obj_session.access_key_id,
                mcg_obj_session.access_key,
                mcg_obj_session.s3_endpoint,
            ],
        )

    return buckets


@pytest.fixture(scope="session")
def block_pod(pvc_factory_session, pod_factory_session):
    """
    Returns:
        obj: Utilized pod with RBD pvc

    """
    pvc = pvc_factory_session(size=5, interface=constants.CEPHBLOCKPOOL)
    pod = pod_factory_session(pvc=pvc, interface=constants.CEPHBLOCKPOOL)
    log.info(f"Utilization of RBD PVC {pvc.name} with pod {pod.name} starts")
    pod.run_io(
        storage_type="fs",
        size="4G",
        fio_filename="fio-rand-write",
    )
    pod.get_fio_results()
    log.info(f"IO finished on pod {pod.name}")
    return pod


@pytest.fixture(scope="session")
def block_md5(block_pod):
    """
    Returns:
        str: md5 of utilized file

    """
    md5 = cal_md5sum(
        pod_obj=block_pod,
        file_name="fio-rand-write",
        block=False,
    )
    log.info(f"RBD md5: {md5}")
    return md5


@pytest.fixture(scope="session")
def fs_pod(pvc_factory_session, pod_factory_session):
    """
    Returns:
        obj: Utilized pod with Ceph FS pvc

    """
    pvc = pvc_factory_session(size=5, interface=constants.CEPHFILESYSTEM)
    pod = pod_factory_session(pvc=pvc, interface=constants.CEPHFILESYSTEM)
    log.info(f"Utilization of Ceph FS PVC {pvc.name} with pod {pod.name} starts")
    pod.run_io(
        storage_type="fs",
        size="4G",
        fio_filename="fio-rand-write",
    )
    pod.get_fio_results()
    log.info(f"IO finished on pod {pod.name}")
    return pod


@pytest.fixture(scope="session")
def fs_md5(fs_pod):
    """
    Returns:
        str: md5 of utilized file

    """
    md5 = cal_md5sum(
        pod_obj=fs_pod,
        file_name="fio-rand-write",
        block=False,
    )
    log.info(f"Ceph FS md5: {md5}")
    return md5


@pytest.fixture(scope="session")
def upgrade_stats():
    """

    Returns:
        dict: List of statistics gathered during performed upgrade.

    """
    return {"odf_upgrade": {}, "ocp_upgrade": {}}


@pytest.fixture(scope="function")
def rook_operator_configmap_cleanup(request):
    """
    Restore values of CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE and
    CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE parameters in
    rook-ceph-operator-config configmap after a test.
    """
    configmap = ocp.OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
    )
    configmap_data = configmap.get()
    rbd_max = configmap_data.get("data", {}).get(
        "CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"
    )
    cephfs_max = configmap_data.get("data", {}).get(
        "CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"
    )

    def restore_values():
        """
        Restore values of CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE and
        CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE to original values.
        Remove them if they were not set.
        """
        if rbd_max is None:
            try:
                params = '[{"op": "remove", "path": "/data/CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"}]'
                configmap.patch(params=params, format_type="json")
            except CommandFailed as e:
                log.warning(
                    "delete failed - it is possible that "
                    f"CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE was removed earlier: {e}"
                )
        else:
            params = f'{{"data": {{"CSI_RBD_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE": "{rbd_max}"}}}}'
            configmap.patch(
                params=params,
                format_type="merge",
            )
        if cephfs_max is None:
            try:
                params = '[{"op": "remove", "path": "/data/CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE"}]'
                configmap.patch(params=params, format_type="json")
            except CommandFailed as e:
                log.warning(
                    "delete failed - it is possible that "
                    f"CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE was removed earlier: {e}"
                )
        else:
            params = f'{{"data": {{"CSI_CEPHFS_PLUGIN_UPDATE_STRATEGY_MAX_UNAVAILABLE": "{cephfs_max}"}}}}'
            configmap.patch(
                params=params,
                format_type="merge",
            )

    request.addfinalizer(restore_values)


@pytest.fixture(scope="function")
def mon_pod_down(request):
    """
    Fixture to scale down one MON deployment to cause HEALTH_WARN.
    This keeps the MON down for a while before rook creates a replacement.
    Restores the MON deployment in teardown.

    Returns:
        str: The MON deployment name that was scaled down

    """
    namespace = config.ENV_DATA["cluster_namespace"]

    # Get MON deployments
    mon_deployments = get_mon_deployments(namespace=namespace)
    if len(mon_deployments) < 3:
        pytest.skip("Need at least 3 MON deployments to safely test MON down scenario")

    # Select one MON deployment to scale down
    mon_deployment_to_scale = mon_deployments[0]
    mon_deployment_name = mon_deployment_to_scale.name
    log.info(
        f"Scaling down MON deployment {mon_deployment_name} "
        "to 0 replicas to cause HEALTH_WARN"
    )

    # Scale down the MON deployment to 0 replicas
    modify_deployment_replica_count(
        deployment_name=mon_deployment_name,
        replica_count=0,
        namespace=namespace,
    )
    log.info(
        f"Successfully scaled down MON deployment {mon_deployment_name} "
        "to 0 replicas"
    )

    # Wait for ceph health to show warning
    log.info("Waiting for ceph health to show warning...")
    timeout = 300

    def check_health_warn():
        """Check if health status contains WARN"""
        health_status = run_ceph_health_cmd(namespace=namespace, detail=False)
        return "WARN" in health_status or "HEALTH_WARN" in health_status

    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=check_health_warn,
    )

    try:
        if sample.wait_for_func_status(result=True):
            health_status = run_ceph_health_cmd(namespace=namespace, detail=False)
            log.info(f"Ceph health status: {health_status}")
        else:
            log.warning("Failed to get HEALTH_WARN status within timeout")
    except Exception as e:
        log.warning(f"Failed to get HEALTH_WARN status: {e}")
        # Continue anyway as the MON deployment is scaled down

    # Add finalizer to restore MON deployment
    def finalizer():
        """Teardown: Scale up the MON deployment back to 1 replica"""
        log.info(
            f"Scaling up MON deployment {mon_deployment_name} " "back to 1 replica..."
        )
        try:
            modify_deployment_replica_count(
                deployment_name=mon_deployment_name,
                replica_count=1,
                namespace=namespace,
            )
            log.info(
                f"Successfully scaled up MON deployment {mon_deployment_name} "
                "to 1 replica"
            )

            # Wait for deployment to have 1 available replica
            log.info("Waiting for MON deployment to have 1 available replica...")
            deployment_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=namespace,
                resource_name=mon_deployment_name,
            )
            sample = TimeoutSampler(
                timeout=600,
                sleep=10,
                func=lambda: (
                    deployment_obj.get().get("status", {}).get("availableReplicas", 0)
                    == 1
                ),
            )
            if sample.wait_for_func_status(result=True):
                log.info(
                    f"MON deployment {mon_deployment_name} " "has 1 available replica"
                )
            else:
                log.warning(
                    f"MON deployment {mon_deployment_name} "
                    "did not reach 1 available replica within timeout. "
                    "Cluster may be in an unhealthy state."
                )

            # Wait for ceph health to return to HEALTH_OK
            log.info("Waiting for ceph health to return to HEALTH_OK...")
            ceph_cluster = CephCluster()
            ceph_cluster.cluster_health_check(timeout=600)
            log.info("Ceph cluster health restored to HEALTH_OK")
        except Exception as e:
            log.error(f"Failed to restore MON deployment or ceph health: {e}")
            # Log but don't fail - this is teardown

    request.addfinalizer(finalizer)
    return mon_deployment_name


@pytest.fixture(scope="function")
def storagecluster_to_progressing(request):
    """
    Fixture that patches StorageCluster resourceProfile to trigger Progressing
    state and restores it in teardown.

    This fixture changes the resourceProfile spec field, which triggers a
    Progressing state transition (3-5 minutes).

    Returns:
        str: The StorageCluster resource name that was patched

    """

    namespace = config.ENV_DATA["cluster_namespace"]

    # Get StorageCluster object
    sc_obj = get_storage_cluster(namespace=namespace)
    sc_data = sc_obj.get()
    sc_items = sc_data.get("items", [])
    if not sc_items:
        pytest.skip("No StorageCluster found in namespace")

    sc_name = sc_items[0]["metadata"]["name"]
    storage_cluster = StorageCluster(resource_name=sc_name, namespace=namespace)
    storage_cluster.reload_data()

    # Get current resourceProfile value
    current_spec = storage_cluster.data.get("spec", {})
    original_resource_profile = current_spec.get("resourceProfile")
    log.info(
        f"Current StorageCluster {sc_name} resourceProfile: "
        f"{original_resource_profile}"
    )

    # Determine new resourceProfile value to trigger Progressing state
    # Cycle through available profiles: balanced -> performance -> lean
    # or set to a different value if already set
    available_profiles = [
        constants.PERFORMANCE_PROFILE_BALANCED,
        constants.PERFORMANCE_PROFILE_PERFORMANCE,
        constants.PERFORMANCE_PROFILE_LEAN,
    ]
    if original_resource_profile in available_profiles:
        # Get next profile in cycle
        current_index = available_profiles.index(original_resource_profile)
        new_resource_profile = available_profiles[
            (current_index + 1) % len(available_profiles)
        ]
    else:
        # If no profile set or unknown, set to balanced
        new_resource_profile = constants.PERFORMANCE_PROFILE_BALANCED

    log.info(
        f"Patching StorageCluster {sc_name} resourceProfile from "
        f"{original_resource_profile} to {new_resource_profile} to trigger "
        "Progressing state"
    )

    # Patch StorageCluster to change resourceProfile
    patch_params = f'{{"spec": {{"resourceProfile": "{new_resource_profile}"}}}}'
    storage_cluster.patch(params=patch_params, format_type="merge")
    log.info(
        f"Successfully patched StorageCluster {sc_name} resourceProfile to "
        f"{new_resource_profile}"
    )

    # Wait for StorageCluster to transition to Progressing state
    log.info("Waiting for StorageCluster to transition to Progressing state...")
    timeout = 300  # 5 minutes

    def check_progressing():
        """Check if StorageCluster is in Progressing phase"""
        storage_cluster.reload_data()
        phase = storage_cluster.data.get("status", {}).get("phase")
        log.info(f"StorageCluster phase: {phase}")
        return phase == constants.STATUS_PROGRESSING

    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=check_progressing,
    )

    try:
        if sample.wait_for_func_status(result=True):
            log.info(
                f"StorageCluster {sc_name} successfully transitioned to "
                "Progressing state"
            )
        else:
            log.warning(
                f"StorageCluster {sc_name} did not reach Progressing state "
                "within timeout, but continuing with test"
            )
    except Exception as e:
        log.warning(
            f"Failed to verify Progressing state for StorageCluster "
            f"{sc_name}: {e}. Continuing with test."
        )

    # Add finalizer to restore original resourceProfile
    def finalizer():
        """Teardown: Restore original resourceProfile"""
        log.info(
            f"Restoring StorageCluster {sc_name} resourceProfile to "
            f"{original_resource_profile}..."
        )
        try:
            storage_cluster.reload_data()
            if original_resource_profile is None:
                # Remove resourceProfile if it was not set originally
                patch_params = '[{"op": "remove", "path": "/spec/resourceProfile"}]'
                storage_cluster.patch(params=patch_params, format_type="json")
            else:
                # Restore original resourceProfile
                patch_params = (
                    f'{{"spec": {{"resourceProfile": '
                    f'"{original_resource_profile}"}}}}'
                )
                storage_cluster.patch(params=patch_params, format_type="merge")
            log.info(
                f"Successfully restored StorageCluster {sc_name} "
                f"resourceProfile to {original_resource_profile}"
            )

            # Wait for StorageCluster to return to Ready state
            log.info("Waiting for StorageCluster to return to Ready state...")
            storage_cluster.wait_for_phase(phase=constants.STATUS_READY, timeout=600)
            log.info(f"StorageCluster {sc_name} returned to Ready state")
        except Exception as e:
            log.error(
                f"Failed to restore StorageCluster {sc_name} " f"resourceProfile: {e}"
            )
            # Log but don't fail - this is teardown

    request.addfinalizer(finalizer)
    return sc_name
