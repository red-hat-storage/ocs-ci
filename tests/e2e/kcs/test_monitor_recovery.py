import logging
import base64
import time
import os
from os.path import join
import tempfile
import re

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    ignore_leftovers,
    tier3,
    skipif_openshift_dedicated,
    skipif_external_mode,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import E2ETest, config
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_osd_pods,
    get_deployments_having_label,
    get_mds_pods,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import ocp, constants, defaults, bucket_utils
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


@tier3
@ignore_leftovers
@pytest.mark.polarion_id("OCS-3911")
@pytest.mark.bugzilla("1973256")
@skipif_ocs_version("<4.9")
@skipif_openshift_dedicated
@skipif_external_mode
class TestMonitorRecovery(E2ETest):
    """
    Test to verify monitor recovery

    """

    @pytest.fixture(autouse=True)
    def mon_recovery_setup(
        self,
        dc_pod_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Creates project, pvcs, dc-pods and obcs

        """
        self.filename = "sample_file.txt"
        self.object_key = "obj-key"
        self.object_data = "Random string data"
        self.dd_cmd = f"dd if=/dev/urandom of=/mnt/{self.filename} bs=4M count=3"
        # self.sanity_helpers = Sanity()

        # Create project, pvc, dc pods
        self.dc_pods = []
        self.dc_pods.append(
            dc_pod_factory(
                interface=constants.CEPHBLOCKPOOL,
            )
        )
        self.dc_pods.append(
            dc_pod_factory(
                interface=constants.CEPHFILESYSTEM,
                access_mode=constants.ACCESS_MODE_RWX,
            )
        )
        for pod_obj in self.dc_pods:
            pod_obj.exec_cmd_on_pod(command=self.dd_cmd)
            # Calculate md5sum
            md5sum = pod.cal_md5sum(pod_obj, self.filename)
            pod_obj.pvc.md5sum = md5sum

        logger.info("Putting object on an object bucket")
        self.bucket_name = bucket_factory(interface="OC")[0].name
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj,
            bucketname=self.bucket_name,
            object_key=self.object_key,
            data=self.object_data,
        ), "Failed: PutObject"

    def test_monitor_recovery(
        self,
        dc_pod_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Verifies Monitor recovery procedure as per:
        https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.9/
        html-single/troubleshooting_openshift_container_storage/index

        """
        # # Initialize mon recovery class
        # mon_recovery = MonitorRecovery()
        #
        # logger.info("Backing up all the deployments")
        # mon_recovery.backup_deployments()
        # mons_revert = mon_recovery.mon_deployments_to_revert()
        # mds_revert = mon_recovery.mds_deployments_to_revert()
        #
        # logger.info("Corrupting ceph monitors by deleting store db")
        # corrupt_ceph_monitors()
        #
        # logger.info("Starting the monitor recovery procedure")
        # logger.info("Scaling down rook and ocs operators")
        # mon_recovery.scale_rook_ocs_operators(replica=0)
        #
        # logger.info(
        #     "Patching OSDs to remove LivenessProbe and setting sleep to infinity"
        # )
        # mon_recovery.patch_sleep_on_osds()
        #
        # logger.info("Getting mon-store from OSDs")
        # mon_recovery.get_monstore_from_osds()
        #
        # logger.info("Patching MONs to sleep infinitely")
        # mon_recovery.patch_sleep_on_mon()
        #
        # logger.info("Updating initial delay on all monitors")
        # update_mon_initial_delay()
        #
        # logger.info("Generating monitor map command using the IPs")
        # self.mon_map_cmd = generate_monmap_cmd()
        #
        # logger.info("Getting ceph keyring from ocs secrets")
        # self.keyring_files = mon_recovery.get_ceph_keyrings()
        #
        # logger.info("Rebuilding Monitors to recover store db")
        # mon_recovery.monitor_rebuild()
        #
        # logger.info("Reverting mon, osd and mgr deployments")
        # mon_recovery.revert_patches(mons_revert)
        #
        # logger.info("Scaling back rook and ocs operators")
        # mon_recovery.scale_rook_ocs_operators(replica=1)
        #
        # logger.info("Sleeping for 150 secs for cluster to stabilize")
        # time.sleep(150)
        # logger.info("Recovering CephFS")
        # mon_recovery.scale_rook_ocs_operators(replica=0)
        #
        # logger.info(
        #     "Patching MDSs to remove LivenessProbe and setting sleep to infinity"
        # )
        # mon_recovery.patch_sleep_on_mds()
        #
        # logger.info("Resetting the fs")
        # ceph_fs_recovery()
        #
        # logger.info("Reverting MDS deployments")
        # mon_recovery.revert_patches(mds_revert)
        #
        # logger.info("Scaling back rook and ocs operators")
        # mon_recovery.scale_rook_ocs_operators(replica=1)
        # archive_and_mute_ceph_warn()
        #
        # self.sanity_helpers.health_check(tries=10)

        logger.info("Verifying md5sum of files after recovery")
        for pod_obj in self.dc_pods:
            current_md5sum = pod.cal_md5sum(pod_obj, self.filename)
            assert current_md5sum == pod_obj.pvc.md5sum, "Data corruption found"
            logger.info(
                f"Verified: md5sum of {self.filename} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        # Create new project, pvc, dc pods
        new_dc_pods = [
            dc_pod_factory(
                interface=constants.CEPHBLOCKPOOL,
            ),
            dc_pod_factory(
                interface=constants.CEPHFILESYSTEM,
            ),
        ]
        for pod_obj in new_dc_pods:
            pod_obj.exec_cmd_on_pod(command=self.dd_cmd)

        logger.info("Getting object after recovery")
        assert bucket_utils.s3_get_object(
            s3_obj=mcg_obj,
            bucketname=self.bucket_name,
            object_key=self.object_key,
        ), "Failed: PutObject"

        logger.info("Creating new bucket and write object")
        new_bucket = bucket_factory(interface="OC")[0].name
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj,
            bucketname=new_bucket,
            object_key=self.object_key,
            data=self.object_data,
        ), "Failed: PutObject"


class MonitorRecovery(object):
    """
    Monitor recovery class

    """

    def __init__(self):
        """
        Initializer

        """
        self.backup_dir = tempfile.mkdtemp(prefix="mon-backup-")
        self.keyring_dir = tempfile.mkdtemp(dir=self.backup_dir, prefix="keyring-")
        self.mon_map_cmd = ""
        self.keyring_files = []
        self.dep_ocp = OCP(
            kind=constants.DEPLOYMENT, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.ocp_obj = ocp.OCP(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)

    def scale_rook_ocs_operators(self, replica=1):
        """
        Scales rook and ocs operators based on replica

        Args:
            replica (int): replica count

        """
        logger.info(f"Scaling rook-ceph operator to replica: {replica}")
        self.dep_ocp.exec_oc_cmd(
            f"scale deployment {constants.ROOK_CEPH_OPERATOR} --replicas={replica}"
        )
        logger.info(f"Scaling ocs-operator to replica: {replica}")
        self.dep_ocp.exec_oc_cmd(
            f"scale deployment {defaults.OCS_OPERATOR_NAME} --replicas={replica}"
        )

    def patch_sleep_on_osds(self):
        """
        Patch the OSD deployments to sleep and remove the `livenessProbe` parameter,

        """
        osd_dep = get_deployments_having_label(
            label=constants.OSD_APP_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        osd_deployments = [OCS(**osd) for osd in osd_dep]
        for osd in osd_deployments:
            logger.info(
                f"Patching OSD: {osd.name} with livenessProbe and sleep infinity"
            )
            params = '[{"op":"remove", "path":"/spec/template/spec/containers/0/livenessProbe"}]'
            self.dep_ocp.patch(
                resource_name=osd.name,
                params=params,
                format_type="json",
            )
            params = (
                '{"spec": {"template": {"spec": {"containers": [{"name": "osd", "command":'
                ' ["sleep", "infinity"], "args": []}]}}}}'
            )
            self.dep_ocp.patch(
                resource_name=osd.name,
                params=params,
            )
        logger.info(
            "Sleeping for 15 seconds and waiting for OSDs to reach running state"
        )
        time.sleep(15)
        for osd in get_osd_pods():
            wait_for_resource_state(resource=osd, state=constants.STATUS_RUNNING)

    def get_monstore_from_osds(self):
        """
        Retrieve the `monstore` cluster map from OSDs

        """
        recover_mon = """
        #!/bin/bash
        ms=/tmp/monstore
        rm -rf $ms
        mkdir $ms
        for osd_pod in $(oc get po -l app=rook-ceph-osd -oname -n openshift-storage); do
            echo "Starting with pod: $osd_pod"
            podname=$(echo $osd_pod| cut -c5-)
            oc exec $osd_pod -- rm -rf $ms
            oc cp $ms $podname:$ms
            rm -rf $ms
            mkdir $ms
            dp=/var/lib/ceph/osd/ceph-$(oc get $osd_pod -ojsonpath='{ .metadata.labels.ceph_daemon_id }')
            op=update-mon-db
            ot=ceph-objectstore-tool
            echo "pod in loop: $osd_pod ; done deleting local dirs"
            oc exec $osd_pod -- $ot --type bluestore --data-path $dp --op $op --no-mon-config --mon-store-path $ms
            echo "Done with COT on pod: $osd_pod"
            oc cp $podname:$ms $ms
            echo "Finished pulling COT data from pod: $osd_pod"
        done
    """
        with open(f"{self.backup_dir}/recover_mon.sh", "w") as file:
            file.write(recover_mon)
        exec_cmd(cmd=f"chmod +x {self.backup_dir}/recover_mon.sh")
        logger.info("Running mon-store script")
        exec_cmd(cmd=f"sh {self.backup_dir}/recover_mon.sh")

    def patch_sleep_on_mon(self):
        """
        Patches sleep to infinity on monitors

        """
        mon_dep = get_deployments_having_label(
            label=constants.MON_APP_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        mon_deployments = [OCS(**mon) for mon in mon_dep]
        for mon in mon_deployments:
            params = (
                '{"spec": {"template": {"spec": {"containers":'
                ' [{"name": "mon", "command": ["sleep", "infinity"], "args": []}]}}}}'
            )
            logger.info(f"Patching monitor: {mon.name} to sleep infinitely")
            self.dep_ocp.patch(
                resource_name=mon.name,
                params=params,
            )

    def monitor_rebuild(self):
        """
        Rebuilds the monitor

        """
        logger.info("Re-spinning the mon pods")
        for mon in get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
            mon.delete()
        mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        for mon in mon_pods:
            wait_for_resource_state(resource=mon, state=constants.STATUS_RUNNING)
        mon_a = mon_pods[0]
        logger.info(f"Working on monitor: {mon_a.name}")

        logger.info(f"Copying mon-store into monitor: {mon_a.name}")
        self._exec_oc_cmd(f"cp /tmp/monstore {mon_a.name}:/tmp/")

        logger.info("Changing ownership of monstore to ceph")
        _exec_cmd_on_pod(cmd="chown -R ceph:ceph /tmp/monstore", pod_obj=mon_a)
        self.copy_and_import_keys(mon_obj=mon_a)
        logger.info("Creating monitor map")
        _exec_cmd_on_pod(cmd=self.mon_map_cmd, pod_obj=mon_a)

        rebuild_mon_cmd = "ceph-monstore-tool /tmp/monstore rebuild -- --keyring /tmp/keyring --monmap /tmp/monmap"
        logger.info("Running command to rebuild monitor")
        mon_a.exec_cmd_on_pod(command=rebuild_mon_cmd, out_yaml_format=False)

        logger.info(f"Copying store.db directory from monitor: {mon_a.name}")
        self._exec_oc_cmd(
            f"cp {mon_a.name}:/tmp/monstore/store.db {self.backup_dir}/store.db"
        )

        logger.info("Copying store.db to rest of the monitors")
        for mon in get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
            cmd = (
                f"cp {self.backup_dir}/store.db {mon.name}:/var/lib/ceph/mon/ceph-"
                f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}/ "
            )
            logger.info(f"Copying store.db to monitor: {mon.name}")
            self._exec_oc_cmd(cmd)
            logger.info("Changing ownership of store.db to ceph:ceph")
            _exec_cmd_on_pod(
                cmd=f"chown -R ceph:ceph /var/lib/ceph/mon/ceph-"
                f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}/store.db",
                pod_obj=mon,
            )

    def copy_and_import_keys(self, mon_obj):
        """
        Copies the keys and imports it using ceph-auth

        Args:
            mon_obj (obj): Monitor object

        """
        logger.info(f"Copying keyring files to monitor: {mon_obj.name}")
        for k_file in self.keyring_files:
            cmd = f"cp {k_file} {mon_obj.name}:/tmp/"
            logger.info(f"Copying keyring: {k_file} into mon {mon_obj.name}")
            self._exec_oc_cmd(cmd)

        logger.info(f"Importing ceph keyrings to a temporary file on: {mon_obj.name}")
        _exec_cmd_on_pod(
            cmd="cp /etc/ceph/keyring-store/keyring /tmp/keyring", pod_obj=mon_obj
        )
        for k_file in self.keyring_files:
            k_file = k_file.split("/")
            logger.info(f"Importing keyring {k_file[-1]}")
            _exec_cmd_on_pod(
                cmd=f"ceph-authtool /tmp/keyring --import-keyring /tmp/{k_file[-1]}",
                pod_obj=mon_obj,
            )

    def revert_patches(self, deployment_paths):
        """
        Reverts the patches done on monitors, osds and mgr by replacing their deployments

        Args:
            deployment_paths (list): List of paths to deployment yamls

        """
        logger.info("Reverting patches on monitors, mgr and osd")
        for dep in deployment_paths:
            logger.info(f"Reverting {dep}")
            revert_patch = f"replace --force -f {dep}"
            self.ocp_obj.exec_oc_cmd(revert_patch)
        logger.info("Sleeping and waiting for all pods up and running..")
        time.sleep(120)
        assert pod.wait_for_pods_to_be_running(
            timeout=600
        ), "Pods did not reach running state"

    def backup_deployments(self):
        """
        Creates a backup of all deployments in the `openshift-storage` namespace

        """
        deployment_names = []
        deployments = self.dep_ocp.get("-o name", out_yaml_format=False)
        deployments_full_name = str(deployments).split()

        for name in deployments_full_name:
            deployment_names.append(name.lstrip("deployment.apps").lstrip("/"))

        for deployment in deployment_names:
            deployment_get = self.dep_ocp.get(resource_name=deployment)
            deployment_yaml = join(self.backup_dir, deployment + ".yaml")
            templating.dump_data_to_temp_yaml(deployment_get, deployment_yaml)

    def mon_deployments_to_revert(self):
        """
        Gets only mon deployments to revert it

        Returns:
            list: list of deployment paths to be reverted

        """
        to_revert_patches = (
            get_deployments_having_label(
                label=constants.OSD_APP_LABEL,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            + get_deployments_having_label(
                label=constants.MON_APP_LABEL,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            + get_deployments_having_label(
                label=constants.MGR_APP_LABEL,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
        )
        logger.info(to_revert_patches)
        to_revert_patches_path = []
        for dep in to_revert_patches:
            to_revert_patches_path.append(
                join(self.backup_dir, dep["metadata"]["name"] + ".yaml")
            )
        return to_revert_patches_path

    def get_ceph_keyrings(self):
        """
        Gets all ceph and csi related keyring from OCS secrets

        Returns:
            list: keyring files

        """
        mon_k = get_ceph_caps(["rook-ceph-mons-keyring"])
        if config.ENV_DATA["platform"] == constants.VSPHERE_PLATFORM:
            rgw_k = get_ceph_caps(
                ["rook-ceph-rgw-ocs-storagecluster-cephobjectstore-a-keyring"]
            )
        else:
            rgw_k = None
        mgr_k = get_ceph_caps(["rook-ceph-mgr-a-keyring"])
        mds_k = get_ceph_caps(
            [
                "rook-ceph-mds-ocs-storagecluster-cephfilesystem-a-keyring",
                "rook-ceph-mds-ocs-storagecluster-cephfilesystem-b-keyring",
            ]
        )
        crash_k = get_ceph_caps(["rook-ceph-crash-collector-keyring"])
        fs_node_k = get_ceph_caps([constants.CEPHFS_NODE_SECRET])
        rbd_node_k = get_ceph_caps([constants.RBD_NODE_SECRET])
        fs_provisinor_k = get_ceph_caps([constants.CEPHFS_PROVISIONER_SECRET])
        rbd_provisinor_k = get_ceph_caps([constants.RBD_PROVISIONER_SECRET])

        keyring_caps = {
            "mons": mon_k,
            "rgws": rgw_k,
            "mgrs": mgr_k,
            "mdss": mds_k,
            "crash": crash_k,
            "fs_node": fs_node_k,
            "rbd_node": rbd_node_k,
            "fs_provisinor": fs_provisinor_k,
            "rbd_provisinor": rbd_provisinor_k,
        }
        keyring_files = []
        mon_a = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)[0]
        logger.info(f"Working on monitor: {mon_a.name}")

        for secret, caps in keyring_caps.items():
            if caps:
                with open(f"{self.keyring_dir}/{secret}.keyring", "w") as fd:
                    fd.write(caps)
                    keyring_files.append(f"{self.keyring_dir}/{secret}.keyring")
        return keyring_files

    def mds_deployments_to_revert(self):
        """
        Gets only mds deployments to revert it

        Returns:
            list: list of deployment paths to be reverted

        """
        to_revert_mds = get_deployments_having_label(
            label=constants.MDS_APP_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        to_revert_mds_path = []
        for dep in to_revert_mds:
            logger.info(dep)
            to_revert_mds_path.append(
                join(self.backup_dir, dep["metadata"]["name"] + ".yaml")
            )
        return to_revert_mds_path

    def patch_sleep_on_mds(self):
        """
        Patch the OSD deployments to sleep and remove the `livenessProbe` parameter,

        """
        mds_dep = get_deployments_having_label(
            label=constants.MDS_APP_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        mds_deployments = [OCS(**mds) for mds in mds_dep]
        for mds in mds_deployments:
            logger.info(
                f"Patching MDS: {mds.name} to remove livenessProbe and setting sleep infinity"
            )
            params = '[{"op":"remove", "path":"/spec/template/spec/containers/0/livenessProbe"}]'
            self.dep_ocp.patch(
                resource_name=mds.name,
                params=params,
                format_type="json",
            )
            params = (
                '{"spec": {"template": {"spec": {"containers": '
                '[{"name": "mds", "command": ["sleep", "infinity"], "args": []}]}}}}'
            )
            self.dep_ocp.patch(
                resource_name=mds.name,
                params=params,
            )
        logger.info("Sleeping for 60s and waiting for MDS pods to reach running state")
        time.sleep(60)
        for mds in get_mds_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
            wait_for_resource_state(resource=mds, state=constants.STATUS_RUNNING)

    @retry(CommandFailed, tries=10, delay=10, backoff=1)
    def _exec_oc_cmd(self, cmd):
        """
        Exec oc cmd with retry

        Args:
            cmd (str): Command

        """
        self.ocp_obj.exec_oc_cmd(cmd)


@retry(CommandFailed, tries=10, delay=10, backoff=1)
def _exec_cmd_on_pod(cmd, pod_obj):
    """
    Exec oc cmd on pods with retry

    Args:
        cmd (str): Command
        pod_obj (obj): Pod object

    """
    pod_obj.exec_cmd_on_pod(cmd)


def insert_delay(mon_dep):
    """
    Inserts delay on a monitor

    Args:
        mon_dep (str): Name of a monitor deployment

    """
    logger.info(f"Updating initialDelaySeconds on deployment: {mon_dep}")
    cmd = (
        f"oc get deployment {mon_dep} -o yaml | "
        f'sed "s/initialDelaySeconds: 10/initialDelaySeconds: 10000/g" | oc replace -f - '
    )
    logger.info(f"Executing {cmd}")
    os.system(cmd)


def update_mon_initial_delay():
    """
    Inserts delay on all monitors

    """
    mon_dep = get_deployments_having_label(
        label=constants.MON_APP_LABEL,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    mon_deployments = [OCS(**mon) for mon in mon_dep]
    for mon in mon_deployments:
        logger.info(f"Updating initialDelaySeconds on {mon.name} deployment")
        insert_delay(mon_dep=mon.name)

    logger.info("Sleeping for mons to get initialized")
    time.sleep(60)
    logger.info("Validating whether all mons reached running state")
    mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    for mon in mon_pods:
        wait_for_resource_state(resource=mon, state=constants.STATUS_RUNNING)


def corrupt_ceph_monitors():
    """
    Corrupts ceph monitors by deleting store.db file

    """
    mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    for mon in mon_pods:
        logger.info(f"Corrupting monitor: {mon.name}")
        mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
        logger.info(
            _exec_cmd_on_pod(
                cmd=f"rm -rf /var/lib/ceph/mon/ceph-{mon_id}/store.db", pod_obj=mon
            )
        )
        try:
            wait_for_resource_state(resource=mon, state=constants.STATUS_CLBO)
        except ResourceWrongStatusException:
            if (
                mon.ocp.get_resource(resource_name=mon.name, column="STATUS")
                != constants.STATUS_CLBO
            ):
                logger.info(
                    f"Re-spinning monitor: {mon.name} since it did not reach CLBO state"
                )
                mon.delete()
    logging.info("Validating all the monitors are in CLBO state")
    for mon in get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
        wait_for_resource_state(resource=mon, state=constants.STATUS_CLBO)


def archive_and_mute_ceph_warn():
    """
    Archives all ceph crashes and mute warnings

    """
    toolbox = pod.get_ceph_tools_pod()
    toolbox.exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
    toolbox.exec_ceph_cmd(
        ceph_cmd="ceph config set mon auth_allow_insecure_global_id_reclaim false"
    )


def ceph_fs_recovery():
    """
    Resets the CephFS

    """
    toolbox = pod.get_ceph_tools_pod()
    try:
        toolbox.exec_cmd_on_pod(
            f"ceph fs reset {defaults.CEPHFILESYSTEM_NAME} --yes-i-really-mean-it"
        )
    except CommandFailed:
        toolbox.exec_cmd_on_pod(
            f"ceph fs new {defaults.CEPHFILESYSTEM_NAME} ocs-storagecluster-cephfilesystem-metadata "
            f"ocs-storagecluster-cephfilesystem-data0 --force"
        )
        toolbox.exec_cmd_on_pod(
            f"ceph fs reset {defaults.CEPHFILESYSTEM_NAME}  --yes-i-really-mean-it"
        )


def get_ceph_caps(secret_resource):
    """
    Gets ocs secrets and decodes it to get ceph keyring

    Args:
        secret_resource (any): secret resource name

    Returns:
        str: Auth keyring

    """
    keyring = ""

    fs_node_caps = """
        caps mds = "allow rw"
        caps mgr = "allow rw"
        caps mon = "allow r"
        caps osd = "allow rw tag cephfs *=*"
"""
    rbd_node_caps = """
        caps mgr = "allow rw"
        caps mon = "profile rbd"
        caps osd = "profile rbd"
"""
    fs_provisinor_caps = """
        caps mgr = "allow rw"
        caps mon = "allow r"
        caps osd = "allow rw tag cephfs metadata=*"
"""
    rbd_provisinor_caps = """
        caps mgr = "allow rw"
        caps mon = "profile rbd"
        caps osd = "profile rbd"
"""

    for resource in secret_resource:
        resource_obj = ocp.OCP(
            resource_name=resource,
            kind=constants.SECRET,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )
        if constants.CEPHFS_NODE_SECRET in resource:
            keyring = (
                keyring
                + base64.b64decode(resource_obj.get().get("data").get("adminKey"))
                .decode()
                .rstrip("\n")
                + "\n"
            )
            keyring = (
                "[client.csi-cephfs-node]" + "\n" + "\t" + "key = "
                f"{keyring}" + fs_node_caps
            )

        elif constants.RBD_NODE_SECRET in resource:
            keyring = (
                keyring
                + base64.b64decode(resource_obj.get().get("data").get("userKey"))
                .decode()
                .rstrip("\n")
                + "\n"
            )
            keyring = (
                "[client.csi-rbd-node]" + "\n" + "\t" + "key = "
                f"{keyring}" + rbd_node_caps
            )

        elif constants.CEPHFS_PROVISIONER_SECRET in resource:
            keyring = (
                keyring
                + base64.b64decode(resource_obj.get().get("data").get("adminKey"))
                .decode()
                .rstrip("\n")
                + "\n"
            )
            keyring = (
                "[client.csi-cephfs-provisioner]" + "\n" + "\t" + "key = "
                f"{keyring}" + fs_provisinor_caps
            )

        elif constants.RBD_PROVISIONER_SECRET in resource:
            keyring = (
                keyring
                + base64.b64decode(resource_obj.get().get("data").get("userKey"))
                .decode()
                .rstrip("\n")
                + "\n"
            )
            keyring = (
                "[client.csi-rbd-provisioner]" + "\n" + "\t" + "key = "
                f"{keyring}" + rbd_provisinor_caps
            )

        else:
            keyring = (
                keyring
                + base64.b64decode(resource_obj.get().get("data").get("keyring"))
                .decode()
                .rstrip("\n")
                + "\n"
            )
    return keyring


def generate_monmap_cmd():
    """
    Generates monmap-tool command used to rebuild monitors

    Returns:
        str: Monitor map command

    """
    mon_ips_dict = {}
    mon_ids = []
    mon_ips = []

    logger.info("Getting monitor pods public IP")
    mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    for mon in mon_pods:
        mon_ids.append(mon.get().get("metadata").get("labels").get("ceph_daemon_id"))
        logger.info(f"getting public ip of {mon.name}")
        logger.info(mon_ids)
        mon_ips.append(
            re.findall(
                r"[0-9]+(?:\.[0-9]+){3}",
                mon.get().get("spec").get("initContainers")[1].get("args")[-2],
            )
        )

    mon_a = mon_pods[0]
    logger.info(f"Working on monitor: {mon_a.name} to get FSID")
    fsid = (
        mon_a.get()
        .get("spec")
        .get("initContainers")[1]
        .get("args")[0]
        .replace("--fsid=", "")
    )

    for ids, ip in zip(mon_ids, mon_ips):
        mon_ips_dict.update({ids: f"{ip}"})

    mon_ip_ids = ""
    for key, val in mon_ips_dict.items():
        mon_ip_ids = mon_ip_ids + f"--add {key} {val}" + " "

    mon_map_cmd = f"monmaptool --create {mon_ip_ids} --enable-all-features --clobber /tmp/monmap --fsid {fsid}"
    logger.info(f"Generated monitor map creation command: {mon_map_cmd}")
    return mon_map_cmd
