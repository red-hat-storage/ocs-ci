import logging
import base64
import time
import os
from os.path import join

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    ignore_leftovers,
    tier3,
    skipif_openshift_dedicated,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import E2ETest, config
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
import tempfile
import re

from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_osd_pods,
    get_deployments_having_label,
    get_mds_pods,
)
from ocs_ci.ocs.resources import pod

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.ocp import OCP

from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


@tier3
@ignore_leftovers
@pytest.mark.polarion_id("")
@pytest.mark.bugzilla("")
@skipif_ocs_version("<4.9")
@skipif_openshift_dedicated
@skipif_external_mode
class TestMonitorRecovery(E2ETest):
    """
    Test to verify monitor recovery

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, multi_dc_pod, mcg_obj, bucket_factory):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()
        project = project_factory()

        rbd_pods = multi_dc_pod(
            num_of_pvcs=1,
            pvc_size=5,
            project=project,
            access_mode="RWX",
            pool_type="rbd",
        )

        cephfs_pods = multi_dc_pod(
            num_of_pvcs=1,
            pvc_size=5,
            project=project,
            access_mode="RWX",
            pool_type="cephfs",
        )
        curl_cmd = "dd if=/dev/urandom of=/tmp/sample_file.txt bs=4M count=3"

        pods = rbd_pods + cephfs_pods
        run_cmd(cmd=curl_cmd)
        for po in pods:
            pod.upload(
                po.name,
                "/tmp/sample_file.txt",
                "/mnt/",
                namespace=project.namespace,
            )

        logger.info("Running ios on OBCs")
        self.sanity_helpers.obc_put_obj_create_delete(mcg_obj, bucket_factory)

    def test_monitor_recovery(self):
        mon_recovery = MonitorRecovery()

        logger.info("Backing up all the deployments")
        mon_recovery.backup_deployments()
        mons_revert = mon_recovery.mon_deployments_to_revert()
        mds_revert = mon_recovery.mds_deployments_to_revert()

        logger.info("Corrupting ceph monitors by deleting store db")
        corrupt_ceph_monitors()

        logger.info("Starting the monitor recovery procedure")
        logger.info("Scaling down rook and ocs operators")
        mon_recovery.scale_rook_ocs_operators(replica=0)

        logger.info(
            "Patching OSDs to remove LivenessProbe and setting sleep to infinity"
        )
        mon_recovery.patch_sleep_on_osds()

        logger.info("Getting mon-store from OSDs")
        mon_recovery.get_monstore_from_osds()

        logger.info("Patching MONs to sleep infinitely")
        mon_recovery.patch_sleep_on_mon()

        logger.info("Updating initial delay on all monitors")
        update_mon_initial_delay()

        logger.info("Rebuilding Monitors to recover store db")
        mon_recovery.monitor_rebuild()

        logger.info("Reverting mon, osd and mgr deployments")
        mon_recovery.revert_patches(mons_revert)

        logger.info("Scaling back rook and ocs operators")
        mon_recovery.scale_rook_ocs_operators(replica=1)

        logger.info("Recovering cephfs")
        mon_recovery.scale_rook_ocs_operators(replica=0)
        mon_recovery.patch_sleep_on_mds()
        ceph_fs_recovery()
        mon_recovery.revert_patches(mds_revert)
        mon_recovery.scale_rook_ocs_operators(replica=1)
        archive_and_mute_ceph_warn()


class MonitorRecovery(object):
    """"""

    def __init__(self):
        self.backup_dir = tempfile.mkdtemp(prefix="mon-backup-")
        self.keyring_dir = tempfile.mkdtemp(dir=self.backup_dir, prefix="keyring-")
        logger.info(self.backup_dir)
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
        logger.info(f"Scaling rook-ceph-operator to replica: {replica}")
        self.dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-operator --replicas={replica}"
        )

        logger.info(f"Scaling ocs-operator to replica: {replica} ")
        self.dep_ocp.exec_oc_cmd(f"scale deployment ocs-operator --replicas={replica}")

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
            "Sleeping for 60 seconds and waiting for OSDs to reach running state"
        )
        time.sleep(60)
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
            echo "pod in loop: $osd_pod ; done deleting local dirs"
            oc exec $osd_pod -- ceph-objectstore-tool --type bluestore \
             --data-path /var/lib/ceph/osd/ceph-$(oc get $osd_pod -ojsonpath='{ .metadata.labels.ceph_daemon_id }') \
              --op update-mon-db --no-mon-config --mon-store-path $ms
            echo "Done with COT on pod: $osd_pod"
            oc cp $podname:$ms $ms
            echo "Finished pulling COT data from pod: $osd_pod"
        done
    """
        with open(f"{self.backup_dir}/recover_mon.sh", "w") as file:
            file.write(recover_mon)
        os.system(command=f"chmod +x {self.backup_dir}/recover_mon.sh")
        logger.info("Getting mon store from OSDs")
        os.system(command=f"sh {self.backup_dir}/recover_mon.sh")

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
        mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        mon_a = mon_pods[0]
        logger.info(f"Working on monitor: {mon_a.name}")

        logger.info(f"Copying mon-store into monitor: {mon_a.name}")
        self.ocp_obj.exec_oc_cmd(f"cp /tmp/monstore {mon_a.name}:/tmp/")

        logger.info("Changing ownership of mons-tore to ceph")
        mon_a.exec_cmd_on_pod(command="chown -R ceph:ceph /tmp/monstore")

        logger.info("Generating monitor map command using the IPs")
        mon_map_cmd = generate_monmap_cmd()

        logger.info("Creating monitor map")
        mon_a.exec_cmd_on_pod(command=mon_map_cmd)

        logger.info("Getting ceph keyring from ocs secrets")
        keyring_files = self.get_ceph_keyrings()

        logger.info(f"Copy keyring files to monitor: {mon_a.name}")
        for k_file in keyring_files:
            cmd = f"oc cp {k_file} {mon_a.name}:/tmp/"
            logger.info(f"Copying keyring: {k_file} into mon {mon_a.name}")
            os.system(cmd)

        logger.info(f"Importing ceph keyrings to a temporary file on: {mon_a.name}")
        mon_a.exec_cmd_on_pod(command="cp /etc/ceph/keyring-store/keyring /tmp/keyring")
        for k_file in keyring_files:
            k_file = k_file.split("/")
            logger.info(f"Importing keyring {k_file[-1]}")
            mon_a.exec_cmd_on_pod(
                command=f"ceph-authtool  /tmp/keyring  --import-keyring /tmp/{k_file[-1]}"
            )

        rebuild_mon_cmd = "ceph-monstore-tool /tmp/monstore rebuild -- --keyring /tmp/keyring --monmap /tmp/monmap"
        logger.info("Running command to rebuild monitor")
        mon_a.exec_cmd_on_pod(command=rebuild_mon_cmd, out_yaml_format=False)

        logger.info(f"Copying store.db directory on monitor: {mon_a.name} locally")
        self.ocp_obj.exec_oc_cmd(
            f"cp {mon_a.name}:/tmp/monstore/store.db {self.backup_dir}/store.db"
        )

        logger.info("Copying store.db to rest of the monitors")
        for mon in get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
            cmd = (
                f"cp {self.backup_dir}/store.db {mon.name}:/var/lib/ceph/mon/ceph-"
                f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}/ "
            )
            logger.info(f"Copying store.db to monitor: {mon.name}")
            self.ocp_obj.exec_oc_cmd(cmd)
            # os.system(cmd)
            logger.info("Changing ownership of store.db to ceph:ceph")
            mon.exec_cmd_on_pod(
                command=f"chown -R ceph:ceph /var/lib/ceph/mon/ceph-"
                f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}/store.db"
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
            # os.system(revert_patch)
        logger.info("Sleeping and waiting for all pods up and running..")
        time.sleep(120)
        assert pod.wait_for_pods_to_be_running(
            timeout=600
        ), "Pods did not reach running state"

    def backup_deployments(self):
        """
        Creates a backup of all deployments in the `openshift-storage` namespace

        Returns:
            str: backup directory path

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
        Gets all deployments to be reverted post monitor recovery procedure

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
            logger.info(dep)
            to_revert_patches_path.append(
                join(self.backup_dir, dep["metadata"]["name"] + ".yaml")
            )
        logger.info(to_revert_patches_path)

        return to_revert_patches_path

    def get_ceph_keyrings(self):
        """
        Gets all ceph and csi related keyring from OCS secrets

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
        fs_node_k = get_ceph_caps(["rook-csi-cephfs-node"])
        rbd_node_k = get_ceph_caps(["rook-csi-rbd-node"])
        fs_provisinor_k = get_ceph_caps(["rook-csi-cephfs-provisioner"])
        rbd_provisinor_k = get_ceph_caps(["rook-csi-rbd-provisioner"])

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
        """"""
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


def corrupt_ceph_monitors():
    """
    Corrupts ceph monitors by deleting store.db file

    """
    mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    for mon in mon_pods:
        logger.info(f"Corrupting monitor: {mon.name}")
        mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
        logger.info(
            mon.exec_cmd_on_pod(
                command=f"rm -rf /var/lib/ceph/mon/ceph-{mon_id}/store.db"
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
    toolbox = pod.get_ceph_tools_pod()
    toolbox.exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
    toolbox.exec_ceph_cmd(
        ceph_cmd="ceph config set mon auth_allow_insecure_global_id_reclaim false"
    )


def ceph_fs_recovery():
    toolbox = pod.get_ceph_tools_pod()
    try:
        toolbox.exec_cmd_on_pod(
            "ceph fs reset ocs-storagecluster-cephfilesystem  --yes-i-really-mean-it"
        )
    except CommandFailed:
        toolbox.exec_cmd_on_pod(
            "ceph fs new ocs-storagecluster-cephfilesystem"
            " ocs-storagecluster-cephfilesystem-metadata ocs-storagecluster-cephfilesystem-data0 --force"
        )
        toolbox.exec_cmd_on_pod(
            "ceph fs reset ocs-storagecluster-cephfilesystem  --yes-i-really-mean-it"
        )


def insert_delay(mon_dep):
    """
    Inserts delay on a monitor

    Args:
        mon_dep (str): Name of a monitor deployment

    """
    logger.info(f"Updating initialDelaySeconds on deployment: {mon_dep}")
    cmd = f"""oc get deployment {mon_dep} -o yaml |
     sed "s/initialDelaySeconds: 10/initialDelaySeconds: 10000/g" | oc replace -f - """
    logger.info(f"Executing {cmd}")
    os.system(cmd)


def get_ceph_caps(secret_resource):
    """
    Gets ocs secrets and decodes it to get ceph keyring

    Args:
        secret_resource (any): secret resource name

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
            resource_name=resource, kind="Secret", namespace="openshift-storage"
        )
        if "rook-csi-cephfs-node" in resource:
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

        elif "rook-csi-rbd-node" in resource:
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

        elif "rook-csi-cephfs-provisioner" in resource:
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

        elif "rook-csi-rbd-provisioner" in resource:
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


def generate_monmap_cmd():
    """
    Generates monmap-tool command used to rebuild monitors

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
