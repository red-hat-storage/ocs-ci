import collections
import logging
import time
import os
from os.path import join
import tempfile
import re

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    ignore_leftovers,
    skipif_openshift_dedicated,
    skipif_external_mode,
    system_test,
    skipif_ocp_version,
    magenta_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.ocp import OCP, switch_to_project
from ocs_ci.framework.testlib import E2ETest, config
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    ResourceNotFoundError,
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_osd_pods,
    get_deployments_having_label,
    get_mds_pods,
    get_cephfsplugin_provisioner_pods,
    get_rbdfsplugin_provisioner_pods,
    get_rgw_pods,
    get_noobaa_pods,
    get_plugin_pods,
    get_ceph_tools_pod,
    wait_for_storage_pods,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import ocp, constants, defaults, bucket_utils
from ocs_ci.helpers.helpers import wait_for_resource_state, get_secret_names
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
@pytest.mark.order("last")
@pytest.mark.polarion_id("OCS-3911")
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.10")
@skipif_openshift_dedicated
@skipif_external_mode
class TestMonitorRecovery(E2ETest):
    """
    Test to verify monitor recovery

    """

    @pytest.fixture(autouse=True)
    def mon_recovery_setup(
        self,
        deployment_pod_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Creates project, pvcs, dc-pods and obcs

        """
        self.filename = "sample_file.txt"
        self.object_key = "obj-key"
        self.object_data = "string data"
        self.dd_cmd = f"dd if=/dev/urandom of=/mnt/{self.filename} bs=5M count=1"

        self.sanity_helpers = Sanity()
        # Create project, pvc, dc pods
        self.dc_pods = []
        self.dc_pods.append(
            deployment_pod_factory(
                interface=constants.CEPHBLOCKPOOL,
            )
        )
        self.dc_pods.append(
            deployment_pod_factory(
                interface=constants.CEPHFILESYSTEM,
                access_mode=constants.ACCESS_MODE_RWX,
            )
        )
        self.md5sum = []
        for pod_obj in self.dc_pods:
            pod_obj.exec_cmd_on_pod(command=self.dd_cmd)
            # Calculate md5sum
            self.md5sum.append(pod.cal_md5sum(pod_obj, self.filename))
        logger.info(f"Md5sum calculated before recovery: {self.md5sum}")

        self.bucket_name = bucket_factory(interface="OC")[0].name
        logger.info(f"Putting object on: {self.bucket_name}")
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj,
            bucketname=self.bucket_name,
            object_key=self.object_key,
            data=self.object_data,
        ), "Failed: PutObject"

    def test_monitor_recovery(
        self,
        deployment_pod_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Verifies Monitor recovery procedure as per:
        https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.8/html/troubleshooting_openshift_container_storage/restoring-the-monitor-pods-in-openshift-container-storage_rhocs

        """
        # Initialize mon recovery class
        mon_recovery = MonitorRecovery()

        logger.info("Corrupting ceph monitors by deleting store.db")
        corrupt_ceph_monitors()

        logger.info("Starting the monitor recovery procedure")
        logger.info("Scaling down rook-ceph-operator and ocs operator deployments")
        mon_recovery.scale_rook_ocs_operators(replica=0)

        logger.info("Backing up all the deployments in openshift-storage namespace")
        mon_recovery.backup_deployments()
        dep_revert, mds_revert = mon_recovery.deployments_to_revert()

        logger.info(
            "Patching OSD deployments to remove LivenessProbe and sleep to infinity"
        )
        mon_recovery.patch_sleep_on_osds()

        switch_to_project(config.ENV_DATA["cluster_namespace"])
        logger.info("Copy tar to the OSDs")
        mon_recovery.copy_tar_to_pods(pod_type="osd")

        logger.info("Preparing the recover_mon.sh script")
        mon_recovery.prepare_monstore_script()

        logger.info("Getting mon-store from OSDs by running recover_mon.sh script")
        mon_recovery.run_mon_store()

        logger.info("Patching MONs to sleep infinitely")
        mon_recovery.patch_sleep_on_mon()

        logger.info("Updating initial delay on all monitors")
        update_mon_initial_delay()

        logger.info("Copy tar to the MONs")
        mon_recovery.copy_tar_to_pods(pod_type="mon")

        logger.info("Copy the previously retrieved monstore to the mon-a pod")
        mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
        mon_a = mon_pods[0]
        logger.info(f"Copying mon-store into monitor: {mon_a.name}")
        ocp_obj = MonitorRecovery()
        ocp_obj._exec_oc_cmd(cmd=f"cp /tmp/monstore {mon_a.name}:/tmp/")

        logger.info(
            "Getting into the mon-a pod and changing the ownership of the retrieved monstore"
        )
        _exec_cmd_on_pod(cmd="chown -R ceph:ceph /tmp/monstore", pod_obj=mon_a)

        logger.info(
            "Getting the keyrings of all the Ceph daemons (OSD, MGR, MDS and RGW) from their respective secrets"
        )
        file_path = mon_recovery.get_ceph_daemons_keyrings()

        logger.info("Copy the ceph daemons key ring to mon-a")
        mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
        mon_a = mon_pods[0]
        logger.info(
            f"Copying the mon keyring stored locally from {file_path} to monitor: {mon_a.name}"
        )
        ocp_obj._exec_oc_cmd(cmd=f"cp {file_path} {mon_a.name}:/tmp/keyring")

        logger.info("Generating monitor map command using the IPs")
        mon_map_cmd = generate_monmap_cmd()

        logger.info("Rebuilding Monitors to recover store db")
        mon_recovery.monitor_rebuild(mon_map_cmd)

        logger.info("Reverting mon, osd and mgr deployments")
        mon_recovery.revert_patches(dep_revert)

        logger.info("Scaling back rook and ocs operators")
        mon_recovery.scale_rook_ocs_operators(replica=1)

        logger.info("Recovering CephFS")
        mon_recovery.scale_rook_ocs_operators(replica=0)
        logger.info(
            "Patching MDSs to remove LivenessProbe and setting sleep to infinity"
        )
        mon_recovery.patch_sleep_on_mds()
        logger.info("Resetting the fs")
        ceph_fs_recovery()
        logger.info("Reverting MDS deployments")
        mon_recovery.revert_patches(mds_revert)
        logger.info("Scaling back rook and ocs operators")
        mon_recovery.scale_rook_ocs_operators(replica=1)
        logger.info("Recovering mcg by re-spinning the pods")
        recover_mcg()
        remove_global_id_reclaim()
        for pod_obj in self.dc_pods:
            pod_obj.delete(force=True)
        new_md5_sum = []
        logger.info("Verifying md5sum of files after recovery")
        for pod_obj in get_spun_dc_pods(self.dc_pods):
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_obj.name,
                timeout=600,
                sleep=10,
            )
            new_md5_sum.append(pod.cal_md5sum(pod_obj, self.filename))
        logger.info(f"Md5sum calculated after recovery: {new_md5_sum}")
        if collections.Counter(new_md5_sum) == collections.Counter(self.md5sum):
            logger.info(
                f"Verified: md5sum of {self.filename} on pods matches with the original md5sum"
            )
        else:
            assert False, f"Data corruption found {new_md5_sum} and {self.md5sum}"
        logger.info("Getting object after recovery")
        assert bucket_utils.s3_get_object(
            s3_obj=mcg_obj,
            bucketname=self.bucket_name,
            object_key=self.object_key,
        ), "Failed: GetObject"

        # New pvc, dc pods, obcs
        new_dc_pods = [
            deployment_pod_factory(
                interface=constants.CEPHBLOCKPOOL,
            ),
            deployment_pod_factory(
                interface=constants.CEPHFILESYSTEM,
            ),
        ]
        for pod_obj in new_dc_pods:
            pod_obj.exec_cmd_on_pod(command=self.dd_cmd)
        logger.info("Creating new bucket and write object")
        new_bucket = bucket_factory(interface="OC")[0].name
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj,
            bucketname=new_bucket,
            object_key=self.object_key,
            data=self.object_data,
        ), "Failed: PutObject"
        wait_for_storage_pods()
        logger.info("Archiving the ceph crash warnings")
        tool_pod = get_ceph_tools_pod()
        tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
        self.sanity_helpers.health_check(tries=10)


class MonitorRecovery(object):
    """
    Monitor recovery class

    """

    def __init__(self):
        """
        Initializer

        """
        self.backup_dir = tempfile.mkdtemp(prefix="mon-backup-")
        self.keyring_dir = tempfile.mkdtemp(dir=self.backup_dir, prefix="keyring")
        self.dep_ocp = OCP(
            kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.ocp_obj = ocp.OCP(namespace=config.ENV_DATA["cluster_namespace"])

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
        if replica == 1:
            logger.info("Sleeping for 150 secs for cluster to stabilize")
            time.sleep(150)

    def patch_sleep_on_osds(self):
        """
        Patch the OSD deployments to sleep and remove the `livenessProbe` parameter,

        """
        osd_dep = get_deployments_having_label(
            label=constants.OSD_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
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

    def copy_tar_to_pods(self, pod_type="osd"):
        """
        Copies local tar binary to the specified type of pods (OSD or MON) pod
        using cat

        Args:
            pod_type (str): Type of pod ("osd" or "mon"). Defaults to "osd".

        Raises:
            ValueError: If the pod_type is neither "osd" nor "mon".

        """
        if pod_type == "osd":
            pod_objs = get_osd_pods()
        elif pod_type == "mon":
            pod_objs = get_mon_pods()
        else:
            raise ValueError(f"Invalid pod type: {pod_type}. Use 'osd' or 'mon' ")
        for pod_obj in pod_objs:
            logger.info(f"Copying tar binary to pod: {pod_obj.name}")
            cmd = (
                f"cat /usr/bin/tar | oc exec -i {pod_obj.name} -n {constants.OPENSHIFT_STORAGE_NAMESPACE} -- bash -c "
                f"'cat > /usr/bin/tar'"
            )
            self._exec_oc_cmd(cmd)
            logger.info(
                f"Setting execute permissions on /usr/bin/tar in pod: {pod_obj.name}"
            )
            cmd = "chmod +x /usr/bin/tar"
            _exec_cmd_on_pod(cmd=cmd, pod_obj=pod_obj)

    def prepare_monstore_script(self):
        """
        Prepares the script to retrieve the `monstore` cluster map from OSDs

        """
        recover_mon = """
        #!/bin/bash
        ms=/tmp/monstore

        rm -rf $ms
        mkdir $ms

        for osd_pod in $(oc get po -l app=rook-ceph-osd -oname -n openshift-storage); do

            echo "Starting with pod: $osd_pod"

            podname=$(echo $osd_pod|sed 's/pod\\///g')
            oc exec $osd_pod -- rm -rf $ms
            oc exec $osd_pod -- mkdir $ms
            oc cp $ms $podname:$ms

            rm -rf $ms
            mkdir $ms

            echo "pod in loop: $osd_pod ; done deleting local dirs"

            oc exec $osd_pod -- ceph-objectstore-tool --type bluestore --data-path \\
            /var/lib/ceph/osd/ceph-$(oc get $osd_pod -ojsonpath='{ .metadata.labels.ceph_daemon_id }') \\
            --op update-mon-db --no-mon-config --mon-store-path $ms
            echo "Done with COT on pod: $osd_pod"

            oc cp $podname:$ms $ms

            echo "Finished pulling COT data from pod: $osd_pod"
        done
        """

        with open(f"{self.backup_dir}/recover_mon.sh", "w") as file:
            file.write(recover_mon)
        exec_cmd(cmd=f"chmod +x {self.backup_dir}/recover_mon.sh")

    @retry(CommandFailed, tries=15, delay=5, backoff=1)
    def run_mon_store(self):
        """
        Runs script to get the mon store from OSDs

        Raise:
            CommandFailed
        """
        logger.info("Running mon-store script..")
        result = exec_cmd(cmd=f"sh {self.backup_dir}/recover_mon.sh")
        result.stdout = result.stdout.decode()
        logger.info(f"OSD mon store retrieval stdout {result.stdout}")
        result.stderr = result.stderr.decode()
        logger.info(f"OSD mon store retrieval stderr {result.stderr}")
        search_pattern = re.search(
            pattern="error|unable to open mon store", string=result.stderr
        )
        if search_pattern:
            logger.info(f"Error found: {search_pattern}")
            raise CommandFailed
        logger.info("Successfully collected mon store from OSDs")

    def patch_sleep_on_mon(self):
        """
        Patches sleep to infinity on monitors

        """
        mon_dep = get_deployments_having_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
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

    def monitor_rebuild(self, mon_map_cmd):
        """
        Rebuilds the monitor

        Args:
            mon_map_cmd (str): mon-store tool command

        """
        mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
        mon_a = mon_pods[0]
        logger.info(f"Working on monitor: {mon_a.name}")

        rebuild_mon_cmd = "ceph-monstore-tool /tmp/monstore rebuild -- --keyring /tmp/keyring --monmap /tmp/monmap"
        logger.info("Running command to rebuild monitor")
        mon_a.exec_cmd_on_pod(command=rebuild_mon_cmd, out_yaml_format=False)

        logger.info("Changing ownership of monstore to ceph")
        _exec_cmd_on_pod(cmd="chown -R ceph:ceph /tmp/monstore", pod_obj=mon_a)

        logger.info("Copy the rebuild store.db file to the monstore directory")
        _exec_cmd_on_pod(
            cmd="mv /tmp/monstore/store.db /var/lib/ceph/mon/ceph-a/store.db",
            pod_obj=mon_a,
        )
        logger.info("Changing ownership of monstore to ceph")
        _exec_cmd_on_pod(
            cmd="chown -R ceph:ceph /var/lib/ceph/mon/ceph-a/store.db", pod_obj=mon_a
        )
        logger.info(
            f"Copying store.db directory from monitor: {mon_a.name} to {self.backup_dir}"
        )
        self._exec_oc_cmd(
            cmd=f"cp {mon_a.name}:/var/lib/ceph/mon/ceph-a/store.db {self.backup_dir}/"
        )
        logger.info("Copying store.db to rest of the monitors")
        for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"]):
            if not mon.get().get("metadata").get("labels").get("ceph_daemon_id") == "a":
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

    def deployments_to_revert(self):
        """
        Gets mon, osd and mgr deployments to revert

        Returns:
            tuple: deployment paths to be reverted

        """
        to_revert_patches = (
            get_deployments_having_label(
                label=constants.OSD_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            + get_deployments_having_label(
                label=constants.MON_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            + get_deployments_having_label(
                label=constants.MGR_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
        )
        to_revert_mds = get_deployments_having_label(
            label=constants.MDS_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        to_revert_patches_path = []
        to_revert_mds_path = []
        for dep in to_revert_patches:
            to_revert_patches_path.append(
                join(self.backup_dir, dep["metadata"]["name"] + ".yaml")
            )
        for dep in to_revert_mds:
            logger.info(dep)
            to_revert_mds_path.append(
                join(self.backup_dir, dep["metadata"]["name"] + ".yaml")
            )
        return to_revert_patches_path, to_revert_mds_path

    def get_all_keyring_secrets(self):
        """
        Get all the keyring secrets

        Returns:
            list: A list of keyring secrets

        """
        all_secrets = get_secret_names()
        keyring_secrets = [keyring for keyring in all_secrets if "keyring" in keyring]
        return keyring_secrets

    def get_ceph_daemons_keyrings(self):
        """
        Gets all ceph and csi related keyring from OCS secrets

        Returns:
            file: A formatted file with ceph daemons keyrings

        """
        logger.info("Getting the daemons keyrings...")
        all_keyring_secrets = self.get_all_keyring_secrets()
        formatted_data = []
        for keyring_secret in all_keyring_secrets:
            cmd = f"oc get secret {keyring_secret} -ojson  | jq .data.keyring | xargs echo | base64 -d"
            out = exec_cmd(cmd=cmd, shell=True)
            out_str = out.stdout.decode("utf-8")
            tmp_lines = out_str.strip().splitlines()
            keyring_data = [line.replace("\t", "").strip() for line in tmp_lines]
            pod_name = keyring_data[0].strip()
            formatted_data.append(f"{pod_name}:")
            for block in keyring_data:
                if block == "[client.admin]" and "[mon.]" in keyring_data:
                    logger.info(
                        "Skipping adding the [client.admin] details present in rook-ceph-mons-keyring"
                        "as the secret details are already fecthed with rook-ceph-admin-keyring"
                    )
                    break
                key = None
                caps = []
                if block.startswith("key ="):
                    key = block.split(" = ")[1].strip()
                elif "caps" in block:
                    caps.append(block.strip())
                if key:
                    logger.info(f"Found key :{key}")
                    formatted_data.append(f"    key = {key}")
                for cap in caps:
                    logger.info(f"Found cap :{cap}")
                    formatted_data.append(f"    {cap}")
        with open(f"{self.keyring_dir}/keyring-mon-a", "w") as f:
            f.write("\n".join(formatted_data))
            logger.info(f"Output saved to {self.keyring_dir}/keyring-mon-a")

        logger.info("Getting the OSDs keys")
        osd_pods = get_osd_pods()
        for osd_pod in osd_pods:
            osd_id = osd_pod.get().get("metadata").get("labels").get("ceph-osd-id")
            cmd = f"oc exec -i {osd_pod.name} -- bash -c 'cat /var/lib/ceph/osd/ceph-{osd_id}/keyring' "
            out = exec_cmd(cmd=cmd, shell=True)
            out_osd_str = out.stdout.decode("utf-8")
            lines = out_osd_str.strip().splitlines()
            osd_keyring_data = [line.replace("\t", "").strip() for line in lines]
            pod_name = osd_keyring_data[0].strip()
            formatted_data.append(f"{pod_name}:")
            key = None
            for block in osd_keyring_data:
                if block.startswith("key ="):
                    key = block.split(" = ")[1].strip()
                if key:
                    formatted_data.append(f"    key = {key}")
                    formatted_data.append('    caps mgr = "allow profile osd"')
                    formatted_data.append('    caps mon = "allow profile osd"')
                    formatted_data.append('    caps osd = "allow *"')

        with open(f"{self.keyring_dir}/keyring-mon-a", "w") as f:
            f.write("\n".join(formatted_data) + "\n")
        logger.info(f"Keyring data saved to {self.keyring_dir}/keyring-mon-a")

        return f"{self.keyring_dir}/keyring-mon-a"

    def patch_sleep_on_mds(self):
        """
        Patch the OSD deployments to sleep and remove the `livenessProbe` parameter,

        """
        mds_dep = get_deployments_having_label(
            label=constants.MDS_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
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
        for mds in get_mds_pods(namespace=config.ENV_DATA["cluster_namespace"]):
            wait_for_resource_state(resource=mds, state=constants.STATUS_RUNNING)

    @retry(CommandFailed, tries=10, delay=10, backoff=1)
    def _exec_oc_cmd(self, cmd, out_yaml_format=True):
        """
        Exec oc cmd with retry

        Args:
            cmd (str): Command

        """
        self.ocp_obj.exec_oc_cmd(cmd, out_yaml_format=out_yaml_format)


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
    kubeconfig = config.RUN.get("kubeconfig")
    cmd = (
        f"oc get --kubeconfig {kubeconfig} deployment {mon_dep} -o yaml | "
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
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    mon_deployments = [OCS(**mon) for mon in mon_dep]
    for mon in mon_deployments:
        logger.info(f"Updating initialDelaySeconds on {mon.name} deployment")
        insert_delay(mon_dep=mon.name)

    logger.info("Sleeping for mons to get initialized")
    time.sleep(90)
    logger.info("Validating whether all mons reached running state")
    validate_mon_pods()


@retry(
    (ResourceWrongStatusException, ResourceNotFoundError), tries=10, delay=5, backoff=1
)
def validate_mon_pods():
    """
    Checks mon pods are running with retries

    """
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    for mon in mon_pods:
        wait_for_resource_state(resource=mon, state=constants.STATUS_RUNNING)


def corrupt_ceph_monitors():
    """
    Corrupts ceph monitors by deleting store.db file

    """
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    for mon in mon_pods:
        logger.info(f"Corrupting monitor: {mon.name}")
        mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
        _exec_cmd_on_pod(
            cmd=f"rm -rf /var/lib/ceph/mon/ceph-{mon_id}/store.db", pod_obj=mon
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
    logger.info("Validating all the monitors are in CLBO state")
    for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"]):
        wait_for_resource_state(resource=mon, state=constants.STATUS_CLBO)


def recover_mcg():
    """
    Recovery procedure for noobaa by re-spinning the pods after mon recovery

    """
    logger.info("Re-spinning noobaa pods")
    for noobaa_pod in get_noobaa_pods():
        noobaa_pod.delete()
    for noobaa_pod in get_noobaa_pods():
        wait_for_resource_state(
            resource=noobaa_pod, state=constants.STATUS_RUNNING, timeout=600
        )
    if config.ENV_DATA["platform"].lower() in constants.ON_PREM_PLATFORMS:
        logger.info("Re-spinning RGW pods")
        for rgw_pod in get_rgw_pods():
            rgw_pod.delete()
        for rgw_pod in get_rgw_pods():
            wait_for_resource_state(
                resource=rgw_pod, state=constants.STATUS_RUNNING, timeout=600
            )


def remove_global_id_reclaim():
    """
    Removes global id warning by re-spinning client and mon pods

    """
    csi_pods = []
    interfaces = [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]
    for interface in interfaces:
        plugin_pods = get_plugin_pods(interface)
        csi_pods += plugin_pods

    cephfs_provisioner_pods = get_cephfsplugin_provisioner_pods()
    rbd_provisioner_pods = get_rbdfsplugin_provisioner_pods()

    csi_pods += cephfs_provisioner_pods
    csi_pods += rbd_provisioner_pods
    for csi_pod in csi_pods:
        csi_pod.delete()
    for mds_pod in get_mds_pods():
        mds_pod.delete()
    for mds_pod in get_mds_pods():
        wait_for_resource_state(resource=mds_pod, state=constants.STATUS_RUNNING)
    for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"]):
        mon.delete()
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    for mon in mon_pods:
        wait_for_resource_state(resource=mon, state=constants.STATUS_RUNNING)


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


def get_spun_dc_pods(pod_list):
    """
    Fetches info about the re-spun dc pods

    Args:
        pod_list (list): list of previous pod objects

    Returns:
        list : list of respun pod objects

    """
    new_pods = []
    for pod_obj in pod_list:
        pod_label = pod_obj.labels.get("deploymentconfig")
        label_selector = f"deploymentconfig={pod_label}"

        pods_data = pod.get_pods_having_label(label_selector, pod_obj.namespace)
        for pod_data in pods_data:
            pod_name = pod_data.get("metadata").get("name")
            if "-deploy" not in pod_name and pod_name not in pod_obj.name:
                new_pods.append(pod.get_pod_obj(pod_name, pod_obj.namespace))
    logger.info(f"Previous pods: {[pod_obj.name for pod_obj in pod_list]}")
    logger.info(f"Re-spun pods: {[pod_obj.name for pod_obj in new_pods]}")
    return new_pods


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
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
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
