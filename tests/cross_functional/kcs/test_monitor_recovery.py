import collections
import logging
import time
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
    get_mgr_pods,
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
from ocs_ci.utility.utils import exec_cmd, run_cmd

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
        request,
        deployment_pod_factory,
        mcg_obj,
        bucket_factory,
    ):
        """
        Creates project, pvcs, dc-pods and obcs

        """

        def finalizer():
            """
            Teardown: Force delete pods and clean up volume attachments
            """
            logger.test_step("Teardown: Clean up test resources")
            logger.info("Force deleting test pods to release PVCs")
            for dc_pod in self.dc_pods:
                try:
                    logger.debug(f"Force deleting pod: {dc_pod.name}")
                    dc_pod.delete(force=True, wait=False)
                except Exception as e:
                    logger.warning(f"Failed to delete pod {dc_pod.name}: {e}")

            logger.info("Waiting 30s for pods to terminate")
            time.sleep(30)

            logger.info("Cleaning up stale volume attachments")
            try:
                va_ocp = OCP(kind="VolumeAttachment", namespace="")
                attachments = va_ocp.get()
                if attachments and "items" in attachments:
                    deleted_count = 0
                    for attachment in attachments["items"]:
                        if not attachment.get("status", {}).get("attached", False):
                            va_name = attachment["metadata"]["name"]
                            logger.debug(
                                f"Deleting unattached VolumeAttachment: {va_name}"
                            )
                            try:
                                va_ocp.delete(resource_name=va_name)
                                deleted_count += 1
                            except Exception as e:
                                logger.warning(
                                    f"Failed to delete VolumeAttachment {va_name}: {e}"
                                )
                    if deleted_count > 0:
                        logger.info(f"Deleted {deleted_count} stale volume attachments")
                    else:
                        logger.debug("No stale volume attachments found")
            except Exception:
                logger.exception("Error during volume attachment cleanup")

        request.addfinalizer(finalizer)

        self.filename = "sample_file.txt"
        self.object_key = "obj-key"
        self.object_data = "string data"
        self.dd_cmd = f"dd if=/dev/urandom of=/mnt/{self.filename} bs=5M count=1"

        self.sanity_helpers = Sanity()

        logger.test_step("Create test deployment pods with PVCs")
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
        logger.info(f"Created {len(self.dc_pods)} deployment pods")

        logger.test_step("Write test data and calculate checksums")
        self.md5sum = []
        for pod_obj in self.dc_pods:
            pod_obj.exec_cmd_on_pod(command=self.dd_cmd)
            checksum = pod.cal_md5sum(pod_obj, self.filename)
            self.md5sum.append(checksum)
            logger.info(f"Pod {pod_obj.name}: checksum={checksum}")
        logger.info(f"Checksums before recovery: {self.md5sum}")

        logger.test_step("Create test bucket and upload object")
        self.bucket_name = bucket_factory(interface="OC")[0].name
        logger.info(f"Created bucket: {self.bucket_name}")
        logger.assertion(
            f"S3 PutObject: bucket={self.bucket_name}, key={self.object_key}"
        )
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
        mon_recovery = MonitorRecovery()
        logger.info(
            f"Monitor recovery initialized with backup dir: {mon_recovery.backup_dir}"
        )

        logger.test_step("Corrupt ceph monitors by deleting store.db")
        corrupt_ceph_monitors()

        logger.test_step("Scale down rook-ceph-operator and ocs-operator")
        mon_recovery.scale_rook_ocs_operators(replica=0)

        logger.test_step("Backup all deployments in openshift-storage namespace")
        mon_recovery.backup_deployments()
        dep_revert, mds_revert = mon_recovery.deployments_to_revert()
        logger.info(
            f"Identified {len(dep_revert)} deployments and {len(mds_revert)} MDS deployments to revert"
        )

        logger.test_step(
            "Patch OSD deployments to remove LivenessProbe and sleep to infinity"
        )
        mon_recovery.patch_sleep_on_osds()

        switch_to_project(config.ENV_DATA["cluster_namespace"])
        logger.test_step("Copy tar binary to OSD pods")
        mon_recovery.copy_tar_to_pods(pod_type="osd")

        logger.test_step("Prepare the recover_mon.sh script")
        mon_recovery.prepare_monstore_script()

        logger.test_step("Retrieve mon-store from OSDs using recover_mon.sh script")
        mon_recovery.run_mon_store()

        logger.test_step("Patch monitor deployments to sleep infinitely")
        mon_recovery.patch_sleep_on_mon()

        logger.test_step("Update initial delay on all monitors")
        update_mon_initial_delay()

        logger.test_step("Copy tar binary to monitor pods")
        mon_recovery.copy_tar_to_pods(pod_type="mon")

        logger.test_step("Copy retrieved monstore to mon-a pod")
        mon_a = next(
            mon
            for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
            if mon.get().get("metadata", {}).get("labels", {}).get("ceph_daemon_id")
            == "a"
        )
        logger.info(f"Copying mon-store to monitor: {mon_a.name}")
        ocp_obj = MonitorRecovery()
        ocp_obj._exec_oc_cmd(
            cmd=f"cp /tmp/monstore {constants.OPENSHIFT_STORAGE_NAMESPACE}/{mon_a.name}:/tmp/"
        )

        logger.info("Changing ownership of retrieved monstore to ceph:ceph")
        _exec_cmd_on_pod(cmd="chown -R ceph:ceph /tmp/monstore", pod_obj=mon_a)

        logger.test_step("Extract keyrings from Ceph daemon secrets")
        file_path = mon_recovery.get_ceph_daemons_keyrings()
        logger.info(f"Keyrings extracted to: {file_path}")

        logger.test_step("Copy ceph daemon keyrings to mon-a pod")
        mon_a = next(
            mon
            for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
            if mon.get().get("metadata", {}).get("labels", {}).get("ceph_daemon_id")
            == "a"
        )
        logger.info(f"Copying keyring from {file_path} to monitor: {mon_a.name}")
        ocp_obj._exec_oc_cmd(
            cmd=f"cp {file_path} {constants.OPENSHIFT_STORAGE_NAMESPACE}/{mon_a.name}:/tmp/keyring"
        )

        logger.test_step("Generate monitor map command using monitor IPs")
        mon_map_cmd = generate_monmap_cmd()
        logger.debug(f"Generated monmap command: {mon_map_cmd}")

        logger.test_step("Rebuild monitors to recover store.db")
        mon_recovery.monitor_rebuild(mon_map_cmd)

        logger.test_step("Revert patches on mon, osd and mgr deployments")
        mon_recovery.revert_patches(dep_revert)

        logger.test_step("Scale up rook-ceph-operator and ocs-operator")
        mon_recovery.scale_rook_ocs_operators(replica=1)

        logger.test_step("Recover CephFS filesystem")
        mon_recovery.scale_rook_ocs_operators(replica=0)
        logger.info(
            "Patching MDS deployments to remove LivenessProbe and sleep to infinity"
        )
        mon_recovery.patch_sleep_on_mds()
        logger.info("Resetting CephFS")
        ceph_fs_recovery()
        logger.info("Scaling back rook and ocs operators after CephFS recovery")
        mon_recovery.scale_rook_ocs_operators(replica=1)

        logger.test_step("Recover MCG by re-spinning noobaa pods")
        recover_mcg()

        logger.test_step("Remove global ID reclaim warnings")
        remove_global_id_reclaim()
        logger.test_step("Verify data integrity after recovery")
        for pod_obj in self.dc_pods:
            logger.debug(f"Force deleting pod: {pod_obj.name}")
            pod_obj.delete(force=True)

        new_md5_sum = []
        logger.info("Waiting for pods to respawn and calculating checksums")
        for pod_obj in get_spun_dc_pods(self.dc_pods):
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_obj.name,
                timeout=600,
                sleep=10,
            )
            checksum = pod.cal_md5sum(pod_obj, self.filename)
            new_md5_sum.append(checksum)
            logger.info(f"Pod {pod_obj.name}: checksum={checksum}")

        logger.info(f"Checksums after recovery: {new_md5_sum}")
        logger.assertion(
            f"Data integrity check: original={self.md5sum}, "
            f"after_recovery={new_md5_sum}, "
            f"match={collections.Counter(new_md5_sum) == collections.Counter(self.md5sum)}"
        )
        if collections.Counter(new_md5_sum) == collections.Counter(self.md5sum):
            logger.info(f"Data integrity verified: checksums match for {self.filename}")
        else:
            pytest.fail(
                f"Data corruption detected: before={self.md5sum}, after={new_md5_sum}"
            )

        logger.test_step("Verify S3 object retrieval after recovery")
        logger.assertion(
            f"S3 GetObject: bucket={self.bucket_name}, key={self.object_key}"
        )
        assert bucket_utils.s3_get_object(
            s3_obj=mcg_obj,
            bucketname=self.bucket_name,
            object_key=self.object_key,
        ), "Failed: GetObject"
        logger.info("S3 object retrieved successfully after recovery")

        logger.test_step("Create new resources to verify cluster functionality")
        logger.info("Creating new deployment pods with PVCs")
        new_dc_pods = [
            deployment_pod_factory(
                interface=constants.CEPHBLOCKPOOL,
            ),
            deployment_pod_factory(
                interface=constants.CEPHFILESYSTEM,
            ),
        ]
        for pod_obj in new_dc_pods:
            logger.debug(f"Writing test data to pod: {pod_obj.name}")
            pod_obj.exec_cmd_on_pod(command=self.dd_cmd)
        logger.info(
            f"Successfully created and wrote data to {len(new_dc_pods)} new pods"
        )

        logger.info("Creating new bucket and uploading object")
        new_bucket = bucket_factory(interface="OC")[0].name
        logger.assertion(
            f"S3 PutObject to new bucket: bucket={new_bucket}, key={self.object_key}"
        )
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj,
            bucketname=new_bucket,
            object_key=self.object_key,
            data=self.object_data,
        ), "Failed: PutObject to new bucket"
        logger.info(
            f"Successfully created new bucket and uploaded object: {new_bucket}"
        )

        logger.test_step("Verify all storage pods are running")
        wait_for_storage_pods()

        logger.test_step("Archive ceph crash warnings and run health check")
        logger.info("Archiving ceph crash warnings")
        tool_pod = get_ceph_tools_pod()
        tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)

        logger.info("Running cluster health check")
        self.sanity_helpers.health_check(tries=10)
        logger.info("Cluster health check passed")


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
        logger.info(f"Scaling rook-ceph-operator to {replica} replica(s)")
        self.dep_ocp.exec_oc_cmd(
            f"scale deployment {constants.ROOK_CEPH_OPERATOR} --replicas={replica}"
        )

        logger.info(f"Scaling ocs-operator to {replica} replica(s)")
        self.dep_ocp.exec_oc_cmd(
            f"scale deployment {defaults.OCS_OPERATOR_NAME} --replicas={replica}"
        )

        if replica == 1:
            logger.info(
                "Waiting 150s for operators to stabilize and cluster to reconcile"
            )
            time.sleep(150)
        logger.info(f"Operator scaling to {replica} replica(s) completed")

    def patch_sleep_on_osds(self):
        """
        Patch the OSD deployments to sleep and remove the `livenessProbe` parameter,

        """
        osd_dep = get_deployments_having_label(
            label=constants.OSD_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        osd_deployments = [OCS(**osd) for osd in osd_dep]
        logger.info(f"Found {len(osd_deployments)} OSD deployments to patch")

        for osd in osd_deployments:
            logger.debug(f"Patching OSD deployment: {osd.name}")
            logger.debug(f"Removing livenessProbe from {osd.name}")
            params = '[{"op":"remove", "path":"/spec/template/spec/containers/0/livenessProbe"}]'
            self.dep_ocp.patch(
                resource_name=osd.name,
                params=params,
                format_type="json",
            )

            logger.debug(f"Setting sleep infinity command on {osd.name}")
            params = (
                '{"spec": {"template": {"spec": {"containers": [{"name": "osd", "command":'
                ' ["sleep", "infinity"], "args": []}]}}}}'
            )
            self.dep_ocp.patch(
                resource_name=osd.name,
                params=params,
            )
        logger.info(f"Successfully patched {len(osd_deployments)} OSD deployments")

        logger.info("Waiting 60s for OSD pods to restart with new configuration")
        time.sleep(60)

        logger.info("Verifying all OSD pods reached running state")
        osd_pods = get_osd_pods()
        for osd in osd_pods:
            logger.debug(f"Waiting for OSD pod: {osd.name}")
            wait_for_resource_state(
                resource=osd, state=constants.STATUS_RUNNING, timeout=600
            )
        logger.info(f"All {len(osd_pods)} OSD pods are running")

    @retry(CommandFailed, tries=10, delay=5, backoff=1)
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

        logger.info(f"Copying tar binary to {len(pod_objs)} {pod_type.upper()} pods")
        for pod_obj in pod_objs:
            logger.debug(f"Copying tar binary to pod: {pod_obj.name}")
            cmd = (
                f"cat /usr/bin/tar | oc exec -i -n {constants.OPENSHIFT_STORAGE_NAMESPACE} {pod_obj.name}  -- bash -c "
                f"'cat > /usr/bin/tar'"
            )
            run_cmd(cmd, shell=True)

            logger.debug(
                f"Setting execute permissions on /usr/bin/tar in pod: {pod_obj.name}"
            )
            cmd = "chmod +x /usr/bin/tar"
            _exec_cmd_on_pod(cmd=cmd, pod_obj=pod_obj)
        logger.info(
            f"Successfully copied tar binary to all {len(pod_objs)} {pod_type.upper()} pods"
        )

    def prepare_monstore_script(self):
        """
        Prepares the script to retrieve the `monstore` cluster map from OSDs

        """
        logger.info(
            f"Preparing mon-store recovery script: {self.backup_dir}/recover_mon.sh"
        )
        recover_mon = f"""
        #!/bin/bash
        ms=/tmp/monstore

        rm -rf $ms
        mkdir $ms

        for osd_pod in $(oc get po -l app=rook-ceph-osd -oname -n openshift-storage); do

            echo "Starting with pod: $osd_pod"

            podname=$(echo $osd_pod|sed 's/pod\\///g')
            oc exec -n {constants.OPENSHIFT_STORAGE_NAMESPACE} $osd_pod -- rm -rf $ms
            oc exec -n {constants.OPENSHIFT_STORAGE_NAMESPACE} $osd_pod -- mkdir $ms
            oc cp $ms {constants.OPENSHIFT_STORAGE_NAMESPACE}/$podname:$ms

            rm -rf $ms
            mkdir $ms

            echo "pod in loop: $osd_pod ; done deleting local dirs"

            oc exec -n {constants.OPENSHIFT_STORAGE_NAMESPACE} $osd_pod -- \\
            ceph-objectstore-tool --type bluestore --data-path \\
            /var/lib/ceph/osd/ceph-$(oc get -n \\
            {constants.OPENSHIFT_STORAGE_NAMESPACE} $osd_pod \\
            -ojsonpath='{{ .metadata.labels.ceph_daemon_id }}') \\
            --op update-mon-db --no-mon-config --mon-store-path $ms
            echo "Done with COT on pod: $osd_pod"

            oc cp {constants.OPENSHIFT_STORAGE_NAMESPACE}/$podname:$ms $ms

            echo "Finished pulling COT data from pod: $osd_pod"
        done
        """

        with open(f"{self.backup_dir}/recover_mon.sh", "w") as file:
            file.write(recover_mon)
        exec_cmd(cmd=f"chmod +x {self.backup_dir}/recover_mon.sh")
        logger.info("Mon-store recovery script prepared successfully")

    @retry(CommandFailed, tries=15, delay=5, backoff=1)
    def run_mon_store(self):
        """
        Runs script to get the mon store from OSDs

        Raise:
            CommandFailed
        """
        logger.info(
            f"Executing mon-store retrieval script: {self.backup_dir}/recover_mon.sh"
        )
        result = exec_cmd(cmd=f"sh {self.backup_dir}/recover_mon.sh")
        result.stdout = result.stdout.decode()
        result.stderr = result.stderr.decode()

        logger.debug(f"Mon store retrieval stdout: {result.stdout}")
        if result.stderr:
            logger.debug(f"Mon store retrieval stderr: {result.stderr}")

        search_pattern = re.search(
            pattern="error|unable to open mon store", string=result.stderr
        )
        if search_pattern:
            logger.warning(
                f"Error pattern detected in stderr: {search_pattern.group()}"
            )
            raise CommandFailed(f"Mon store retrieval failed: {search_pattern.group()}")

        logger.info("Successfully collected mon store from all OSDs")

    def patch_sleep_on_mon(self):
        """
        Patches sleep to infinity on monitors

        """
        mon_dep = get_deployments_having_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        mon_deployments = [OCS(**mon) for mon in mon_dep]
        logger.info(f"Found {len(mon_deployments)} monitor deployments to patch")

        for mon in mon_deployments:
            logger.debug(f"Patching monitor deployment: {mon.name} to sleep infinitely")
            params = (
                '{"spec": {"template": {"spec": {"containers":'
                ' [{"name": "mon", "command": ["sleep", "infinity"], "args": []}]}}}}'
            )
            self.dep_ocp.patch(
                resource_name=mon.name,
                params=params,
            )
        logger.info(f"Successfully patched {len(mon_deployments)} monitor deployments")

    def monitor_rebuild(self, mon_map_cmd):
        """
        Rebuilds the monitor

        Args:
            mon_map_cmd (str): mon-store tool command

        """
        logger.info("Starting monitor rebuild process on mon-a")
        mon_a = next(
            mon
            for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
            if mon.get().get("metadata", {}).get("labels", {}).get("ceph_daemon_id")
            == "a"
        )
        logger.info(f"Selected monitor for rebuild: {mon_a.name}")

        logger.info("Creating monmap using extracted monitor IPs")
        logger.debug(f"Monmap command: {mon_map_cmd}")
        mon_a.exec_cmd_on_pod(command=mon_map_cmd, out_yaml_format=False)
        logger.info("Monmap created successfully")

        rebuild_mon_cmd = "ceph-monstore-tool /tmp/monstore rebuild -- --keyring /tmp/keyring --monmap /tmp/monmap"
        logger.info("Rebuilding monitor using ceph-monstore-tool")
        logger.debug(f"Rebuild command: {rebuild_mon_cmd}")
        mon_a.exec_cmd_on_pod(command=rebuild_mon_cmd, out_yaml_format=False)
        logger.info("Monitor rebuild completed successfully")

        logger.info("Updating ownership of rebuilt monstore")
        _exec_cmd_on_pod(cmd="chown -R ceph:ceph /tmp/monstore", pod_obj=mon_a)

        logger.info("Moving rebuilt store.db to monitor data directory")
        _exec_cmd_on_pod(
            cmd="mv /tmp/monstore/store.db /var/lib/ceph/mon/ceph-a/store.db",
            pod_obj=mon_a,
        )

        logger.info("Setting ownership on store.db in monitor data directory")
        _exec_cmd_on_pod(
            cmd="chown -R ceph:ceph /var/lib/ceph/mon/ceph-a/store.db", pod_obj=mon_a
        )

        logger.info(f"Backing up store.db from {mon_a.name} to {self.backup_dir}")
        self._exec_oc_cmd(
            cmd=(
                f"cp {constants.OPENSHIFT_STORAGE_NAMESPACE}/"
                f"{mon_a.name}:/var/lib/ceph/mon/ceph-a/store.db "
                f"{self.backup_dir}/store.db"
            )
        )

        logger.info("Distributing store.db to remaining monitor pods")
        other_mons = [
            mon
            for mon in get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
            if mon.get().get("metadata").get("labels").get("ceph_daemon_id") != "a"
        ]
        logger.info(f"Found {len(other_mons)} other monitors to update")

        for mon in other_mons:
            mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
            logger.info(f"Copying store.db to monitor: {mon.name} (mon-{mon_id})")

            cmd = (
                f"cp {self.backup_dir}/store.db "
                f"{constants.OPENSHIFT_STORAGE_NAMESPACE}/"
                f"{mon.name}:/var/lib/ceph/mon/ceph-{mon_id}/"
            )
            self._exec_oc_cmd(cmd)

            logger.debug(f"Setting ownership on store.db for monitor: {mon.name}")
            _exec_cmd_on_pod(
                cmd=f"chown -R ceph:ceph /var/lib/ceph/mon/ceph-{mon_id}/store.db",
                pod_obj=mon,
            )
        logger.info(
            f"Successfully distributed store.db to all {len(other_mons) + 1} monitors"
        )

    def revert_patches(self, deployment_paths):
        """
        Reverts the patches done on monitors, osds and mgr by replacing their deployments

        Args:
            deployment_paths (list): List of paths to deployment yamls

        """
        logger.info(
            f"Reverting {len(deployment_paths)} deployments to original configuration"
        )
        for dep in deployment_paths:
            dep_name = dep.split("/")[-1].replace(".yaml", "")
            logger.info(f"Reverting deployment: {dep_name}")
            revert_patch = f"replace --force -f {dep}"
            self.ocp_obj.exec_oc_cmd(revert_patch)

            logger.info(
                f"Waiting for pods from deployment {dep_name} to reach running state"
            )

            if "rook-ceph-mon" in dep_name:
                logger.debug("Waiting 30s before validating monitor pods")
                time.sleep(30)
                logger.debug("Validating monitor pods are running")
                validate_mon_pods()
                logger.info(f"Monitor deployment {dep_name} pods are running")
            elif "rook-ceph-osd" in dep_name:
                logger.debug("Waiting 30s before checking OSD pods")
                time.sleep(30)
                osd_pods = get_osd_pods()
                for osd in osd_pods:
                    logger.debug(f"Waiting for OSD pod: {osd.name}")
                    wait_for_resource_state(
                        resource=osd, state=constants.STATUS_RUNNING, timeout=600
                    )
                logger.info(
                    f"OSD deployment {dep_name}: all {len(osd_pods)} pods are running"
                )
            elif "rook-ceph-mgr" in dep_name:
                logger.debug("Waiting 30s before checking MGR pods")
                time.sleep(30)
                mgr_pods = get_mgr_pods(namespace=config.ENV_DATA["cluster_namespace"])
                for mgr_pod in mgr_pods:
                    logger.debug(f"Waiting for MGR pod: {mgr_pod.name}")
                    wait_for_resource_state(
                        resource=mgr_pod, state=constants.STATUS_RUNNING, timeout=600
                    )
                logger.info(
                    f"MGR deployment {dep_name}: all {len(mgr_pods)} pods are running"
                )
            elif "rook-ceph-mds" in dep_name:
                logger.debug("Waiting 30s before checking MDS pods")
                time.sleep(30)
                mds_pods = get_mds_pods()
                for mds_pod in mds_pods:
                    logger.debug(f"Waiting for MDS pod: {mds_pod.name}")
                    wait_for_resource_state(
                        resource=mds_pod, state=constants.STATUS_RUNNING, timeout=600
                    )
                logger.info(
                    f"MDS deployment {dep_name}: all {len(mds_pods)} pods are running"
                )
        logger.info("All deployments successfully reverted")

    def backup_deployments(self):
        """
        Creates a backup of all deployments in the `openshift-storage` namespace

        """
        logger.info("Retrieving all deployments in openshift-storage namespace")
        deployment_names = []
        deployments = self.dep_ocp.get("-o name", out_yaml_format=False)
        deployments_full_name = str(deployments).split()

        for name in deployments_full_name:
            deployment_names.append(name.lstrip("deployment.apps").lstrip("/"))

        logger.info(
            f"Backing up {len(deployment_names)} deployments to {self.backup_dir}"
        )
        for deployment in deployment_names:
            logger.debug(f"Backing up deployment: {deployment}")
            deployment_get = self.dep_ocp.get(resource_name=deployment)
            deployment_yaml = join(self.backup_dir, deployment + ".yaml")
            templating.dump_data_to_temp_yaml(deployment_get, deployment_yaml)
        logger.info(f"Successfully backed up {len(deployment_names)} deployments")

    def deployments_to_revert(self):
        """
        Gets mon, osd and mgr deployments to revert.
        Returns deployments in order: MON -> OSD -> MGR for proper cluster recovery.

        Returns:
            tuple: deployment paths to be reverted

        """
        logger.debug("Identifying deployments to revert")
        to_revert_patches = (
            get_deployments_having_label(
                label=constants.MON_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            + get_deployments_having_label(
                label=constants.OSD_APP_LABEL,
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
        logger.debug(
            f"Found {len(to_revert_patches)} MON/OSD/MGR deployments and {len(to_revert_mds)} MDS deployments"
        )

        to_revert_patches_path = []
        to_revert_mds_path = []
        for dep in to_revert_patches:
            to_revert_patches_path.append(
                join(self.backup_dir, dep["metadata"]["name"] + ".yaml")
            )
        for dep in to_revert_mds:
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
        logger.info("Extracting Ceph daemon keyrings from secrets")
        all_keyring_secrets = self.get_all_keyring_secrets()
        logger.info(f"Found {len(all_keyring_secrets)} keyring secrets")
        formatted_data = []
        for keyring_secret in all_keyring_secrets:
            logger.debug(f"Processing keyring secret: {keyring_secret}")
            cmd = (
                f"oc get secret {keyring_secret} -n "
                f"{constants.OPENSHIFT_STORAGE_NAMESPACE} -ojson | "
                f"jq .data.keyring | xargs echo | base64 -d"
            )
            out = exec_cmd(cmd=cmd, shell=True)
            out_str = out.stdout.decode("utf-8")
            tmp_lines = out_str.strip().splitlines()
            keyring_data = [line.replace("\t", "").strip() for line in tmp_lines]
            pod_name = keyring_data[0].strip()
            formatted_data.append(pod_name)
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
                    logger.debug("Found key entry in keyring data")
                    formatted_data.append(f"    key = {key}")
                for cap in caps:
                    logger.debug(f"Found cap: {cap}")
                    formatted_data.append(f"    {cap}")
        with open(f"{self.keyring_dir}/keyring-mon-a", "w") as f:
            f.write("\n".join(formatted_data))
        logger.debug(f"Saved daemon keyrings to {self.keyring_dir}/keyring-mon-a")

        logger.info("Extracting OSD keys from OSD pods")
        osd_pods = get_osd_pods()
        logger.info(f"Found {len(osd_pods)} OSD pods")
        for osd_pod in osd_pods:
            logger.debug(f"Extracting keyring from OSD pod: {osd_pod.name}")
            osd_id = osd_pod.get().get("metadata").get("labels").get("ceph-osd-id")
            cmd = (
                f"oc exec -i -n {constants.OPENSHIFT_STORAGE_NAMESPACE} "
                f"{osd_pod.name} -- bash -c "
                f"'cat /var/lib/ceph/osd/ceph-{osd_id}/keyring' "
            )
            out = exec_cmd(cmd=cmd, shell=True)
            out_osd_str = out.stdout.decode("utf-8")
            lines = out_osd_str.strip().splitlines()
            osd_keyring_data = [line.replace("\t", "").strip() for line in lines]
            pod_name = osd_keyring_data[0].strip()
            formatted_data.append(pod_name)
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
        Patch the MDS deployments to sleep and remove the `livenessProbe` parameter,

        """
        mds_dep = get_deployments_having_label(
            label=constants.MDS_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        mds_deployments = [OCS(**mds) for mds in mds_dep]
        logger.info(f"Found {len(mds_deployments)} MDS deployments to patch")

        for mds in mds_deployments:
            logger.debug(f"Patching MDS deployment: {mds.name}")
            logger.debug(f"Removing livenessProbe from {mds.name}")
            params = '[{"op":"remove", "path":"/spec/template/spec/containers/0/livenessProbe"}]'
            self.dep_ocp.patch(
                resource_name=mds.name,
                params=params,
                format_type="json",
            )

            logger.debug(f"Setting sleep infinity command on {mds.name}")
            params = (
                '{"spec": {"template": {"spec": {"containers": '
                '[{"name": "mds", "command": ["sleep", "infinity"], "args": []}]}}}}'
            )
            self.dep_ocp.patch(
                resource_name=mds.name,
                params=params,
            )
        logger.info(f"Successfully patched {len(mds_deployments)} MDS deployments")

        logger.info("Waiting 60s for MDS pods to restart with new configuration")
        time.sleep(60)

        logger.info("Verifying all MDS pods reached running state")
        mds_pods = get_mds_pods(namespace=config.ENV_DATA["cluster_namespace"])
        for mds in mds_pods:
            logger.debug(f"Waiting for MDS pod: {mds.name}")
            wait_for_resource_state(resource=mds, state=constants.STATUS_RUNNING)
        logger.info(f"All {len(mds_pods)} MDS pods are running")

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
    logger.debug(f"Updating initialDelaySeconds on monitor deployment: {mon_dep}")
    kubeconfig = config.RUN.get("kubeconfig")
    namespace = config.ENV_DATA["cluster_namespace"]
    cmd = (
        f"oc --kubeconfig {kubeconfig} -n {namespace} get deployment {mon_dep} -o yaml | "
        f'sed "s/initialDelaySeconds: 10/initialDelaySeconds: 10000/g" | '
        f"oc --kubeconfig {kubeconfig} -n {namespace} replace -f - "
    )
    logger.debug(f"Executing command: {cmd}")
    exec_cmd(cmd=cmd, shell=True)
    logger.debug(f"Successfully updated initialDelaySeconds for {mon_dep}")


def update_mon_initial_delay():
    """
    Inserts delay on all monitors

    """
    logger.info("Updating initialDelaySeconds on all monitor deployments")
    mon_dep = get_deployments_having_label(
        label=constants.MON_APP_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    mon_deployments = [OCS(**mon) for mon in mon_dep]
    logger.info(f"Found {len(mon_deployments)} monitor deployments to update")

    for mon in mon_deployments:
        logger.debug(f"Updating initialDelaySeconds on deployment: {mon.name}")
        insert_delay(mon_dep=mon.name)

    logger.info("Waiting 90s for monitors to initialize with new delay settings")
    time.sleep(90)

    logger.info("Validating all monitor pods reached running state")
    validate_mon_pods()
    logger.info("All monitor pods are running")


@retry(
    (ResourceWrongStatusException, ResourceNotFoundError), tries=10, delay=5, backoff=1
)
def validate_mon_pods():
    """
    Checks mon pods are running with retries

    """
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    logger.debug(f"Validating {len(mon_pods)} monitor pods are in running state")
    for mon in mon_pods:
        logger.debug(f"Waiting for monitor pod: {mon.name}")
        wait_for_resource_state(resource=mon, state=constants.STATUS_RUNNING)
    logger.debug(f"All {len(mon_pods)} monitor pods validated successfully")


def corrupt_ceph_monitors():
    """
    Corrupts ceph monitors by deleting store.db file

    """
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    logger.info(f"Corrupting {len(mon_pods)} monitor pods by deleting store.db")

    for mon in mon_pods:
        mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
        logger.info(f"Corrupting monitor pod: {mon.name} (mon-{mon_id})")
        _exec_cmd_on_pod(
            cmd=f"rm -rf /var/lib/ceph/mon/ceph-{mon_id}/store.db", pod_obj=mon
        )
        logger.debug(f"Deleted store.db from monitor: {mon.name}")

        try:
            logger.debug(f"Waiting for {mon.name} to reach CrashLoopBackOff state")
            wait_for_resource_state(resource=mon, state=constants.STATUS_CLBO)
        except ResourceWrongStatusException:
            current_status = mon.ocp.get_resource(
                resource_name=mon.name, column="STATUS"
            )
            if current_status != constants.STATUS_CLBO:
                logger.warning(
                    f"Monitor {mon.name} did not reach CLBO state (current: {current_status}), "
                    f"forcing pod deletion"
                )
                mon.delete()

    logger.info("Validating all monitors are in CrashLoopBackOff state")
    corrupted_mons = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    for mon in corrupted_mons:
        logger.debug(f"Verifying monitor {mon.name} is in CLBO state")
        wait_for_resource_state(resource=mon, state=constants.STATUS_CLBO)
    logger.info(
        f"All {len(corrupted_mons)} monitors successfully corrupted and in CLBO state"
    )


def handle_multi_attach_error(pod_obj, timeout=300):
    """
    Handle multi-attach volume errors by deleting stale VolumeAttachment resources.

    Args:
        pod_obj: Pod object that may have multi-attach error
        timeout: Maximum time to wait for pod to become running (default: 300s)

    Returns:
        bool: True if pod is running, False otherwise

    """
    logger.debug(f"Checking pod {pod_obj.name} for multi-attach errors")

    try:
        pod_describe = pod_obj.ocp.exec_oc_cmd(
            f"describe pod {pod_obj.name}", out_yaml_format=False
        )

        if "Multi-Attach error" in pod_describe:
            logger.warning(f"Multi-attach error detected for pod: {pod_obj.name}")

            pvc_match = re.search(
                r'Multi-Attach error for volume "([^"]+)"', pod_describe
            )
            if pvc_match:
                pvc_id = pvc_match.group(1)
                logger.info(f"Identified PVC with multi-attach error: {pvc_id}")

                va_ocp = OCP(kind="VolumeAttachment", namespace="")
                try:
                    attachments = va_ocp.get()
                    if attachments and "items" in attachments:
                        for attachment in attachments["items"]:
                            if (
                                attachment.get("spec", {})
                                .get("source", {})
                                .get("persistentVolumeName")
                                == pvc_id
                            ):
                                va_name = attachment["metadata"]["name"]
                                logger.info(
                                    f"Deleting stale VolumeAttachment: {va_name}"
                                )
                                va_ocp.delete(resource_name=va_name)
                                logger.info(
                                    "Waiting 10s for VolumeAttachment cleanup to propagate"
                                )
                                time.sleep(10)
                                break
                except Exception:
                    logger.exception(
                        f"Failed to handle VolumeAttachment for PVC {pvc_id}"
                    )
            else:
                logger.warning(
                    "Multi-attach error detected but could not extract PVC ID from pod description"
                )
    except Exception:
        logger.exception(
            f"Error while checking pod {pod_obj.name} for multi-attach errors"
        )

    logger.debug(
        f"Waiting for pod {pod_obj.name} to reach running state (timeout: {timeout}s)"
    )
    try:
        wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=timeout
        )
        logger.info(f"Pod {pod_obj.name} is running")
        return True
    except Exception as e:
        logger.error(
            f"Pod {pod_obj.name} failed to reach running state after {timeout}s: {e}"
        )
        return False


def recover_mcg():
    """
    Recovery procedure for noobaa by re-spinning the pods after mon recovery.
    Handles multi-attach errors by deleting stale VolumeAttachment resources.

    Raises:
        AssertionError: If any NooBaa or RGW pods fail to reach running state

    """
    logger.info("Starting MCG recovery by re-spinning noobaa pods")
    noobaa_pods = get_noobaa_pods()
    logger.info(f"Found {len(noobaa_pods)} noobaa pods to respawn")

    for noobaa_pod in noobaa_pods:
        logger.info(f"Force deleting noobaa pod: {noobaa_pod.name}")
        noobaa_pod.delete(force=True)

    logger.info("Waiting 120s for noobaa pods to respawn")
    time.sleep(120)

    logger.info("Waiting for postgres cluster pods to spawn (timeout: 300s)")
    max_wait_time = 300
    start_time = time.time()
    expected_pg_pods = 2
    active_pg_pods = []

    while time.time() - start_time < max_wait_time:
        noobaa_db_pods = pod.get_pods_having_label(
            constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
            config.ENV_DATA["cluster_namespace"],
        )
        active_pg_pods = [
            p
            for p in noobaa_db_pods
            if p.get("metadata", {}).get("deletionTimestamp") is None
        ]
        if len(active_pg_pods) >= expected_pg_pods:
            elapsed = int(time.time() - start_time)
            logger.info(
                f"All {len(active_pg_pods)} postgres cluster pods spawned after {elapsed}s"
            )
            break
        logger.debug(
            f"Postgres pods: active={len(active_pg_pods)}, total={len(noobaa_db_pods)}, "
            f"expected={expected_pg_pods}, elapsed={int(time.time() - start_time)}s"
        )
        time.sleep(10)
    else:
        logger.warning(
            f"Timeout waiting for postgres pods after {max_wait_time}s: "
            f"found {len(active_pg_pods)}/{expected_pg_pods} expected"
        )

    logger.info("Verifying all noobaa pods reached running state")
    respawned_noobaa_pods = get_noobaa_pods()
    logger.info(
        f"Found {len(respawned_noobaa_pods)} noobaa pods to verify (includes postgres cluster)"
    )
    failed_noobaa_pods = []

    for noobaa_pod in respawned_noobaa_pods:
        logger.debug(f"Checking noobaa pod: {noobaa_pod.name}")
        if not handle_multi_attach_error(noobaa_pod, timeout=600):
            logger.error(f"Noobaa pod {noobaa_pod.name} failed to reach running state")
            failed_noobaa_pods.append(noobaa_pod.name)

    if failed_noobaa_pods:
        logger.error(
            f"MCG recovery failed: {len(failed_noobaa_pods)} noobaa pods not running: {failed_noobaa_pods}"
        )
        raise AssertionError(f"NooBaa pod recovery failed: {failed_noobaa_pods}")

    logger.info(
        f"MCG noobaa pods recovery completed: all {len(respawned_noobaa_pods)} pods running"
    )

    if config.ENV_DATA["platform"].lower() in constants.ON_PREM_PLATFORMS:
        logger.info("On-prem platform detected: recovering RGW pods")
        rgw_pods = get_rgw_pods()
        logger.info(f"Found {len(rgw_pods)} RGW pods to respawn")

        for rgw_pod in rgw_pods:
            logger.info(f"Force deleting RGW pod: {rgw_pod.name}")
            rgw_pod.delete(force=True)

        logger.info("Waiting 120s for RGW pods to respawn")
        time.sleep(120)

        logger.info("Verifying RGW pods reached running state")
        failed_rgw_pods = []
        respawned_rgw_pods = get_rgw_pods()
        for rgw_pod in respawned_rgw_pods:
            logger.debug(f"Checking RGW pod: {rgw_pod.name}")
            if not handle_multi_attach_error(rgw_pod, timeout=600):
                logger.error(f"RGW pod {rgw_pod.name} failed to reach running state")
                failed_rgw_pods.append(rgw_pod.name)

        if failed_rgw_pods:
            logger.error(
                f"RGW recovery failed: {len(failed_rgw_pods)} RGW pods not running: {failed_rgw_pods}"
            )
            raise AssertionError(f"RGW pod recovery failed: {failed_rgw_pods}")

        logger.info(
            f"RGW pods recovery completed: all {len(respawned_rgw_pods)} pods running"
        )
    else:
        logger.debug(
            f"Skipping RGW recovery on non-on-prem platform: {config.ENV_DATA['platform']}"
        )


def remove_global_id_reclaim():
    """
    Removes global id warning by re-spinning client and mon pods

    """
    logger.info(
        "Removing global ID reclaim warnings by re-spinning CSI, MDS, and monitor pods"
    )

    logger.info("Collecting CSI plugin and provisioner pods")
    csi_pods = []
    interfaces = [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]
    for interface in interfaces:
        plugin_pods = get_plugin_pods(interface)
        logger.debug(f"Found {len(plugin_pods)} plugin pods for interface: {interface}")
        csi_pods += plugin_pods

    cephfs_provisioner_pods = get_cephfsplugin_provisioner_pods()
    rbd_provisioner_pods = get_rbdfsplugin_provisioner_pods()
    logger.debug(f"Found {len(cephfs_provisioner_pods)} CephFS provisioner pods")
    logger.debug(f"Found {len(rbd_provisioner_pods)} RBD provisioner pods")

    csi_pods += cephfs_provisioner_pods
    csi_pods += rbd_provisioner_pods
    logger.info(f"Deleting {len(csi_pods)} CSI pods")
    for csi_pod in csi_pods:
        logger.debug(f"Deleting CSI pod: {csi_pod.name}")
        csi_pod.delete()

    logger.info("Deleting MDS pods")
    mds_pods = get_mds_pods()
    logger.info(f"Found {len(mds_pods)} MDS pods to delete")
    for mds_pod in mds_pods:
        logger.debug(f"Deleting MDS pod: {mds_pod.name}")
        mds_pod.delete()

    logger.info("Waiting for MDS pods to respawn and reach running state")
    respawned_mds_pods = get_mds_pods()
    for mds_pod in respawned_mds_pods:
        logger.debug(f"Waiting for MDS pod: {mds_pod.name}")
        wait_for_resource_state(resource=mds_pod, state=constants.STATUS_RUNNING)
    logger.info(f"All {len(respawned_mds_pods)} MDS pods are running")

    logger.info("Deleting monitor pods")
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    logger.info(f"Found {len(mon_pods)} monitor pods to delete")
    for mon in mon_pods:
        logger.debug(f"Deleting monitor pod: {mon.name}")
        mon.delete()

    logger.info("Waiting for monitor pods to respawn and reach running state")
    respawned_mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    for mon in respawned_mon_pods:
        logger.debug(f"Waiting for monitor pod: {mon.name}")
        wait_for_resource_state(resource=mon, state=constants.STATUS_RUNNING)
    logger.info(f"All {len(respawned_mon_pods)} monitor pods are running")

    logger.info("Global ID reclaim warning removal completed")


def replace_mds_deployments():
    """
    Backup and replace MDS deployments to recover from CephFS reset

    This function backs up the MDS deployments and replaces them using oc replace --force

    """
    logger.info("Replacing MDS deployments to recover from CephFS reset")

    mds_deployment_names = [
        "rook-ceph-mds-ocs-storagecluster-cephfilesystem-a",
        "rook-ceph-mds-ocs-storagecluster-cephfilesystem-b",
    ]

    dep_ocp = OCP(kind=constants.DEPLOYMENT, namespace=defaults.ROOK_CLUSTER_NAMESPACE)

    with tempfile.TemporaryDirectory() as backup_dir:
        logger.info(
            f"Backing up {len(mds_deployment_names)} MDS deployments to temporary directory"
        )
        logger.debug(f"Backup directory: {backup_dir}")

        for deployment in mds_deployment_names:
            logger.debug(f"Backing up deployment: {deployment}")
            deployment_get = dep_ocp.get(resource_name=deployment)
            deployment_yaml = join(backup_dir, deployment + ".yaml")
            templating.dump_data_to_temp_yaml(deployment_get, deployment_yaml)

        logger.info(f"Successfully backed up {len(mds_deployment_names)} deployments")

        logger.info(f"Replacing {len(mds_deployment_names)} MDS deployments")
        for deployment in mds_deployment_names:
            deployment_yaml = join(backup_dir, deployment + ".yaml")
            logger.debug(f"Replacing deployment: {deployment}")
            exec_cmd(
                f"oc replace --force -f {deployment_yaml} -n {defaults.ROOK_CLUSTER_NAMESPACE}"
            )

        logger.info(
            f"Successfully replaced {len(mds_deployment_names)} MDS deployments"
        )


def ceph_fs_recovery():
    """
    Resets the CephFS and replaces MDS deployments

    """
    logger.info(
        f"Starting CephFS recovery for filesystem: {defaults.CEPHFILESYSTEM_NAME}"
    )
    toolbox = pod.get_ceph_tools_pod()
    logger.debug(f"Using ceph tools pod: {toolbox.name}")

    try:
        logger.info(f"Attempting to reset CephFS: {defaults.CEPHFILESYSTEM_NAME}")
        toolbox.exec_cmd_on_pod(
            f"ceph fs reset {defaults.CEPHFILESYSTEM_NAME} --yes-i-really-mean-it"
        )
        logger.info("CephFS reset successful")
    except CommandFailed as e:
        logger.warning(f"CephFS reset failed, creating new filesystem: {e}")
        try:
            logger.info(f"Creating new CephFS: {defaults.CEPHFILESYSTEM_NAME}")
            toolbox.exec_cmd_on_pod(
                f"ceph fs new {defaults.CEPHFILESYSTEM_NAME} ocs-storagecluster-cephfilesystem-metadata "
                f"ocs-storagecluster-cephfilesystem-data0 --force"
            )
            logger.info("New CephFS created, attempting reset again")
            toolbox.exec_cmd_on_pod(
                f"ceph fs reset {defaults.CEPHFILESYSTEM_NAME} --yes-i-really-mean-it"
            )
            logger.info("CephFS reset successful after recreation")
        except CommandFailed:
            logger.exception(
                f"Failed to recover CephFS: {defaults.CEPHFILESYSTEM_NAME}"
            )
            raise

    replace_mds_deployments()

    logger.info("Verifying MDS pods reach running state after CephFS recovery")
    mds_pods = get_mds_pods()
    logger.info(f"Found {len(mds_pods)} MDS pods to verify")
    failed_mds_pods = []
    for mds_pod in mds_pods:
        logger.debug(f"Checking MDS pod: {mds_pod.name}")
        if not handle_multi_attach_error(mds_pod, timeout=600):
            logger.warning(f"MDS pod {mds_pod.name} did not reach running state")
            failed_mds_pods.append(mds_pod.name)

    if failed_mds_pods:
        logger.warning(
            f"{len(failed_mds_pods)} MDS pods did not reach running state: {failed_mds_pods}"
        )
    else:
        logger.info(f"All {len(mds_pods)} MDS pods are running after CephFS recovery")


def get_spun_dc_pods(pod_list):
    """
    Fetches info about the re-spun dc pods

    Args:
        pod_list (list): list of previous pod objects

    Returns:
        list : list of respun pod objects

    """
    logger.debug(f"Looking for re-spun pods for {len(pod_list)} deployment configs")
    new_pods = []

    for pod_obj in pod_list:
        pod_label = pod_obj.labels.get("deploymentconfig")
        label_selector = f"deploymentconfig={pod_label}"
        logger.debug(f"Searching for pods with label: {label_selector}")

        pods_data = pod.get_pods_having_label(label_selector, pod_obj.namespace)
        for pod_data in pods_data:
            pod_name = pod_data.get("metadata").get("name")
            if "-deploy" not in pod_name and pod_name not in pod_obj.name:
                logger.debug(f"Found re-spun pod: {pod_name}")
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

    logger.info("Extracting monitor IPs and IDs for monmap generation")
    mon_pods = get_mon_pods(namespace=config.ENV_DATA["cluster_namespace"])
    logger.info(f"Found {len(mon_pods)} monitor pods")

    for mon in mon_pods:
        mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
        mon_ids.append(mon_id)

        logger.debug(
            f"Extracting public IP from monitor pod: {mon.name} (mon-{mon_id})"
        )
        ip_match = re.findall(
            r"[0-9]+(?:\.[0-9]+){3}",
            mon.get().get("spec").get("initContainers")[1].get("args")[-2],
        )
        if ip_match:
            mon_ips.append(ip_match[0])
            logger.debug(f"Monitor {mon.name}: ID={mon_id}, IP={ip_match[0]}")
        else:
            logger.error(f"Could not extract IP from monitor {mon.name}")
            raise ValueError(f"Could not extract IP from monitor {mon.name}")

    mon_a = mon_pods[0]
    logger.debug(f"Extracting FSID from monitor: {mon_a.name}")
    fsid = (
        mon_a.get()
        .get("spec")
        .get("initContainers")[1]
        .get("args")[0]
        .replace("--fsid=", "")
    )
    logger.info(f"Cluster FSID: {fsid}")

    for mon_id, ip in zip(mon_ids, mon_ips):
        mon_ips_dict.update({mon_id: ip})

    logger.debug(f"Monitor ID to IP mapping: {mon_ips_dict}")

    mon_ip_ids = ""
    for key, val in mon_ips_dict.items():
        mon_ip_ids = mon_ip_ids + f"--add {key} {val}" + " "

    mon_map_cmd = f"monmaptool --create {mon_ip_ids} --enable-all-features --clobber /tmp/monmap --fsid {fsid}"
    logger.info(f"Generated monmap command with {len(mon_ips_dict)} monitors")
    logger.debug(f"Monmap command: {mon_map_cmd}")
    return mon_map_cmd
