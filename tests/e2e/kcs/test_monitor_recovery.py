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
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating
import tempfile
import re

from ocs_ci.framework import config

from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_osd_pods,
    get_deployments_having_label,
)
from ocs_ci.ocs.resources import pod

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.ocp import OCP

from ocs_ci.helpers.helpers import wait_for_resource_state

logger = logging.getLogger(__name__)


@tier3
@ignore_leftovers
@pytest.mark.polarion_id("")
@pytest.mark.bugzilla("")
@skipif_ocs_version("<4.6")
@skipif_openshift_dedicated
@skipif_external_mode
class TestMonitorRecovery(E2ETest):
    """
    Test to verify monitor recovery

    """
    def test_monitor_recovery(self):
        corrupt_ceph_monitors()
        logger.info("Starting the monitor recovery procedure")
        scale_rook_ocs_operators(replica=0)
        backup_dir = backup_deployments()
        patch_sleep_on_osds()
        get_monstore()
        patch_sleep_on_mon()
        mon_rebuild()
        rebuilding_other_mons()
        revert_patches(backup_dir)
        scale_rook_ocs_operators(replica=1)


def corrupt_ceph_monitors():
    """
    Corrupts ceph monitors by deleting store.db file

    """
    mon_pods = get_mon_pods()
    for mon in mon_pods:
        logger.info(f"Corrupting mon {mon.name}")
        mon_id = mon.get().get("metadata").get("labels").get("ceph_daemon_id")
        logger.info(
            mon.exec_cmd_on_pod(
                command=f"rm -rf /var/lib/ceph/mon/ceph-{mon_id}/store.db"
            )
        )
        wait_for_resource_state(mon, state=constants.STATUS_CLBO)
        if (
            mon.ocp.get_resource(resource_name=mon.name, column="STATUS")
            == constants.STATUS_RUNNING
        ):
            logger.info(
                f"Re-spinning monitor: {mon.name} since it did not reach CLBO state"
            )
            mon.delete()


def scale_rook_ocs_operators(replica=1):
    """
    Scales rook and ocs operators based on replica

    Args:
        replica (int): replica count

    """
    dep_ocp = OCP(kind="Deployment", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    logger.info(f"Scaling rook-ceph-operator to replica: {replica}")
    dep_ocp.exec_oc_cmd(f"scale deployment rook-ceph-operator --replicas={replica}")
    logger.info(f"Scaling ocs-operator to replica: {replica} ")
    dep_ocp.exec_oc_cmd(f"scale deployment ocs-operator --replicas={replica}")


def patch_sleep_on_osds():
    """
    Patch the OSD deployments to remove the `livenessProbe` parameter,
    and run it with the `sleep` command.

    """
    dep_ocp = ocp.OCP(kind="Deployment", namespace="openshift-storage")
    osd_deployments = get_deployments_having_label(
        label=constants.OSD_APP_LABEL, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    for osd in osd_deployments:
        logger.info("Patching OSDs with livenessProbe and sleep infinity")
        params = (
            '[{"op":"remove", "path":"/spec/template/spec/containers/0/livenessProbe"}]'
        )
        dep_ocp.patch(
            resource_name=osd.name,
            params=params,
            format_type="json",
        )
        params = (
            '{"spec": {"template": {"spec": {"containers": [{"name": "osd", "command":'
            ' ["sleep", "infinity"], "args": []}]}}}}'
        )
        dep_ocp.patch(
            resource_name=osd.name,
            params=params,
        )
    logger.info("Sleeping for 30 seconds and waiting for OSDs to reach running state")
    time.sleep(30)
    for osd in get_osd_pods():
        wait_for_resource_state(osd, state=constants.STATUS_RUNNING)


def get_monstore():
    """
    Retrieve the `monstore` cluster map from all the OSDs

    """
    logger.info("Taking COT data from Each OSDs")
    recover_mon = """
        #!/bin/bash -x
        ms=/tmp/monstore
        rm -rf $ms
        mkdir $ms
        for osd_pod in $(oc get po -l app=rook-ceph-osd -oname -n openshift-storage); do
          echo "Starting with pod: $osd_pod"
          oc rsync $ms $osd_pod:$ms
          rm -rf $ms
          mkdir $ms
          echo "pod in loop: $osd_pod ; done deleting local dirs"
          oc exec $osd_pod -- rm -rf  $ms
          oc exec $osd_pod -- mkdir $ms
          oc exec $osd_pod -- ceph-objectstore-tool --type bluestore --data-path /var/lib/ceph/osd/ceph-$(oc get $osd_pod -ojsonpath='{ .metadata.labels.ceph_daemon_id }') --op update-mon-db --no-mon-config --mon-store-path $ms
          echo "Done with COT on pod: $osd_pod"
          echo "$osd_pod:$ms $ms"
          oc rsync $osd_pod:$ms $ms
          echo "Finished pulling COT data from pod: $osd_pod"
        done
    """
    with open("/tmp/recover_mon.sh", "w") as file:
        file.write(recover_mon)
    os.system(command="chmod +x /tmp/recover_mon.sh")
    logger.info("Getting mon store")
    os.system(command="sh /tmp/recover_mon.sh")


def patch_sleep_on_mon():
    """
    Patches sleep and inserts delay on a monitors

    """
    dep_ocp = ocp.OCP(kind="Deployment", namespace="openshift-storage")
    mon_deployments = get_deployments_having_label(
        label=constants.MON_APP_LABEL, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    for mon in mon_deployments:
        params = (
            '{"spec": {"template": {"spec": {"containers":'
            ' [{"name": "mon", "command": ["sleep", "infinity"], "args": []}]}}}}'
        )
        logger.info(f"Patching mon {mon.name} for sleep")
        dep_ocp.patch(
            resource_name=mon.name,
            params=params,
        )
    mons_dep = get_deployments_having_label(
        label=constants.MON_APP_LABEL, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    logger.info(f"Updating initialDelaySeconds on {mons_dep[0].name} deployment")
    insert_delay(mon_dep=mons_dep[0].name)
    logger.info("Sleeping and waiting for mon to reach running state")
    time.sleep(30)
    wait_for_resource_state(get_mon_pods()[0], state=constants.STATUS_RUNNING)


def mon_rebuild():
    """
    Rebuilds the monitor

    """
    mon_a = get_mon_pods()[0]
    logger.info(f"Working on mon: {mon_a.name}")
    cmd = f"oc cp /tmp/monstore/monstore {mon_a.name}:/tmp/"
    logger.info(f"Copying monstore into mon {mon_a.name}")
    os.system(cmd)
    logger.info("Changing ownership of monstore to ceph")
    mon_a.exec_cmd_on_pod(command="chown -R ceph:ceph /tmp/monstore")

    mon_map_cmd = generate_monmap_cmd()
    logger.info("Creating monmap")
    mon_a.exec_cmd_on_pod(command=mon_map_cmd)

    logger.info("Getting secrets")
    keyring_files = get_ceph_keyrings()

    for k_file in keyring_files:
        cmd = f"oc cp {k_file} {mon_a.name}:/tmp/"
        logger.info(f"Copying keyring into mon {mon_a.name}")
        os.system(cmd)

    logger.info("Importing keyring")
    mon_a.exec_cmd_on_pod(command="cp /etc/ceph/keyring-store/keyring /tmp/keyring")
    for k_file in keyring_files:
        logger.info(f"Importing keyring {k_file}")
        mon_a.exec_cmd_on_pod(
            command=f"ceph-authtool  /tmp/keyring  --import-keyring {k_file}"
        )

    rebuild_mon = "ceph-monstore-tool /tmp/monstore rebuild -- --keyring /tmp/keyring --monmap /tmp/monmap"
    logger.info("Running command to rebuild monitor:")
    mon_a.exec_cmd_on_pod(command=rebuild_mon, out_yaml_format=False)

    logger.info("Changing ownership of monstore to ceph")
    mon_a.exec_cmd_on_pod(command="chown -R ceph:ceph /tmp/monstore")
    logger.info("Getting backup of store.db")
    try:
        mon_a.exec_cmd_on_pod(
            command=f"mv /var/lib/ceph/mon/ceph-"
            f"{mon_a.get().get('metadata').get('labels').get('ceph_daemon_id')}/"
            f"store.db /var/lib/ceph/mon/ceph-"
            f"{mon_a.get().get('metadata').get('labels').get('ceph_daemon_id')}/store.db.crr "
        )
    except CommandFailed:
        pass
    logger.info("Copying rebuilt Db into mon")
    mon_a.exec_cmd_on_pod(
        command=f"mv /tmp/monstore/store.db /var/lib/ceph/mon/ceph-"
        f"{mon_a.get().get('metadata').get('labels').get('ceph_daemon_id')}/"
    )

    logger.info("Changing ownership of store.db to ceph")
    mon_a.exec_cmd_on_pod(
        command=f"chown -R ceph:ceph /var/lib/ceph/mon/ceph-"
        f"{mon_a.get().get('metadata').get('labels').get('ceph_daemon_id')}/store.db"
    )

    cmd = (
        f"oc cp {mon_a.name}:/var/lib/ceph/mon/ceph-"
        f"{mon_a.get().get('metadata').get('labels').get('ceph_daemon_id')}/store.db /tmp/store.db"
    )
    logger.info("Copying store.db dir to local")
    os.system(cmd)


def rebuilding_other_mons():
    """
    Rebuilds rest of the monitors by copy the store.db from first monitors

    """
    mons_dep = get_deployments_having_label(
        label=constants.MON_APP_LABEL, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    insert_delay(mons_dep[1].name)
    insert_delay(mons_dep[2].name)
    logger.info("Sleeping and waiting for mons to reach running state")
    time.sleep(90)
    for po in get_mon_pods()[1:]:
        wait_for_resource_state(po, state=constants.STATUS_RUNNING)
    logger.info("Backing up and copying store.db")
    for mon in get_mon_pods()[1:]:
        try:
            mon.exec_cmd_on_pod(
                command=f"mv /var/lib/ceph/mon/ceph-"
                f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}"
                f"/store.db /var/lib/ceph/mon/ceph-"
                f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}"
                f"/store.db.crr "
            )
        except CommandFailed:
            pass
        cmd = (
            f"oc cp /tmp/store.db {mon.name}:/var/lib/ceph/mon/ceph-"
            f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}/ "
        )
        logger.info(f"Copying store.db to {mon.name}")
        os.system(cmd)
        logger.info("Changing ownership of store.db to ceph")
        mon.exec_cmd_on_pod(
            command=f"chown -R ceph:ceph /var/lib/ceph/mon/ceph-"
            f"{mon.get().get('metadata').get('labels').get('ceph_daemon_id')}/store.db"
        )


def insert_delay(mon_dep):
    """
    Inserts delay on a monitor

    Args:
        mon_dep (str): Name of a monitor deployment

    """
    logger.info(f"Updating initialDelaySeconds on deployment: {mon_dep}")
    cmd = f""" oc get deployment {mon_dep} -o yaml |
     sed "s/initialDelaySeconds: 10/initialDelaySeconds: 10000/g" | oc replace -f - """
    logger.info(f"Executing {cmd}")
    os.system(cmd)


def revert_patches(backup_dir):
    """
    Reverts the patches done on monitors and osds by replacing the deployments

    """
    logger.info("Reverting patches on osds and mons ")
    for dep in backup_dir:
        revert_patch = f"oc replace --force -f {dep}"
        os.system(revert_patch)
    logger.info("Sleeping and waiting for all pods up and running..")
    time.sleep(120)
    assert pod.wait_for_pods_to_be_running(
        timeout=300
    ), "Pods did not reach running state"


def backup_deployments():
    """
    Creates a backup of all deployments in the `openshift-storage` namespace

    Returns:
        list: The backup paths of mon and osd deployments

    """
    dep_ocp = OCP(kind="Deployment", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    tmp_backup_dir = tempfile.mkdtemp(prefix="backup")
    deployments = dep_ocp.get("-o name", out_yaml_format=False)
    deployments_full_name = str(deployments).split()
    deployment_names = []
    for name in deployments_full_name:
        deployment_names.append(name.lstrip("deployment.apps").lstrip("/"))
    for deployment in deployment_names:
        deployment_get = dep_ocp.get()
        deployment_yaml = join(tmp_backup_dir, deployment + ".yaml")
        templating.dump_data_to_temp_yaml(deployment_get, deployment_yaml)
    to_revert_patches = get_deployments_having_label(
        label=constants.OSD_APP_LABEL, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    ) + get_deployments_having_label(
        label=constants.MON_APP_LABEL, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    to_revert_patches_path = []
    for dep in to_revert_patches:
        to_revert_patches_path.append(join(tmp_backup_dir, dep.name + ".yaml"))
    return to_revert_patches_path


def get_ceph_keyrings():
    """
    Gets all ceph related keyrings from OCS secrets

    """
    secret_resources = {
        "mons": {"rook-ceph-mons-keyring"},
        "osds": {
            "rook-ceph-osd-0-keyring",
            "rook-ceph-osd-1-keyring",
            "rook-ceph-osd-2-keyring",
        },
        "rgws": {
            "rook-ceph-rgw-ocs-storagecluster-cephobjectstore-a-keyring",
        },
        "mgrs": {"rook-ceph-mgr-a-keyring"},
        "crash": {"rook-ceph-crash-collector-keyring"},
        "provisioners": {"rook-csi-cephfs-provisioner", "rook-csi-rbd-provisioner"},
        "mdss": {
            "rook-ceph-mds-ocs-storagecluster-cephfilesystem-a-keyring",
            "rook-ceph-mds-ocs-storagecluster-cephfilesystem-b-keyring",
        },
    }
    mon_k = get_secrets(secret_resource=secret_resources.get("mons"))
    if config.ENV_DATA["platform"] == "aws":
        rgw_k = None
    else:
        rgw_k = get_secrets(secret_resource=secret_resources.get("rgws"))
    mgr_k = get_secrets(secret_resource=secret_resources.get("mgrs"))
    mds_k = get_secrets(secret_resource=secret_resources.get("mdss"))
    osd_k = get_secrets(secret_resource=secret_resources.get("osds"))

    keyring = {
        "mons": mon_k,
        "rgws": rgw_k,
        "mgrs": mgr_k,
        "mdss": mds_k,
        "osds": osd_k,
    }
    keyring_files = []

    for k, v in keyring.items():
        with open(f"/tmp/{k}.keyring", "w") as fd:
            fd.write(v)
            keyring_files.append(f"/tmp/{k}.keyring")

    return keyring_files


def generate_monmap_cmd():
    """
    Generates monmap-tool command used to rebuild monitors

    """
    logger.info("Generating monmap creation command")
    mon_a = get_mon_pods()[0]
    logger.info("Getting mon pods public ip")
    mon_ips_dict = {}
    mon_pods = get_mon_pods()
    mon_ids = []
    mon_ips = []
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
    logger.info(mon_ips)
    logger.info("Getting FSID of monitor")
    fsid = (
        mon_a.get()
        .get("spec")
        .get("initContainers")[1]
        .get("args")[0]
        .replace("--fsid=", "")
    )
    for ids, ip in zip(mon_ids, mon_ips):
        ipv1 = ipv2 = ip
        ipv1 = "v1:" + ipv1[0] + ":6789"
        ipv2 = "v2:" + ipv2[0] + ":3300"
        mon_ips_dict.update({ids: f"[{ipv2},{ipv1}]"})
    mon_ip_ids = ""
    for key, val in mon_ips_dict.items():
        mon_ip_ids = mon_ip_ids + f"--addv {key} {val}" + " "
    mon_map_cmd = f"monmaptool --create {mon_ip_ids} --enable-all-features --clobber /tmp/monmap --fsid {fsid}"
    logger.info(mon_map_cmd)
    return mon_map_cmd


def get_secrets(secret_resource):
    """
    Gets ocs secrets and decodes it to get ceph keyring

    Args:
        secret_resource (any): secret resource name

    """
    keyring = ""

    osd_caps = """
        caps mgr = "allow profile osd"
        caps mon = "allow profile osd"
        caps osd = "allow *"
    """

    for resource in secret_resource:
        resource_obj = ocp.OCP(
            resource_name=resource, kind="Secret", namespace="openshift-storage"
        )

        keyring = (
            keyring
            + base64.b64decode(resource_obj.get().get("data").get("keyring"))
            .decode()
            .rstrip("\n")
            + "\n"
        )
        if "osd" in resource:
            keyring = keyring + osd_caps
    return keyring
