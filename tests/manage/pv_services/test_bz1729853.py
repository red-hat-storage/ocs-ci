# -*- coding: utf8 -*-

import logging
import tempfile
import textwrap
import yaml

import pytest

from ocs_ci.framework.pytest_customization.marks import tier3
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from tests import helpers


logger = logging.getLogger(__name__)


def get_pvc_dict(name):
    """
    Creates new copy of PVC dict to tweak and use.
    """
    template = textwrap.dedent(f"""
        kind: PersistentVolumeClaim
        apiVersion: v1
        metadata:
          name: {name}
        spec:
          storageClassName: ocs-storagecluster-ceph-rbd
          accessModes:
           - ReadWriteOnce
          resources:
            requests:
              storage: '2Gi'
        """)
    pvc_dict = yaml.safe_load(template)
    return pvc_dict


def get_deploymentconfig_dict(name, pvc_name):
    template = textwrap.dedent(f"""
        kind: 'DeploymentConfig'
        apiVersion: 'v1'
        metadata:
          name: {name}
        spec:
          template:
            metadata:
              labels:
                name: {name}
            spec:
              restartPolicy: 'Always'
              volumes:
              - name: 'cirros-vol'
                persistentVolumeClaim:
                  claimName: {pvc_name}
              containers:
              - name: 'cirros'
                image: 'cirros'
                imagePullPolicy: 'IfNotPresent'
                volumeMounts:
                - mountPath: '/mnt'
                  name: 'cirros-vol'
                command: ['sh']
                args:
                - '-ec'
                - 'while true; do
                       (mount | grep /mnt) && (head -c 1048576 < /dev/urandom > /mnt/random-data.log) || exit 1;
                       sleep 20 ;
                   done'
                livenessProbe:
                  exec:
                    command:
                    - 'sh'
                    - '-ec'
                    - 'mount | grep /mnt && head -c 1024 < /dev/urandom >> /mnt/random-data.log'
                  initialDelaySeconds: 3
                  periodSeconds: 3
          replicas: 1
          triggers:
            - type: 'ConfigChange'
          paused: false
          revisionHistoryLimit: 2
        """)
    dc_dict = yaml.safe_load(template)
    return dc_dict


@tier3
@pytest.mark.polarion_id("OCS-278")
@pytest.mark.bugzilla("1729853")
@pytest.mark.bugzilla("1716276")
def test_bz1729853(tmp_path):
    """
    Test covers a bulk delete of CSI based rbd volumes through project delete
    leaves behind some undeleted pvs, with case reported in BZ 1716276 in mind
    (which is why this deals with RBD only).
    """
    total_runs = 10
    for pn in range(1, total_runs + 1):
        namespace = f"bz-1729853-{pn:02d}"
        logger.info(
            f"creating new project {namespace} ({pn} out of {total_runs})")
        project = ocp.OCP(kind='Project', namespace=namespace)
        project.new_project(namespace)

        ocp_pvc = ocp.OCP(kind=constants.PVC, namespace=namespace)
        ocp_dc = ocp.OCP(kind=constants.DEPLOYMENTCONFIG, namespace=namespace)

        # create few PVCs in the project
        total_vols = 100
        logger.info((
            f"now we are going to create {total_vols} "
            "PVC and DeploymentConfig pairs"))
        pvc_list = []
        for i in range(1, total_vols + 1):
            pvc_name = f"{namespace}-pvc-{i:03d}"
            pvc_list.append(pvc_name)
            pvc_dict = get_pvc_dict(pvc_name)
            dc_name = f"{namespace}-dc-{i:03d}"
            dc_dict = get_deploymentconfig_dict(dc_name, pvc_name)
            ocf = ObjectConfFile(
                f"{namespace}-{i:03d}", [pvc_dict, dc_dict], project, tmp_path)
            ocf.create()

        logger.info(
            f"now we are going to wait for {total_vols} PVCs to be Bound")
        # Note: wait_for_resource() method could be a bit faster,
        # reported as https://github.com/red-hat-storage/ocs-ci/issues/778
        ocp_pvc.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_count=total_vols,
            timeout=total_vols*30)

        # using https://github.com/red-hat-storage/ocs-ci/pull/1077 which
        # fixes https://github.com/red-hat-storage/ocs-ci/issues/792
        logger.info(
            f"now we are going to wait for {total_vols} DCs to be Running")
        ocp_dc.wait_for_resource(
            condition="1",
            column='CURRENT',
            resource_count=total_vols,
            timeout=total_vols*30)

        logger.info(f"initiating delete of project {namespace}")
        # note that this just initializes the deletion, it doesn't wait for all
        # resources in the namespace to be deleted (which is exactly what we
        # need to do here)
        project.delete(resource_name=namespace)
        logger.info(f"deletion of project {namespace} finished")

        # Wait for all PVCs in the test namespace to be gone
        for pvc_name in pvc_list:
            logger.info(f"waiting for PVC {pvc_name} to be deleted")
            ocp_pvc.wait_for_delete(resource_name=pvc_name, timeout=180)
        # Wait for all PVs in the test namespace to be gone as well
        ocp_pv = ocp.OCP(kind=constants.PV, namespace=namespace)
        for pvc_name in pvc_list:
            logger.info(f"waiting for PV {pvc_name} to be deleted")
            try:
                ocp_pv.wait_for_delete(resource_name=pvc_name, timeout=180)
            except TimeoutError:
                msg = (
                    "PV {pvc_name} failed to be deleted, "
                    "we will recheck that again anyway")
                logger.warning(msg)
        logger.info("rechecking that PVs are gone (again) at this point")
        pv_obj_list = ocp_pv.get(all_namespaces=True)['items']
        # and preparing list of PVs using ocs-storagecluster-ceph-rbd for
        # the final check
        pv_list = []  # all PVs of ocs-storagecluster-ceph-rbd sc
        undeleted_pv_list = []  # list of undeleted PVs created by the test
        for pv in pv_obj_list:
            pv_name = pv['metadata']['name']
            if pv_name.startswith("bz-1729853"):
                logger.error("volume {pv_name} was not deleted!")
                undeleted_pv_list.append(pv_name)
            sc_name = pv['spec']['storageClassName']
            if sc_name == "ocs-storagecluster-ceph-rbd":
                pv_list.append(pv_name)

        # Now check that the number of PVs which uses RBD storage class
        # matches ceph side via `rbd ls -p ocs-storagecluster-ceph-rbd`
        # so that we know that the numbers in openshift and ceph match.
        logger.info("checking that number of PVs and RBDs matches")
        ct_pod = get_ceph_tools_pod()
        rbd_out = ct_pod.exec_cmd_on_pod(
            "rbd ls -p ocs-storagecluster-cephblockpool",
            out_yaml_format=True)
        # rbd tool doesn't have a yaml output feature
        rbd_list = rbd_out.split(" ")
        logger.info(f"PV list: {pv_list}")
        logger.info(f"RBD list: {rbd_list}")

        msg_leftovers = "There should be no undeleted PVs"
        assert undeleted_pv_list == [], msg_leftovers

        msg_match = "number of PVs and RBDs should match"
        assert len(pv_list) == len(rbd_list), msg_match
