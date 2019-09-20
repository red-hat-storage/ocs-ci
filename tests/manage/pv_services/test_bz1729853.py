# -*- coding: utf8 -*-

import logging
import tempfile
import textwrap
import yaml

import pytest


from ocs_ci.ocs import ocp, constants
from tests import helpers


logger = logging.getLogger(__name__)


def get_pvc_dict(name, storage_class):
    """
    Creates new copy of PVC dict to tweak and use.
    """
    template = textwrap.dedent(f"""
        kind: PersistentVolumeClaim
        apiVersion: v1
        metadata:
          name: {name}
          annotations:
            volume.beta.kubernetes.io/storage-class: {storage_class}
        spec:
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


# TODO: this is a hack, move this functionality (creating a resource from a
# dict) into OCP class
def ocp_create(ocp_obj, resource_dict):
    with tempfile.NamedTemporaryFile(prefix='ocs-ci') as tf:
        resource_str = yaml.dump(resource_dict).encode()
        tf.write(resource_str)
        tf.file.flush()
        ocp_obj.create(yaml_file=tf.name)


@pytest.mark.polarion_id("OCS-278")
@pytest.mark.bugzilla("1729853")
def test_bz1729853(storageclass_factory):
    """
    Test covers a bulk delete of CSI based rbd volumes through project delete
    leaves behind some undeleted pvs, with case reported in BZ 1716276 in mind
    (which is why this deals with RBD only).
    """
    # create cluster wide resources: a storage classe, secret ...
    rbd_sc = storageclass_factory(constants.CEPHBLOCKPOOL)

    total_runs = 1
    for pn in range(1, total_runs + 1):
        namespace = f"bz-1729853-{pn:02d}"
        logger.info(
            f"creating new project {namespace} ({pn} out of {total_runs})")
        project = ocp.OCP(kind='Project', namespace=namespace)
        project.new_project(namespace)

        # create few PVcs in the project
        ocp_pvc = ocp.OCP(kind=constants.PVC, namespace=namespace)
        ocp_dc = ocp.OCP(kind=constants.DEPLOYMENTCONFIG, namespace=namespace)
        total_vols = 100
        logger.info((
            f"now we are going to create {total_vols} "
            "PVC and DeploymentConfig pairs"
            f"using {rbd_sc.name}"))
        for i in range(1, total_vols + 1):
            pvc_name = f"{namespace}-pvc-{i:03d}"
            pvc_dict = get_pvc_dict(pvc_name, rbd_sc.name)
            ocp_create(ocp_pvc, pvc_dict)
            dc_name = f"{namespace}-dc-{i:03d}"
            dc_dict = get_deploymentconfig_dict(dc_name, pvc_name)
            ocp_create(ocp_dc, dc_dict)

        logger.info(
            f"now we are going to wait for {total_vols} PVCs to be Bound")
        # Note: wait_for_resource() method could be a bit faster,
        # reported as https://github.com/red-hat-storage/ocs-ci/issues/778
        ocp_pvc.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_count=100,
            timeout=100*30)

        # TODO: implement this wait
        # logger.info(
        #     f"now we are going to wait for {total_vols} DCs to be Running")
        # ocp_dc.wait_for_resource(
        #     condition=constants.STATUS_RUNNING,
        #     resource_count=100,
        #     timeout=100*30)

        logger.info(f"initiating delete of project {namespace}")
        # note that this just initializes the deletion, it doesn't wait for all
        # reousrces in the namespace to be deleted (which is exactly what we
        # need to do here)
        project.delete(resource_name=namespace)

        # TODO: wait, checking the status
