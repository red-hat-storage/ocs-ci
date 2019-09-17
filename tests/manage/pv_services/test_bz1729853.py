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


# TODO: this is a hack, move this functionality (creating a resource from a
# dict) into OCP class
def ocp_create(ocp_obj, resource_dict):
    with tempfile.NamedTemporaryFile(prefix='ocs-ci') as tf:
        tf.write(yaml.dump(resource_dict).encode())
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

    total_runs = 3
    for pn in range(1, total_runs + 1):
        namespace = f"bz-1729853-{pn:02d}"
        logger.info(
            f"creating new project {namespace} ({pn} out of {total_runs})")
        project = ocp.OCP(kind='Project', namespace=namespace)
        project.new_project(namespace)

        # create few PVcs in the project
        ocp_pvc = ocp.OCP(kind=constants.PVC, namespace=namespace)
        total_vols = 100
        logger.info(
            f"now we are going to create {total_vols} PVCs using {rbd_sc.name}")
        for i in range(1, total_vols + 1):
            pvc_name = f"{namespace}-pvc-{i:03d}"
            pvc_dict = get_pvc_dict(pvc_name, rbd_sc.name)
            ocp_create(ocp_pvc, pvc_dict)

        logger.info(
            f"now we are going to wait for {total_vols} PVCs to be Bound")
        # Note: wait_for_resource() method could be a bit faster,
        # reported as https://github.com/red-hat-storage/ocs-ci/issues/778
        ocp_pvc.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_count=100,
            timeout=100*30)

        logger.info(f"initiating delete of project {namespace}")
        # TODO: don't wait for completion ...
        project.delete(resource_name=namespace)

        # TODO: wait, checking the status
