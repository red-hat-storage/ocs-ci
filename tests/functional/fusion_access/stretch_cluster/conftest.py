import logging

import pytest

from ocs_ci.helpers.fusion_access_helper import (
    create_baseline_file,
    create_workload_from_template,
    discover_iscsi_luns,
    get_fusion_access_storageclass,
    get_workload_pod,
    wait_for_workload_pods,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.resources.stretchcluster import StretchCluster

log = logging.getLogger(__name__)


@pytest.fixture()
def reconnect_san_teardown(request):
    """
    Log every node back into the iSCSI target once the test is over.

    The tests that take the SAN away from a zone reconnect it themselves as
    part of what they are verifying, but only if they get that far. Without
    this the cluster would be left with a zone that has no storage whenever
    one of them fails in between, and every test after it would fail too.
    Reconnecting is idempotent, the login of a node that never lost its
    session fails harmlessly with 'already exists'.

    Every node is covered and not just the workers, a data zone of a stretch
    cluster also holds a control plane node which is part of the Storage Scale
    cluster and reaches the same SAN.

    """

    def finalizer():
        node_names = [node_obj.name for node_obj in get_node_objs()]
        log.info(f"Making sure the nodes {node_names} are logged back into the SAN")
        discover_iscsi_luns(node_names=node_names)

    request.addfinalizer(finalizer)


@pytest.fixture()
def setup_fusion_access_workloads(
    request,
    setup_stretch_cluster_project,
    setup_logwriter_rbd_workload_factory,
    teardown_factory,
):
    """
    Deploy the application set used by the Fusion Access stretch cluster tests
    and take a data integrity baseline of every volume.

    The applications are:

    * logwriter: a zone aware StatefulSet, one replica per data zone, each with
      its own volume. Reuses the stretch cluster logwriter template and factory,
      only the storage class and the access mode are overridden so the volumes
      are backed by the IBM Storage Scale filesystem
    * busybox: a single replica Deployment on a shared volume
    * file-uploader: a single replica Deployment on a shared volume, exposed
      through a Service and a Route
    * TODO: a virtual machine using a standalone PVC, same as the ODF arbiter
      stretch cluster tests do through the 'cnv_workload' fixture. Blocked on
      the CNSA regression that makes QEMU fail to take OFD locks on an IBM
      Storage Scale backed disk, so no VM can boot off a Scale storage class

    Returns:
        tuple: StretchCluster object and a dict mapping each workload label to
            the md5sum of its baseline file

    """

    def factory():
        namespace = setup_stretch_cluster_project.namespace
        sc_name = get_fusion_access_storageclass()
        sc_obj = StretchCluster()

        # Zone aware logwriter StatefulSet, backed by RWX Scale volumes
        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=True,
            sc_name=sc_name,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        log.info("Logwriter workload is running")

        # busybox and file-uploader Deployments
        create_workload_from_template(
            template_path=constants.BUSYBOX_FUSION_PATH,
            namespace=namespace,
            sc_name=sc_name,
            teardown_factory=teardown_factory,
        )
        create_workload_from_template(
            template_path=constants.FILE_UPLOADER_FUSION_PATH,
            namespace=namespace,
            sc_name=sc_name,
            teardown_factory=teardown_factory,
        )
        wait_for_workload_pods(
            labels=[
                constants.BUSYBOX_FUSION_LABEL,
                constants.FILE_UPLOADER_FUSION_LABEL,
            ],
            namespace=namespace,
        )
        log.info("Busybox and file-uploader workloads are running")

        # TODO: deploy a VM on a standalone PVC backed by the Scale storage
        # class and write a test file into it, once VMs can boot off a Scale
        # storage class again. See the 'cnv_workload' fixture and
        # 'verify_vm_workload' for the ODF arbiter equivalent.

        # Take a data integrity baseline of every volume
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, namespace=namespace
        )
        md5sum_before = {}
        for pod_obj in sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][0]:
            md5sum_before[pod_obj.name] = create_baseline_file(
                pod_obj, mount_path="/mnt/target"
            )

        for label, mount_path in (
            (constants.BUSYBOX_FUSION_LABEL, "/mnt/test"),
            (constants.FILE_UPLOADER_FUSION_LABEL, "/srv"),
        ):
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            md5sum_before[label] = create_baseline_file(pod_obj, mount_path=mount_path)

        log.info(f"Baseline md5sums of the workload volumes: {md5sum_before}")
        return sc_obj, md5sum_before

    return factory
