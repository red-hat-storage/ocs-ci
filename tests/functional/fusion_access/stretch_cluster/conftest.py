import logging

import pytest

from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_fio
from ocs_ci.helpers.fusion_access_helper import (
    create_baseline_file,
    create_workload_from_template,
    get_fusion_access_storageclass,
    get_workload_pod,
    verify_vm_fio_running,
    wait_for_workload_pods,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.stretchcluster import StretchCluster

log = logging.getLogger(__name__)


@pytest.fixture()
def setup_fusion_access_workloads(
    request,
    setup_stretch_cluster_project,
    setup_logwriter_rbd_workload_factory,
    teardown_factory,
    cnv_workload,
    setup_cnv,
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
    * virtual machine: booted off a standalone PVC on the IBM Storage Scale
      storage class, same as the ODF arbiter stretch cluster tests do through
      the 'cnv_workload' fixture. A test file is written into it and a
      continuous fio workload is started, so there is I/O going on while the
      failure is injected

    Returns:
        tuple: StretchCluster object, a dict mapping each workload label to
            the md5sum of its baseline file and the VirtualMachine object

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

        # VM on a standalone PVC backed by the Scale storage class, with a test
        # file written into it and continuous fio I/O running
        vm_obj = cnv_workload(
            volume_interface=constants.VM_VOLUME_PVC,
            access_mode=constants.ACCESS_MODE_RWX,
            storageclass=sc_name,
            namespace=namespace,
        )
        vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
        )
        vm_md5sum = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        log.info(f"VM {vm_obj.name} test file md5sum: {vm_md5sum}")

        # Continuous I/O. The fio service is restarted by systemd if it exits,
        # so it keeps writing for the whole test, including after the VM is
        # rebooted on another node
        run_fio(vm_obj, filename="/test/fio_testfile")
        verify_vm_fio_running(vm_obj)
        log.info(f"VM {vm_obj.name} is running with continuous fio I/O")

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

        md5sum_before[vm_obj.name] = vm_md5sum
        log.info(f"Baseline md5sums of the workload volumes: {md5sum_before}")
        return sc_obj, md5sum_before, vm_obj

    return factory
