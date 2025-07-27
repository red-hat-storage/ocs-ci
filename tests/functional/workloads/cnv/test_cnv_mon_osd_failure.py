import random
import time

import pytest
import logging


from ocs_ci.framework.pytest_customization.marks import polarion_id, magenta_squad
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs.resources.pod import get_deployment_name
from ocs_ci.ocs import constants
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm, run_dd_io
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.pod import get_osd_pods

logger = logging.getLogger(__name__)


@magenta_squad
@pytest.mark.usefixtures("setup_cnv_workload")
class TestMonAndOSDFailures:
    """
    Here we test the Mon and OSD failures while CNV
    workloads are being run in the background.
        * Run the workloads in the session scoped fixture setup
        * Execute the mon & osd failure tests
        * Verify the IO during the teardown

    """

    @pytest.fixture(scope="class")
    def setup_cnv_workload(
        self, request, project_factory_class, multi_cnv_workload, setup_cnv
    ):
        """
        Set up CNV workload and create initial data.
        """
        logger.info("Setting up CNV workload and creating initial data")

        file_paths = ["/source_file.txt", "/new_file.txt"]
        self.proj_obj = project_factory_class()
        (
            self.vm_objs_def,
            self.vm_objs_aggr,
            _,
            _,
        ) = multi_cnv_workload(namespace=self.proj_obj.namespace)
        self.all_vms = self.vm_objs_def + self.vm_objs_aggr
        source_csums = {
            vm_obj.name: run_dd_io(vm_obj, file_path=file_paths[0], verify=True)
            for vm_obj in self.all_vms
        }

        def finalizer():
            for vm_obj in self.all_vms:
                vm_obj.wait_for_ssh_connectivity()
                md5sum_after = cal_md5sum_vm(vm_obj, file_path=file_paths[0])
                assert (
                    source_csums[vm_obj.name] == md5sum_after
                ), "Data integrity of the file inside VM is not maintained during the failure"
                logger.info(
                    "Data integrity of the file inside VM is maintained during the failure"
                )

                run_dd_io(vm_obj=vm_obj, file_path=file_paths[1])
                vm_obj.stop()

        request.addfinalizer(finalizer)

    def verify_vm_status(self):
        """
        Verify the status of randomly selected VMs.
        """
        vm_samples = random.sample(self.all_vms, 3)
        for vm in vm_samples:
            assert (
                vm.printableStatus() == constants.VM_RUNNING
            ), f"{vm.name} did not reach the running state."

        for vm in vm_samples:
            vm.verify_vm(verify_ssh=True)

    @polarion_id("OCS-6609")
    def test_single_mon_failures(self):
        """
        Test single mon failure with cephFS/RBD workloads running in the background

        """
        ceph_obj = CephCluster()
        logger.info("testing single mon failures scenario")

        self.mons = ceph_obj.get_mons_from_cluster()[:1]

        # Scale Down Mon Count to replica=0
        for mon in self.mons:
            modify_deployment_replica_count(mon, 0)

        # Sleeping for 600 seconds to emulate a condition where the 2 mons is inaccessibe  for 10 seconds.
        time.sleep(600)

        # Verify vm statuses when mon pod is down
        self.verify_vm_status()

        # scale mon deployment back to 1
        for mon in self.mons:
            modify_deployment_replica_count(mon, 1)

    @polarion_id("OCS-6609")
    def test_both_mon_failure(self):
        """
        Test both data zone mon failure with cnv workloads running in the background

        """
        ceph_obj = CephCluster()

        self.mons = ceph_obj.get_mons_from_cluster()[:2]

        # Scale Down Mon Count to replica=0
        for mon in self.mons:
            modify_deployment_replica_count(mon, 0)

        # Sleeping for 600 seconds to emulate a condition where the 2 mons is inaccessibe  for 10 seconds.
        time.sleep(600)

        # Verify vm statuses when mon pod is down
        self.verify_vm_status()

        # scale mon deployment back to 1
        logger.info(f"Scaling up mons {','.join(self.mons)}")
        for mon in self.mons:
            modify_deployment_replica_count(mon, 1)

    @polarion_id("OCS-6608")
    def test_single_osd_failure(self):
        """
        Test single osd failure with cephFS/RBD workloads running in the background

        """
        logger.info("testing single osd failure scenarios")

        self.osd_pods = get_osd_pods
        osd_pod_to_fail = random.choice(self.osd_pods).name
        osd_dep = get_deployment_name(osd_pod_to_fail)

        # scale down the osd deployment to 0
        logger.info(f"Failing osd by scaling down osd deployment {osd_dep}")
        if modify_deployment_replica_count(osd_dep, 0):
            time.sleep(600)

        # Verify vm statuses when osd pod is down
        self.verify_vm_status()

        # scale the deployment back to 1
        logger.info(f"Recovering osd by scaling up osd deployment {osd_dep}")
        modify_deployment_replica_count(osd_dep, 1)
