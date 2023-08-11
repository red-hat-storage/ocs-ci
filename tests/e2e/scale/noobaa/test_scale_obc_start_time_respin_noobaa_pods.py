import logging
import time

import pytest
import csv

from ocs_ci.ocs import constants, scale_noobaa_lib
from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import scale, E2ETest
from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    def finalizer():
        scale_noobaa_lib.cleanup(constants.OPENSHIFT_STORAGE_NAMESPACE)

    request.addfinalizer(finalizer)


@orange_squad
@scale
class TestScaleOBCStartTime(E2ETest):

    namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
    scale_obc_count = 10
    scale_obc_count_io = 2
    num_obc_batch = 5
    nb_pod_start_time = dict()

    @pytest.mark.parametrize(
        argnames=["pod_name", "sc_name"],
        argvalues=[
            pytest.param(
                *["noobaa-core", constants.NOOBAA_SC],
                marks=[
                    pytest.mark.polarion_id("OCS-5127"),
                ],
            ),
            pytest.param(
                *["noobaa-db", constants.NOOBAA_SC],
                marks=[
                    pytest.mark.polarion_id("OCS-5128"),
                ],
            ),
        ],
    )
    def test_scale_obc_start_time_noobaa_pod_respin(
        self, tmp_path, pod_name, sc_name, mcg_job_factory, timeout=5
    ):
        """
        Created OBC without I/O running
        Created OBC with I/O using mcg_job_factory()
        Reset node which Noobaa pod is running on, then measure the startup time when
        Noobaa pod reaches Running state
        """

        # Create OBCs with FIO running using mcg_job_factory()
        for i in range(self.scale_obc_count_io):
            exec(f"job{i} = mcg_job_factory()")

        log.info(
            f"Start creating  {self.scale_obc_count} "
            f"OBC in a batch of {self.num_obc_batch}"
        )
        for i in range(int(self.scale_obc_count / self.num_obc_batch)):
            obc_dict_list = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=self.num_obc_batch,
                    sc_name=sc_name,
                    namespace=self.namespace,
                )
            )
            # Create job profile
            job_file = ObjectConfFile(
                name="job_profile",
                obj_dict_list=obc_dict_list,
                project=self.namespace,
                tmp_path=tmp_path,
            )
            # Create kube_job
            job_file.create(namespace=self.namespace)
            time.sleep(timeout)

        # Check all the OBCs reached Bound state
        obc_bound_list = scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file,
            namespace=self.namespace,
            no_of_obc=self.num_obc_batch,
        )
        log.info(f"Number of OBCs in Bound state: {len(obc_bound_list)}")

        # Reset node which noobaa pods is running on
        # And validate noobaa pods are re-spinned and in Running state
        pod_obj = scale_noobaa_lib.get_pod_obj(pod_name)
        scale_noobaa_lib.noobaa_running_node_restart(pod_name=pod_name)

        # Store noobaa pod start time on csv file
        pod_start_time = helpers.pod_start_time(pod_obj=pod_obj)
        log.info(
            f"{pod_name} is taking {pod_start_time} seconds to reach Running state."
        )

        self.nb_pod_start_time.update(pod_start_time)
        data_file = f"{ocsci_log_path()}/noobaa_pod_start_time.csv"
        with open(f"{data_file}", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in self.nb_pod_start_time.items():
                csv_obj.writerow([k, v])
        log.info(f"Noobaa pod(s) start time is saved in {data_file}")

        # Verify all OBCs are in Bound state after node restart
        log.info("Verify all OBCs are in Bound state after node restart.....")
        obc_status_list = scale_noobaa_lib.check_all_obcs_status(
            namespace=self.namespace
        )
        log.info(
            "Number of OBCs in Bound state after node reset: "
            f"{len(obc_status_list[0])}"
        )
        assert (
            len(obc_status_list[0]) == self.scale_obc_count
        ), "Not all OBCs in Bound state"
