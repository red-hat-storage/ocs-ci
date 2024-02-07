import logging
import pytest
import csv

from ocs_ci.ocs import constants, scale_noobaa_lib
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import mcg
from ocs_ci.framework.testlib import scale, E2ETest
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs.utils import oc_get_all_obc_names

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    def finalizer():
        scale_noobaa_lib.cleanup(config.ENV_DATA["cluster_namespace"])

    request.addfinalizer(finalizer)


@mcg
@scale
class TestScaleOCBCreateDelete(E2ETest):
    """
    OBC scale creation and deletion using Multi cloud Object Gateway
    * Creating up to max support number of OBCs, capture creation time in sec.
    * Deleting OBCs and capture deleting time in sec.

    """

    namespace = config.ENV_DATA["cluster_namespace"]
    scale_obc_count = 500
    num_obc_batch = 50

    @pytest.mark.polarion_id("OCS-2667")
    def test_scale_obc_create_delete_time(self, tmp_path):
        """
        MCG OBC creation and deletion using Noobaa MCG storage class

        """

        log.info(
            f"Start creating  {self.scale_obc_count} "
            f"OBCs in a batch of {self.num_obc_batch}"
        )
        obc_create = dict()
        obc_delete = dict()
        for i in range(int(self.scale_obc_count / self.num_obc_batch)):
            obc_dict_list = (
                scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
                    no_of_obc=self.num_obc_batch,
                    sc_name=constants.NOOBAA_SC,
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

            # Check all the OBCs to reach Bound state
            obc_bound_list = (
                scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
                    kube_job_obj=job_file,
                    namespace=self.namespace,
                    no_of_obc=self.num_obc_batch,
                )
            )
            log.info(f"Number of OBCs in Bound state {len(obc_bound_list)}")

            # Measure obc creation and deletion time
            obc_creation_time = scale_noobaa_lib.measure_obc_creation_time(
                obc_name_list=obc_bound_list
            )
            obc_create.update(obc_creation_time)

        # Delete all obcs in a batch
        obc_name_list = list(oc_get_all_obc_names())
        new_list = [
            obc_name_list[i : i + 20]
            for i in range(0, len(obc_name_list), self.num_obc_batch)
        ]

        for i in range(len(new_list)):
            scale_noobaa_lib.cleanup(self.namespace, obc_list=new_list[i])
            obc_deletion_time = scale_noobaa_lib.measure_obc_deletion_time(
                obc_name_list=new_list[i]
            )
            obc_delete.update(obc_deletion_time)

        # Store obc creation time on csv file
        log_path = f"{ocsci_log_path()}/obc-creation"
        with open(f"{log_path}-{constants.NOOBAA_SC}.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in obc_create.items():
                csv_obj.writerow([k, v])
        log.info(f"OBC creation data present in {log_path}-{constants.NOOBAA_SC}.csv")

        # Store obc deletion time on csv file
        log_path = f"{ocsci_log_path()}/obc-deletion"
        with open(f"{log_path}-{constants.NOOBAA_SC}.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in obc_create.items():
                csv_obj.writerow([k, v])
        log.info(f"OBC deletion data present in {log_path}-{constants.NOOBAA_SC}.csv")
