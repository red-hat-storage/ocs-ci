"""
Test to measure pvc scale creation & deletion time. Total pvc count would be 1500
"""
import logging
import pytest

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.framework.testlib import performance, E2ETest
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile

log = logging.getLogger(__name__)


@performance
class TestBulkCloneCreation(E2ETest):
    """
    Base class for PVC scale creation and deletion
    """

    @pytest.fixture()
    def namespace(self, project_factory, interface_iterate):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace
        self.interface = interface_iterate

    @pytest.mark.usefixtures(namespace.__name__)
    def test_multiple_pvc_creation_deletion_scale(
        self, namespace, tmp_path
    ):
        """
        Measuring PVC creation time while scaling PVC
        Measure PVC deletion time after creation test
        """
        pvc_count = 3
        log.info(f"Start creating {self.interface} {pvc_count} PVC")
        if self.interface == constants.CEPHBLOCKPOOL:
            sc_name = constants.DEFAULT_STORAGECLASS_RBD
        elif self.interface == constants.CEPHFILESYSTEM:
            sc_name = constants.DEFAULT_STORAGECLASS_CEPHFS

        access_mode = constants.ACCESS_MODE_RWO
        # Get pvc_dict_list, append all the pvc.yaml dict to pvc_dict_list
        pvc_dict_list = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=pvc_count, access_mode=access_mode, sc_name=sc_name
        )

        job_file = ObjectConfFile(
            name="job_profile",
            obj_dict_list=pvc_dict_list,
            project=self.namespace,
            tmp_path=tmp_path,
        )

        # Create kube_job
        job_file.create(namespace=self.namespace)

        # Check all the PVC reached Bound state
        pvc_bound_list = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file,
            namespace=self.namespace,
            no_of_pvc=pvc_count,
        )

        logging.info(f"Number of PVCs in Bound state {len(pvc_bound_list)}")

        # Get PVC creation time
        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=self.interface, pvc_name_list=pvc_bound_list
        )
        logging.info(f"Printing creation time")
        for k, v in pvc_create_time.items():
            logging.info(f"Creation time of {k} is {v}")
        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        # log_path = f"{ocsci_log_path()}/{interface}-{access_mode}"
        # with open(f"{log_path}-creation-time.csv", "w") as fd:
        #     csv_obj = csv.writer(fd)
        #     for k, v in pvc_create_time.items():
        #         csv_obj.writerow([k, v])
        # logging.info(f"Create data present in {log_path}-creation-time.csv file")

        # Get pv_name, require pv_name to fetch deletion time data from log
        # pv_name_list = list()
        # get_kube_job_1 = job_file1.get(namespace=self.namespace)
        # for i in range(int(scale_pvc_count / 2)):
        #     pv_name_list.append(get_kube_job_1["items"][i]["spec"]["volumeName"])
        #
        # get_kube_job_2 = job_file2.get(namespace=self.namespace)
        # for i in range(int(scale_pvc_count / 2)):
        #     pv_name_list.append(get_kube_job_2["items"][i]["spec"]["volumeName"])
        #
        # # Delete kube_job
        # job_file1.delete(namespace=self.namespace)
        # job_file2.delete(namespace=self.namespace)
        #
        # # Get PVC deletion time
        # pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
        #     interface=interface, pv_name_list=pv_name_list
        # )

        # Update result to csv file.
        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        # with open(f"{log_path}-deletion-time.csv", "w") as fd:
        #     csv_obj = csv.writer(fd)
        #     for k, v in pvc_deletion_time.items():
        #         csv_obj.writerow([k, v])
        # logging.info(f"Delete data present in {log_path}-deletion-time.csv file")

