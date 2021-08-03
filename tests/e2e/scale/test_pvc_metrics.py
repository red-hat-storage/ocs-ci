"""
Test to measure pvc scale creation & deletion time. Total pvc count would be
500 times the number of worker nodes
"""
import logging
import csv
import pytest
import time
from timeit import default_timer

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.framework.testlib import scale, E2ETest
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.metrics import collect_pod_metrics

log = logging.getLogger(__name__)


@scale
#
# NOTE: This code class was cannibalized from test_pvc_creation_deletion_scale.py
#       This is a WIP branch which I would like to clean up by not duplicating this code.
#
class TestScaleAndRecordMetrics(E2ETest):
    """
    Base class for testing metrics recording
    """

    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        self.start_time = default_timer()
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.mark.parametrize(
        argnames=["access_mode", "interface"],
        argvalues=[
            pytest.param(
                *[constants.ACCESS_MODE_RWO, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1225"),
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2010"),
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHFS_INTERFACE],
                marks=pytest.mark.polarion_id("OCS-2008"),
            ),
        ],
    )
    @pytest.mark.usefixtures(namespace.__name__)
    def test_multiple_pvc_creation_deletion_scale(
        self, es, namespace, tmp_path, access_mode, interface
    ):
        """
        Measuring PVC creation time while scaling PVC
        Measure PVC deletion time after creation test
        """
        crd_data = templating.load_yaml(constants.OSD_SCALE_BENCHMARK_YAML)
        our_uuid = uuid4().hex
        self.elastic_info = ElasticData(our_uuid, crd_data)
        self.elastic_info.es_connect()
        metrics = collect_pod_metrics()
        logging.info("Beginning of run METRICS collection")
        logging.info(metrics)
        scale_pvc_count = scale_lib.get_max_pvc_count()
        log.info(f"Start creating {access_mode}-{interface} {scale_pvc_count} PVC")
        if interface == constants.CEPHBLOCKPOOL:
            sc_name = constants.DEFAULT_STORAGECLASS_RBD
        elif interface == constants.CEPHFS_INTERFACE:
            sc_name = constants.DEFAULT_STORAGECLASS_CEPHFS

        # Get pvc_dict_list, append all the pvc.yaml dict to pvc_dict_list
        pvc_dict_list1 = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=int(scale_pvc_count / 2), access_mode=access_mode, sc_name=sc_name
        )
        pvc_dict_list2 = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=int(scale_pvc_count / 2), access_mode=access_mode, sc_name=sc_name
        )

        # There is 2 kube_job to reduce the load, observed time_out problems
        # during delete process of single kube_job and heavy load.
        job_file1 = ObjectConfFile(
            name="job_profile_1",
            obj_dict_list=pvc_dict_list1,
            project=self.namespace,
            tmp_path=tmp_path,
        )
        job_file2 = ObjectConfFile(
            name="job_profile_2",
            obj_dict_list=pvc_dict_list2,
            project=self.namespace,
            tmp_path=tmp_path,
        )

        # Create kube_job
        job_file1.create(namespace=self.namespace)
        job_file2.create(namespace=self.namespace)

        # Check all the PVC reached Bound state
        pvc_bound_list = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file1,
            namespace=self.namespace,
            no_of_pvc=int(scale_pvc_count / 2),
        )
        pvc_bound_list.extend(
            scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
                kube_job_obj=job_file2,
                namespace=self.namespace,
                no_of_pvc=int(scale_pvc_count / 2),
            )
        )

        logging.info(f"Number of PVCs in Bound state {len(pvc_bound_list)}")

        # Get PVC creation time
        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=interface,
            pvc_name_list=pvc_bound_list,
            wait_time=300,
        )

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        log_path = f"{ocsci_log_path()}/{interface}-{access_mode}"
        with open(f"{log_path}-creation-time.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in pvc_create_time.items():
                csv_obj.writerow([k, v])
        logging.info(f"Create data present in {log_path}-creation-time.csv file")

        # Get pv_name, require pv_name to fetch deletion time data from log
        pv_name_list = list()
        get_kube_job_1 = job_file1.get(namespace=self.namespace)
        for i in range(int(scale_pvc_count / 2)):
            pv_name_list.append(get_kube_job_1["items"][i]["spec"]["volumeName"])

        get_kube_job_2 = job_file2.get(namespace=self.namespace)
        for i in range(int(scale_pvc_count / 2)):
            pv_name_list.append(get_kube_job_2["items"][i]["spec"]["volumeName"])

        # Delete kube_job
        job_file1.delete(namespace=self.namespace)
        job_file2.delete(namespace=self.namespace)

        # Adding 1min wait time for PVC deletion logs to be updated
        # Observed failure when we immediately check the logs for pvc delete time
        # https://github.com/red-hat-storage/ocs-ci/issues/3371
        time.sleep(60)

        # Get PVC deletion time
        pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=interface, pv_name_list=pv_name_list
        )

        # Update result to csv file.
        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        with open(f"{log_path}-deletion-time.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in pvc_deletion_time.items():
                csv_obj.writerow([k, v])
        logging.info(f"Delete data present in {log_path}-deletion-time.csv file")
        end_time = default_timer()
        logging.info(f"Elapsed time -- {end_time - self.start_time} seconds")
        metrics = collect_pod_metrics()
        logging.info("End of Run METRICS collection")
        logging.info(metrics)
