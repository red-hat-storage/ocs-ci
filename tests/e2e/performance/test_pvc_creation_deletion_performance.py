"""
Test to verify PVC deletion performance
"""
import logging
import datetime
import pytest
import ocs_ci.ocs.exceptions as ex
import threading
import statistics

from ocs_ci.framework.testlib import performance, E2ETest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import defaults, constants
from ocs_ci.utility.performance_dashboard import push_to_pvc_time_dashboard


log = logging.getLogger(__name__)


@performance
class TestPVCCreationDeletionPerformance(E2ETest):
    """
    Test to verify PVC creation and deletion performance
    """

    @pytest.fixture()
    def base_setup(self, interface_iterate, storageclass_factory, pod_factory):
        """
        A setup phase for the test

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
            pod_factory: A fixture to create new pod
        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)
        self.pod_factory = pod_factory

    @pytest.mark.parametrize(
        argnames=["pvc_size"],
        argvalues=[
            pytest.param(*["25Gi"], marks=pytest.mark.polarion_id("OCS-2007")),
            pytest.param(*["50Gi"], marks=pytest.mark.polarion_id("OCS-2007")),
            pytest.param(*["100Gi"], marks=pytest.mark.polarion_id("OCS-2007")),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_creation_deletion_measurement_performance(
        self, teardown_factory, pvc_size
    ):
        """
        Measuring PVC creation and deletion times for pvc samples
        Verifying that those times are within required limits
        """

        num_of_samples = 5
        accepted_creation_time = 1
        accepted_deletion_time = 2 if self.interface == constants.CEPHFILESYSTEM else 1
        creation_time_measures = []
        deletion_time_measures = []
        msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."

        for i in range(num_of_samples):
            logging.info(f"{msg_prefix} Start creating PVC number {i + 1}.")
            start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            pvc_obj = helpers.create_pvc(sc_name=self.sc_obj.name, size=pvc_size)
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()

            creation_time = performance_lib.measure_pvc_creation_time(
                self.interface, pvc_obj.name, start_time
            )

            logging.info(
                f"{msg_prefix} PVC number {i + 1} was created in {creation_time} seconds."
            )
            if creation_time > accepted_creation_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} PVC creation time is {creation_time} and is greater than "
                    f"{accepted_creation_time} seconds."
                )
            creation_time_measures.append(creation_time)

            pv_name = pvc_obj.backed_pv
            pvc_reclaim_policy = pvc_obj.reclaim_policy

            pod_obj = self.write_file_on_pvc(pvc_obj)
            pod_obj.delete(wait=True)
            teardown_factory(pvc_obj)
            logging.info(f"{msg_prefix} Start deleting PVC number {i + 1}")
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(pvc_obj.backed_pv)
            deletion_time = helpers.measure_pvc_deletion_time(self.interface, pv_name)
            logging.info(
                f"{msg_prefix} PVC number {i + 1} was deleted in {deletion_time} seconds."
            )
            if deletion_time > accepted_deletion_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} PVC deletion time is {deletion_time} and is greater than "
                    f"{accepted_deletion_time} seconds."
                )

            deletion_time_measures.append(deletion_time)

        self.process_measurements(
            creation_time_measures, deletion_time_measures, msg_prefix
        )

    def process_measurements(
        self, creation_time_measures, deletion_time_measures, msg_prefix
    ):
        """
           Analyses the PVC creation and deletion times. If these times, for both creation and deletion, are within
           the given limits and the standard deviation is smaller than the predefined accepted one,
           writes them to the codespeed dashboard. Otherwise, fails the test
        Args:
            creation_time_measures: A list of PVC creation time measurements
            deletion_time_measures: A list of PVC deletion time measurements
            msg_prefix: A string for comprehensive logging

        """
        accepted_creation_deviation_percent = 50
        accepted_deletion_deviation_percent = 50

        creation_average = statistics.mean(creation_time_measures)
        log.info(
            f"{msg_prefix} The average creation time for the sampled {len(creation_time_measures)} "
            f"PVCs is {creation_average} seconds."
        )

        deletion_average = statistics.mean(deletion_time_measures)
        log.info(
            f"{msg_prefix}The average deletion time for the sampled {len(deletion_time_measures)} PVCs "
            f"is {deletion_average} seconds."
        )

        creation_st_deviation = statistics.stdev(creation_time_measures)
        creation_st_deviation_percent = creation_st_deviation / creation_average * 100.0
        if creation_st_deviation_percent > accepted_creation_deviation_percent:
            raise ex.PerformanceException(
                f"{msg_prefix} PVC creation time deviation is {creation_st_deviation_percent}% "
                f"and is greater than the allowed {accepted_creation_deviation_percent}%."
            )

        log.info(
            f"{msg_prefix} The standard deviation percent for creation of {len(deletion_time_measures)} sampled "
            f"PVCs is {creation_st_deviation_percent}%."
        )

        deletion_st_deviation = statistics.stdev(deletion_time_measures)
        deletion_st_deviation_percent = deletion_st_deviation / deletion_average * 100.0
        if deletion_st_deviation_percent > accepted_deletion_deviation_percent:
            raise ex.PerformanceException(
                f"{msg_prefix} PVC deletion time deviation is {deletion_st_deviation_percent}%"
                f"and is greater than the allowed {accepted_deletion_deviation_percent}%."
            )

        log.info(
            f"{msg_prefix} The standard deviation percent for deletion of {len(deletion_time_measures)} sampled PVCs "
            f"is {deletion_st_deviation_percent}%."
        )

        # all the results are OK, the test passes, push the results to the codespeed
        push_to_pvc_time_dashboard(self.interface, "1-pvc-creation", creation_average)
        push_to_pvc_time_dashboard(self.interface, "1-pvc-deletion", deletion_average)

    def write_file_on_pvc(self, pvc_obj, filesize=10):
        """
        Writes a file on given PVC
        Args:
            pvc_obj: PVC object to write a file on
            filesize: size of file to write (in GB)

        Returns:
            Pod on this pvc on which the file was written
        """
        pod_obj = self.pod_factory(
            interface=self.interface, pvc=pvc_obj, status=constants.STATUS_RUNNING
        )

        # filesize to be written is always 10 GB
        file_size = f"{int(filesize * 1024)}M"

        log.info(f"Starting IO on the POD {pod_obj.name}")
        # Going to run only write IO
        pod_obj.fillup_fs(size=file_size, fio_filename=f"{pod_obj.name}_file")

        # Wait for fio to finish
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert (
            err_count == 0
        ), f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
        log.info("IO on the PVC has finished")
        return pod_obj

    @pytest.mark.usefixtures(base_setup.__name__)
    def test_multiple_pvc_deletion_measurement_performance(self, teardown_factory):
        """
        Measuring PVC deletion time of 120 PVCs in 180 seconds

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
        Returns:

        """
        number_of_pvcs = 120
        pvc_size = "1Gi"
        msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."

        log.info(f"{msg_prefix} Start creating new 120 PVCs")

        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=pvc_size,
        )

        for pvc_obj in pvc_objs:
            pvc_obj.reload()
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)

        pod_objs = []
        for pvc_obj in pvc_objs:
            pod_obj = self.write_file_on_pvc(pvc_obj, 0.3)
            pod_objs.append(pod_obj)

        # Get pvc_name, require pvc_name to fetch deletion time data from log
        threads = list()
        for pvc_obj in pvc_objs:
            process = threading.Thread(target=pvc_obj.reload)
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

        pvc_name_list, pv_name_list = ([] for i in range(2))
        threads = list()
        for pvc_obj in pvc_objs:
            process1 = threading.Thread(target=pvc_name_list.append(pvc_obj.name))
            process2 = threading.Thread(target=pv_name_list.append(pvc_obj.backed_pv))
            process1.start()
            process2.start()
            threads.append(process1)
            threads.append(process2)
        for process in threads:
            process.join()
        log.info(f"{msg_prefix} Preparing to delete 120 PVC")

        # Delete PVC
        for pvc_obj, pod_obj in zip(pvc_objs, pod_objs):
            pod_obj.delete(wait=True)
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)

        # Get PVC deletion time
        pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=self.interface, pv_name_list=pv_name_list
        )
        log.info(
            f"{msg_prefix} {number_of_pvcs} bulk deletion time is {pvc_deletion_time}"
        )

        # accepted deletion time is 2 secs for each PVC
        accepted_pvc_deletion_time = number_of_pvcs * 2

        for del_time in pvc_deletion_time.values():
            if del_time > accepted_pvc_deletion_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} {number_of_pvcs} PVCs deletion time is {pvc_deletion_time.values()} and is "
                    f"greater than {accepted_pvc_deletion_time} seconds"
                )

        logging.info(f"{msg_prefix} {number_of_pvcs} PVCs deletion times are:")
        for name, time in pvc_deletion_time.items():
            logging.info(f"{name} deletion time is: {time} seconds")
