import logging
import time
import threading

import numpy
import pytest

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
)
from ocs_ci.ocs.cluster_load import ClusterLoad

log = logging.getLogger(__name__)


@ignore_leftovers
class TestGoldmanWorkload(ManageTest):
    @pytest.fixture()
    def setup(
        self,
        request,
        project_factory_session,
        service_account_factory_session,
        pvc_factory_session,
        pod_factory_session,
        io_load=0.2,
    ):
        self.cluster_load_obj = ClusterLoad(
            project_factory=project_factory_session,
            sa_factory=service_account_factory_session,
            pvc_factory=pvc_factory_session,
            pod_factory=pod_factory_session,
            target_percentage=io_load,
        )
        self.latency_samples = list()

        # Run 1 FIO pod in the test background
        self.cluster_load_obj.increase_load(rate="15M")

        def finalizer():
            """
            Stop the thread that executed watch_load()

            """
            self.cluster_load_obj.decrease_load()
            if config.RUN["load_status"] == "running":
                config.RUN["load_status"] = "finished"
            if self.thread:
                self.thread.join()

        request.addfinalizer(finalizer)

    def test_goldman_workload(self, pvc_factory, pod_factory):
        """"""
        self.pvc_objs = list()
        self.pod_objs = list()

        self.accepted_latency = 10
        self.accepted_pvc_creation_time = 10
        self.accepted_pvc_deletion_time = 30

        self.pvc_creation_time_list = list()
        self.pvc_deletion_time_list = list()

        def track_latency():
            """"""
            while config.RUN["load_status"] != "finished":
                time.sleep(5)
                try:
                    self.latency_samples.append(
                        self.cluster_load_obj.get_query(
                            constants.LATENCY_QUERY, mute_logs=True
                        )
                        * 1000
                    )

                except Exception:
                    continue

        self.thread = threading.Thread(target=track_latency)
        self.thread.start()

        time_to_wait = 60 * 5
        time_before = time.time()
        while time.time() > time_before + time_to_wait:
            for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
                pvc_obj = pvc_factory(interface)
                self.pvc_objs.append(pvc_obj)
                pod_obj = pod_factory(pvc=pvc_obj, interface=interface)
                self.pod_objs.append(pod_obj)

                pvc_create_time = helpers.measure_pvc_creation_time(
                    interface, pvc_obj.name
                )
                logging.info(f"PVC {pvc_obj.name} created in {pvc_create_time} seconds")
                self.pvc_creation_time_list.append(pvc_create_time)

                pod_obj.run_io(
                    storage_type="fs", size="1G", runtime=30, verify="sha1", do_verify=1
                )
                pod_obj.get_fio_results()

            for pvc_obj in self.pvc_objs:
                pvc_name = pvc_obj.name
                if pvc_obj.provisioner == "openshift-storage.rbd.csi.ceph.com":
                    interface = constants.CEPHBLOCKPOOL
                elif pvc_obj.provisioner == "openshift-storage.cephfs.csi.ceph.com":
                    interface = constants.CEPHFILESYSTEM

                pvc_obj.delete()
                pvc_obj.ocp.wait_for_delete(pvc_obj.name)
                pvc_delete_time = helpers.measure_pvc_deletion_time(interface, pvc_name)
                logging.info(f"PVC {pvc_name} deleted in {pvc_create_time} seconds")
                self.pvc_deletion_time_list.append(pvc_delete_time)

        self.thread.join()

        latency_array = numpy.array(self.latency_samples)
        p99_latency = numpy.percentile(latency_array, 99)

        pvc_create_array = numpy.array(self.pvc_creation_time_list)
        p99_pvc_creation_time = numpy.percentile(pvc_create_array, 99)

        pvc_delete_array = numpy.array(self.pvc_deletion_time_list)
        p99_pvc_deletion_time = numpy.percentile(pvc_delete_array, 99)

        assert (
            p99_latency > self.accepted_latency
        ), f"The p99 latency is too high: {p99_latency}"
        assert (
            p99_pvc_creation_time > self.accepted_pvc_creation_time
        ), f"The p99 PVC creation time is too high: {p99_pvc_creation_time}"
        assert (
            p99_pvc_deletion_time > self.accepted_pvc_deletion_time
        ), f"The p99 PVC deletion time is too high: {p99_pvc_deletion_time}"
