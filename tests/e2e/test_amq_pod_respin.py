import logging

import pytest

from ocs_ci.framework.testlib import E2ETest, google_api_required, workloads
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.exceptions import (ResourceWrongStatusException)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import utils
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from tests import disruption_helpers
from tests import helpers

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def test_fixture_amq(request, storageclass_factory):
    cephfs_sc = "ocs-storagecluster-cephfs"
    helpers.change_default_storageclass(scname=cephfs_sc)

    # Confirm that the default StorageClass is changed
    tmp_default_sc = helpers.get_default_storage_class()
    assert len(
        tmp_default_sc
    ) == 1, "More than 1 default storage class exist"
    log.info(f"Current Default StorageClass is:{tmp_default_sc[0]}")
    assert tmp_default_sc[0] == cephfs_sc, (
        "Failed to change default StorageClass"
    )
    log.info(
        f"Successfully changed the default StorageClass to "
        f"{cephfs_sc}"
    )

    amq = AMQ()
    amq.namespace = "my-project1"

    def teardown():
        amq.cleanup()

    request.addfinalizer(teardown)
    return amq


@workloads
@google_api_required
class TestAMQCephPodRespin(E2ETest):

    @pytest.fixture()
    def amq_setup(self, test_fixture_amq):
        amq = test_fixture_amq.setup_amq()
        if amq.is_amq_pod_running(pod_pattern="cluster-operator"):
            log.info("strimzi-cluster-operator pod is in running state")
        else:
            raise ResourceWrongStatusException("strimzi-cluster-operator pod is not getting to running state")

        if amq.is_amq_pod_running(pod_pattern="zookeeper"):
            log.info("my-cluster-zookeeper Pod is in running state")
        else:
            raise ResourceWrongStatusException("my-cluster-zookeeper Pod is not getting to running state")

        if amq.is_amq_pod_running(pod_pattern="my-connect-cluster-connect"):
            log.info("my-connect-cluster-connect Pod is in running state")
        else:
            raise ResourceWrongStatusException("my-connect-cluster-connect pod is not getting to running state")

        if amq.is_amq_pod_running(pod_pattern="my-bridge-bridge"):
            log.info("my-bridge-bridge Pod is in running state")
        else:
            raise ResourceWrongStatusException("my-bridge-bridge is not getting to running state")
        return amq

    @pytest.mark.parametrize(
        argnames="pod_name",
        argvalues=[
            pytest.param(
                *['mon'], marks=pytest.mark.polarion_id("OCS-1275")
            ),
            pytest.param(
                *['osd'], marks=pytest.mark.polarion_id("OCS-1276")
            ),
            pytest.param(
                *['cephfsplugin'], marks=pytest.mark.polarion_id("OCS-1277")
            ),
            pytest.param(
                *['cephfsplugin-provisioner'], marks=pytest.mark.polarion_id("OCS-1283")
            ),
        ]
    )
    @pytest.mark.usefixtures(amq_setup.__name__)
    def test_install_amq_cephfs(self, pod_name):
        """
        Testing basics: secret creation,
        storage class creation, pvc and pod with cephfs
        """
        # Respin Ceph pod
        resource_osd = [f'{pod_name}']
        log.info(f"Respin pod {pod_name}")
        disruption = disruption_helpers.Disruptions()
        for resource in resource_osd:
            disruption.set_resource(resource=resource)
            disruption.delete_resource()

        for pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, "cluster-operator", self.namespace
        ):
            try:
                if pod[0] is not None:
                    amq_pod = pod[0]
                    break
            except IndexError as ie:
                log.error("cluster-operator pod not ready yet")
                raise ie
        # checking pod status
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Running',
            resource_name=amq_pod,
            timeout=1600,
            sleep=30,
        )
        # Parsing logs
        output = run_cmd(f'oc logs {amq_pod}')
        amq_output = utils.parse_pgsql_logs(output)
        log.info(
            "*******amq  output log*********\n"
            f"{amq_output}"
        )
        for data in amq_output:
            latency_avg = data['latency_avg']
            if not latency_avg:
                raise UnexpectedBehaviour(
                    "amq failed to run, no data found on latency_avg"
                )
        log.info("amq has completed successfully")

        # Collect data and export to Google doc spreadsheet
        g_sheet = GoogleSpreadSheetAPI(sheet_name="OCS AMQ", sheet_index=1)
        for lat in amq_output:
            lat_avg = lat['latency_avg']
            lat_stddev = lat['lat_stddev']
            tps_incl = lat['tps_incl']
            tps_excl = lat['tps_excl']
            g_sheet.insert_row(
                [int(lat_avg),
                 int(lat_stddev),
                 int(tps_incl),
                 int(tps_excl)], 2
            )
