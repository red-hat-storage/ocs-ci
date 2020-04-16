import logging

import pytest

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.exceptions import (ResourceWrongStatusException)
from tests import helpers

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def test_fixture_amq(request, storageclass_factory):
    # Change the above created StorageClass to default
    log.info(
        f"Changing the default StorageClass to {constants.DEFAULT_STORAGECLASS_CEPHFS}"
    )
    helpers.change_default_storageclass(scname=constants.DEFAULT_STORAGECLASS_CEPHFS)

    # Confirm that the default StorageClass is changed
    tmp_default_sc = helpers.get_default_storage_class()
    assert len(
        tmp_default_sc
    ) == 1, "More than 1 default storage class exist"
    log.info(f"Current Default StorageClass is:{tmp_default_sc[0]}")
    assert tmp_default_sc[0] == constants.DEFAULT_STORAGECLASS_CEPHFS, (
        "Failed to change default StorageClass"
    )
    log.info(
        f"Successfully changed the default StorageClass to "
        f"{constants.DEFAULT_STORAGECLASS_CEPHFS}"
    )

    amq = AMQ()
    amq.namespace = "my-project"

    def teardown():
        amq.cleanup()

    request.addfinalizer(teardown)
    return amq


@workloads
class TestAMQBasics(E2ETest):
    @pytest.mark.polarion_id("OCS-346")
    def test_install_amq_cephfs(self, test_fixture_amq):
        """
        Testing basics: secret creation,
        storage class creation, pvc and pod with cephfs
        """

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
