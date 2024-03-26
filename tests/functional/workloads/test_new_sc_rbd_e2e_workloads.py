import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    tier2,
    ignore_leftovers,
    skipif_external_mode,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
)
from ocs_ci.ocs import flowtest


log = logging.getLogger(__name__)


@magenta_squad
@skipif_external_mode
@skipif_hci_provider_and_client
@ignore_leftovers
@tier2
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestCreateNewScWithNeWRbDPoolE2EWorkloads(E2ETest):
    @pytest.mark.parametrize(
        argnames=["replica", "compression"],
        argvalues=[
            pytest.param(*[3, "aggressive"], marks=pytest.mark.polarion_id("OCS-2347")),
            pytest.param(*[2, "aggressive"], marks=pytest.mark.polarion_id("OCS-2345")),
            pytest.param(*[3, "none"], marks=pytest.mark.polarion_id("OCS-2346")),
            pytest.param(*[2, "none"], marks=pytest.mark.polarion_id("OCS-2344")),
        ],
    )
    def test_new_sc_new_rbd_pool_e2e_wl(
        self,
        storageclass_factory,
        amq_factory_fixture,
        pgsql_factory_fixture,
        replica,
        compression,
    ):
        """
        Testing workloads on new storage class with new cephblockpool
        """
        interface_type = constants.CEPHBLOCKPOOL
        sc_obj = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
        )
        bg_handler = flowtest.BackgroundOps()
        executor_run_bg_ios_ops = ThreadPoolExecutor(max_workers=5)
        self.amq, self.threads = amq_factory_fixture(sc_name=sc_obj.name)

        pgsql_workload = executor_run_bg_ios_ops.submit(
            bg_handler.handler,
            pgsql_factory_fixture,
            replicas=1,
            clients=1,
            transactions=100,
            timeout=100,
            sc_name=sc_obj.name,
            iterations=1,
        )
        bg_handler = flowtest.BackgroundOps()
        bg_ops = [pgsql_workload]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
        # AMQ Validate the results
        log.info("Validate message run completely")
        for thread in self.threads:
            thread.result(timeout=1800)

        cluster_used_space = get_percent_used_capacity()
        log.info(
            f" Cluster used percentage space with replica size {replica}, "
            f"compression mode {compression}={cluster_used_space}"
        )
