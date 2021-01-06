import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, tier2, ignore_leftovers
from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
)
from ocs_ci.ocs import flowtest


log = logging.getLogger(__name__)


@ignore_leftovers
@tier2
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
        couchbase_factory_fixture,
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
        self.amq, self.threads = amq_factory_fixture(sc_name=sc_obj.name)
        self.cb = couchbase_factory_fixture(sc_name=sc_obj.name, run_in_bg=True)
        self.pgsql = pgsql_factory_fixture(
            replicas=3, clients=3, transactions=600, sc_name=sc_obj.name
        )

        bg_handler = flowtest.BackgroundOps()
        bg_ops = [self.cb.result]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
        cluster_used_space = get_percent_used_capacity()
        log.info(
            f" Cluster used percentage space with replica size {replica}, "
            f"compression mode {compression}={cluster_used_space}"
        )
