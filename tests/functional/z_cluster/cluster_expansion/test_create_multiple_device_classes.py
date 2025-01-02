import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier1,
    brown_squad,
)
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.helpers.multiple_device_classes import create_new_lvs_for_new_deviceclass

log = logging.getLogger(__name__)


@brown_squad
@tier1
@ignore_leftovers
class TestMultipleDeviceClasses(ManageTest):
    def test_add_new_ssd_device_class(self):
        osd_node_names = get_osd_running_nodes()
        log.info(f"osd node names = {osd_node_names}")
        create_new_lvs_for_new_deviceclass(osd_node_names)
