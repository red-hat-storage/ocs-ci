import logging

from ocs_ci.ocs.ui.infra_ui import InfraUI
from ocs_ci.framework.testlib import ui, ignore_leftovers, skipif_lso
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    skipif_bm,
    skipif_external_mode,
    skipif_bmpsi,
)

logger = logging.getLogger(__name__)


class TestAddCapacityUI(object):
    """
    est Add Capacity on via UI

    """

    @ui
    @skipif_lso
    @skipif_bm
    @skipif_bmpsi
    @ignore_leftovers
    @skipif_external_mode
    def test_add_capacity_internal(self, setup_ui):
        """
        Test Add Capacity on Internal cluster via UI

        """
        osd_pods_before_add_capacity = get_osd_pods()
        osd_count = len(osd_pods_before_add_capacity)

        infra_ui_obj = InfraUI(setup_ui)
        infra_ui_obj.add_capacity_ui()

        logging.info("Wait for osd pods to be in Running state")
        for osd_pods in TimeoutSampler(
            timeout=600,
            sleep=10,
            func=get_osd_pods,
        ):
            if len(osd_pods) == (osd_count + 3):
                break

        for osd_pod in osd_pods:
            wait_for_resource_state(
                resource=osd_pod, state=constants.STATUS_RUNNING, timeout=300
            )
