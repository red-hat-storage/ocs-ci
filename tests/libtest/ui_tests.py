import logging

from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import human_to_bytes

logger = logging.getLogger(__name__)


@libtest
def test_raw_capacity(setup_ui_session):
    """
    Test to verify the used capacity of the cluster can be parsed and compared to the available capacity
    """
    block_and_file = (
        PageNavigator()
        .nav_odf_default_page()
        .nav_storage_systems_tab()
        .nav_storagecluster_storagesystem_details()
        .nav_block_and_file()
    )
    used, available = block_and_file.get_raw_capacity_card_values()
    used_bytes = human_to_bytes(used)
    logger.info(f"Used capacity: {used_bytes}")
    available_bytes = human_to_bytes(available)
    logger.info(f"Available capacity: {available_bytes}")
    assert (
        used_bytes < available_bytes
    ), "Used capacity is not less than available capacity"
