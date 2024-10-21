import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    runs_on_provider,
    black_squad,
    hci_provider_required,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


logger = logging.getLogger(__name__)


@tier1
@black_squad
@skipif_ocs_version("<4.17")
@skipif_ocp_version("<4.17")
@runs_on_provider
@hci_provider_required
class TestOnboardingTokenGenerationWithQuota(ManageTest):
    """
    Test onboarding token generation when quota is specified
    """

    storage_clients = PageNavigator().nav_to_storageclients_page()
    token = storage_clients.generate_client_onboarding_ticket(
        quota_value=2, quota_tib=True
    )
    logger.info(f"Token generated. It begins with {token[:20]}")
    assert len(token) > 20, "Token is too short"
