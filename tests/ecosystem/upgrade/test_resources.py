import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    post_upgrade,
    skipif_aws_creds_are_missing,
)
from ocs_ci.ocs.resources.pod import wait_for_storage_pods

log = logging.getLogger(__name__)


@skipif_aws_creds_are_missing
@post_upgrade
@pytest.mark.polarion_id("OCS-2220")
def test_storage_pods_running(multiregion_mirror_setup_session):
    """
    Test that all pods from openshift-storage namespace have status Running
    or Completed after upgrade is completed.

    multiregion_mirror_setup_session fixture is present during this test to
    make sure that NooBaa backing stores from other upgrade tests were
    not yet deleted. This is done to test scenario from BZ 1823775.

    """
    wait_for_storage_pods(timeout=10), 'Some pods were not in expected state'
