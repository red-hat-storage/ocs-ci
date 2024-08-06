import pandas as pd
import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_hci_client,
    skipif_mcg_only,
    skipif_disconnected_cluster,
    polarion_id,
    black_squad,
    tier2,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import is_hci_provider_cluster
from ocs_ci.ocs.ui.page_objects.alerting import Runbook
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


logger = logging.getLogger(__name__)


internal_mode_OCP_ODF_alerts = {
    constants.ALERT_CLUSTERERRORSTATE: "c27c66813b585dd5d091f0305a7e0c34",
    constants.ALERT_CLUSTERWARNINGSTATE: "109d0e4a8a2ca356f00093552ca26f67",
    constants.ALERT_CEPH_OSD_VERSION_MISMATCH: "7b43a3eeb6ffc1fd3d4e5f0cc1614f32",
    constants.ALERT_PERSISTENT_VOLUME_USAGE_CRITICAL: "375ffa877b36a083f84aae62b254a163",
    constants.ALERT_CLUSTERCRITICALLYFULL: "2ce640cb5ebfe32bb92cfb3fb0fdad3d",
    constants.ALERT_CLUSTERNEARFULL: "4cc6fa4d12cb1f918c4b26ef09ea86c4",
    constants.ALERT_CEPH_CLUSTER_READ_ONLY: "fe05cce0d04692e7c18024399e8eb665",
    constants.ALERT_CEPH_MON_VERSION_MISMATCH: "60ea71a6bf58e140da9eacf06c10f821",
    constants.ALERT_CEPH_POOL_QUOTA_BYTES_CRITICALLY_EXHAUSTED: "c891af3cfc8bc4cbc26f1300bf6920d3",
    constants.ALERT_CEPH_POOL_QUOTA_BYTES_NEAR_EXHAUSTION: "3695319b318fd9167af7ccaaa66cb802",
    constants.ALERT_MGRISABSENT: "6f60fe15ad5ac9b1dde9e933fd1c59aa",
    constants.ALERT_MGRISMISSINGREPLICAS: "b3757ad3e40a6865772759a3652b1f54",
    constants.ALERT_CEPH_MDS_MISSING_REPLICAS: "003d21ab882afa98663887ed5695d12f",
    constants.ALERT_MONQUORUMATRISK: "2e77dc226b8026833854cd9ad66653e3",
    constants.ALERT_MONQUORUMLOST: "b1cc0824d91dd3408c8277923ff60501",
    constants.ALERT_CEPH_MON_HIGH_NUMBER_OF_LEADER_CHANGES: "445b3522779a9bfd96467758bf610149",
    constants.ALERT_NODEDOWN: "294fdea46b4af6e60621d728b034b4c0",
    constants.ALERT_CEPH_OSD_CRITICALLY_FULL: "08ffb8cc96bd8e063d156725aee8009c",
    constants.ALERT_OBC_QUOTA_OBJECTS_ALERT: "af3ca514d544d19effed127ea939078f",
    constants.ALERT_OBC_QUOTA_BYTES_EXHAUSED_ALERT: "07cba786a43d821fb155c14d46b0bf41",
    constants.ALERT_CLUSTEROBJECTSTORESTATE: "8d3b68a43603c0cfe06d146a571e28c4",
    constants.ALERT_ODF_RBD_CLIENT_BLOCKED: "f0c5f376ca52bb4acb3f51d345269834",
    constants.ALERT_ODF_MIRROR_DAEMON_STATUS: "cd721b8b90942fe527dc24661a044100",
    constants.ALERT_ODF_MIRRORING_IMAGE_HEALTH: "2f63d9aea88be3e94f16bea912105ffd",
    constants.ALERT_CEPH_OSD_FLAPPING: "053f317b3b064e5a52b7b836b35abd53",
    constants.ALERT_CEPH_OSD_NEAR_FULL: "eae5c5a61107cf9655bff36f05ed379b",
    constants.ALERT_OSDDISKNOTRESPONDING: "ff8c4df521d4aa883ff522d50f26c869",
    constants.ALERT_OSDDISKUNAVAILABLE: "e66865572e56fce2f9fc44f3479fc89a",
    constants.ALERT_CEPHOSDSLOWOPS: "85c3a867cefd54685e8ee2da4026d7ae",
    constants.ALERT_DATARECOVERYTAKINGTOOLONG: "4ec366536bbd75310d9cce1ab11174a6",
    constants.ALERT_PGREPAIRTAKINGTOOLONG: "a1d659aaf4823c1cd3836cd29bd966dc",
    constants.ALERT_PERSISTENT_VOLUME_USAGE_NEAR_FULL: "441dabf582bf8c22e456d1e83677a6eb",
    constants.ALERT_ODF_PERSISTENT_VOLUME_MIRROR_STATUS: "3c0c7404d9421b0ebf8015b449f82d87",
    constants.ALERT_OBC_QUOTA_BYTES_ALERT: "0086e448dea6c1ac13fcfcb0185ed061",
}
provider_mode_alerts = {
    constants.ALERT_STORAGECLIENTHEARTBEATMISSED: "f649b222a76e8b5e72ad6427fa610bdc",
    constants.ALERT_STORAGECLIENTINCOMPATIBLEOPERATORVERSION: "6365d0af0198bd4c6a40f68b7382f942",
}


@pytest.fixture(scope="class")
def alerts_expected():
    """
    Get alerts hash values based on the cluster mode

    """
    if is_hci_provider_cluster():
        return {**internal_mode_OCP_ODF_alerts, **provider_mode_alerts}
    else:
        return internal_mode_OCP_ODF_alerts


@tier2
@black_squad
@skipif_mcg_only
@skipif_hci_client
@polarion_id("OCS-5509")
@skipif_disconnected_cluster
def test_runbooks(setup_ui, alerts_expected):
    """
    Test runbooks for alerts. Texts are validated manually and hash values are created based on the texts.
    The test will check all runbooks from the list, even if one of them fails.

    The test do the steps:
    1. Navigate to the alerting page
    2. Navigate to the details page of each alert
    3. Get the runbook text
    4. Navigate back to the alerting page
    5. Check if the runbook text is valid (alert name and mandatory headers are present)
    6. Check if the runbook text is as expected through the hash value entered as expected result for the test
    7. If the runbook text is not as expected, log the alert name and the expected and actual runbook hash values
    8. Assert that all runbook texts are valid and as expected

    """
    test_res = dict()
    mandatory_headers = ["Meaning", "Impact", "Diagnosis", "Mitigation"]

    alerting_rules_page = PageNavigator().navigate_alerting_page().nav_alerting_rules()
    for alert_name, runbook_hash in alerts_expected.items():
        alert_details_page = alerting_rules_page.navigate_alerting_rule_details(
            alert_name
        )
        runbook_actual = alert_details_page.get_raw_runbook()

        alert_details_page.navigate_backward()

        text_valid = runbook_actual.check_text_content(mandatory_headers, alert_name)
        text_as_expected = runbook_actual == Runbook(runbook_hash=runbook_hash)

        if not text_as_expected:
            logger.error(
                f"Runbook hash mismatch for alert {alert_name}. Expected: {runbook_hash}, Actual: {runbook_actual}"
            )

        test_res[alert_name] = text_as_expected and text_valid

    assert_msg = "Failed to match runbook hash for alerts: \n"
    if test_res:
        test_res_df = pd.DataFrame.from_dict(
            test_res, orient="index", columns=["Runbook Hash Match"]
        )
        alerts_failed_check = test_res_df[
            ~test_res_df["Runbook Hash Match"]
        ].to_markdown(headers="keys", index=True, tablefmt="grid")
        assert_msg = f"{assert_msg}{alerts_failed_check}"

    assert all(test_res.values()), assert_msg
