from ocs_ci.framework.testlib import MCGTest

# from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import skipif_ocs_version, skipif_aws_creds_are_missing


@skipif_ocs_version("<4.13")
@skipif_aws_creds_are_missing
class TestLogBasedBucketReplication(MCGTest):
    """
    Test log-based replication with deletions sync.
    """

    pass
