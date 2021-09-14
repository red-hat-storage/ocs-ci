import pytest

from ocs_ci.cleanup.aws.defaults import CLUSTER_PREFIXES_SPECIAL_RULES
from ocs_ci.utility.deployment import get_cluster_prefix


@pytest.mark.parametrize(
    "cluster_name,expected",
    [
        ("mycluster", "mycluster"),
        ("lr1-mycluster", "mycluster"),
        ("mycluster-t1", "mycluster"),
        ("lr1-mycluster-t1", "mycluster"),
        ("j-123mycluster-t1", "mycluster"),
    ],
)
def test_get_cluster_prefix(cluster_name, expected):
    assert get_cluster_prefix(cluster_name, CLUSTER_PREFIXES_SPECIAL_RULES) == expected
