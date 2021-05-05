import logging
import pytest

from ocs_ci.framework.testlib import (
    tier1,
    config,
    bugzilla,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    skipif_openshift_dedicated,
    skipif_upgraded_from,
)
from ocs_ci.ocs.bucket_utils import get_bucket_available_size
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@retry(UnexpectedBehaviour, tries=30, delay=15, backoff=1)
def compare_sizes(mcg_obj, ceph_obj, bucket_name):
    """
    Adds bucket policy to a bucket

    Args:
        mcg_obj (obj): MCG object
        ceph_obj (obj): OCP object of Ceph cluster
        bucket_name (str): Name of the bucket

    Raises:
        UnexpectedBehaviour: In case sizes does not match

    """
    ceph_size = (
        ceph_obj.get().get("status").get("ceph").get("capacity").get("bytesAvailable")
    )
    ceph_size_in_gb = float(format(ceph_size / constants.GB, ".3f"))
    bucket_size = get_bucket_available_size(mcg_obj, bucket_name)
    bucket_size_in_gb = float(format(bucket_size / constants.GB, ".3f"))

    if not abs(ceph_size_in_gb - bucket_size_in_gb) <= 1.5:
        raise UnexpectedBehaviour(
            f"Available size in ceph cluster:{ceph_size_in_gb} and object bucket:{bucket_size_in_gb} are not "
            f"matching. Retrying..."
        )
    else:
        logger.info(
            f"Available size in ceph cluster:{ceph_size_in_gb} and object bucket:{bucket_size_in_gb} matches"
        )


@skipif_openshift_dedicated
@skipif_ocs_version("<4.7")
@pytest.mark.polarion_id("OCS-2476")
@bugzilla("1880747")
@bugzilla("1880748")
@skipif_upgraded_from(["4.6"])
@tier1
def test_object_bucket_size(mcg_obj, bucket_factory, rgw_deployments):
    """
    Test to verify object bucket(backed by RGW) available size

    """
    ceph_obj = OCP(
        namespace=config.ENV_DATA["cluster_namespace"],
        kind="CephCluster",
        resource_name=f"{constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE}-cephcluster"
        if config.DEPLOYMENT["external_mode"]
        else f"{constants.DEFAULT_CLUSTERNAME}-cephcluster",
    )
    bucket_name = bucket_factory(amount=1, interface="S3")[0].name
    assert not compare_sizes(
        mcg_obj, ceph_obj, bucket_name
    ), "Failed: Available size mismatch"
