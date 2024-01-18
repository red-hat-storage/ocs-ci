import logging
import pytest

from ocs_ci.framework.testlib import (
    tier1,
    config,
    bugzilla,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    mcg,
    skipif_managed_service,
)
from ocs_ci.ocs.bucket_utils import get_bucket_available_size
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, NoobaaConditionException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import skipif_upgraded_from as upgraded_from

logger = logging.getLogger(__name__)


@retry((NoobaaConditionException, UnexpectedBehaviour), tries=30, delay=15, backoff=1)
def compare_sizes(mcg_obj, ceph_obj, bucket_name):
    """
    Compares the sizes

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
    try:
        bucket_size_in_gb = float(format(bucket_size / constants.GB, ".3f"))
    except TypeError:
        raise NoobaaConditionException(
            "Noobaa backingstore has not yet synced the backend size. Retrying."
        )
    if not abs(ceph_size_in_gb - bucket_size_in_gb) <= 1.5:
        raise UnexpectedBehaviour(
            f"Available size in ceph cluster:{ceph_size_in_gb} and object bucket:{bucket_size_in_gb} are not "
            f"matching. Retrying..."
        )
    else:
        logger.info(
            f"Available size in ceph cluster:{ceph_size_in_gb} and object bucket:{bucket_size_in_gb} matches"
        )


@mcg
@red_squad
@skipif_managed_service
@skipif_ocs_version("<4.7")
@pytest.mark.polarion_id("OCS-2476")
@bugzilla("1880747")
@bugzilla("1880748")
@tier1
def test_object_bucket_size(mcg_obj, bucket_factory, rgw_deployments):
    """
    Test to verify object bucket(backed by RGW) available size

    """
    # Checks if the cluster is upgraded from OCS 4.6 #bz 1952848
    if upgraded_from(["4.6"]):
        bs_obj = ocp.OCP(
            kind="backingstore", namespace=config.ENV_DATA["cluster_namespace"]
        )
        bs_obj.patch(
            resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
            params='{"metadata":{"annotations":{"rgw":""}}}',
            format_type="merge",
        )
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
