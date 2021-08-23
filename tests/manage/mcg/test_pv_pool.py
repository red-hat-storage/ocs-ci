import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    tier3,
    ignore_data_rebalance,
)
from ocs_ci.ocs.bucket_utils import (
    wait_for_pv_backingstore,
    check_pv_backingstore_status,
)
from ocs_ci.ocs.constants import MIN_PV_BACKINGSTORE_SIZE_IN_GB
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)
LOCAL_DIR_PATH = "/awsfiles"


@ignore_data_rebalance
class TestPvPool:
    """
    Test pv pool related operations
    """

    @pytest.mark.skip(
        reason="Skipped because of https://github.com/red-hat-storage/ocs-ci/issues/3323"
    )
    @pytest.mark.polarion_id("OCS-2332")
    @tier3
    def test_write_to_full_bucket(
        self, mcg_obj_session, awscli_pod_session, bucket_class_factory, bucket_factory
    ):
        """
        Test to check the full capacity functionality of a pv based backing store.
        """
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {
                "pv": [
                    (
                        1,
                        MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                        "ocs-storagecluster-ceph-rbd",
                    )
                ]
            },
        }
        bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]

        for i in range(1, 18):
            # add some data to the first pod
            awscli_pod_session.exec_cmd_on_pod(
                "dd if=/dev/urandom of=/tmp/testfile bs=1M count=1000"
            )
            try:
                awscli_pod_session.exec_s3_cmd_on_pod(
                    f"cp /tmp/testfile s3://{bucket.name}/testfile{i}", mcg_obj_session
                )
            except CommandFailed:
                assert check_pv_backingstore_status(
                    bucket.bucketclass.backingstores[0],
                    config.ENV_DATA["cluster_namespace"],
                    "`NO_CAPACITY`",
                ), "Failed to fill the bucket"
            awscli_pod_session.exec_cmd_on_pod("rm -f /tmp/testfile")
        try:
            awscli_pod_session.exec_s3_cmd_on_pod(
                f"cp s3://{bucket.name}/testfile1 /tmp/testfile", mcg_obj_session
            )
        except CommandFailed as e:
            raise e
        try:
            awscli_pod_session.exec_s3_cmd_on_pod(
                f"rm s3://{bucket.name}/testfile1", mcg_obj_session
            )
        except CommandFailed as e:
            raise e
        try:
            awscli_pod_session.exec_s3_cmd_on_pod(
                f"cp /tmp/testfile s3://{bucket.name}/testfile1", mcg_obj_session
            )
        except CommandFailed:
            assert not check_pv_backingstore_status(
                bucket.bucketclass.backingstores[0],
                config.ENV_DATA["cluster_namespace"],
                "`NO_CAPACITY`",
            ), "Failed to re-upload the removed file file"

    @pytest.mark.polarion_id("OCS-2333")
    @tier2
    def test_pv_scale_out(self, backingstore_factory):
        """
        Test to check the scale out functionality of pv pool backing store.
        """
        pv_backingstore = backingstore_factory(
            "OC",
            {
                "pv": [
                    (1, MIN_PV_BACKINGSTORE_SIZE_IN_GB, "ocs-storagecluster-ceph-rbd")
                ]
            },
        )[0]

        logger.info(f"Scaling out PV Pool {pv_backingstore.name}")
        pv_backingstore.vol_num += 1
        edit_pv_backingstore = OCP(
            kind="BackingStore", namespace=config.ENV_DATA["cluster_namespace"]
        )
        params = f'{{"spec":{{"pvPool":{{"numVolumes":{pv_backingstore.vol_num}}}}}}}'
        edit_pv_backingstore.patch(
            resource_name=pv_backingstore.name, params=params, format_type="merge"
        )
        logger.info("Waiting for backingstore to return to OPTIMAL state")
        wait_for_pv_backingstore(
            pv_backingstore.name, config.ENV_DATA["cluster_namespace"]
        )

        logger.info("Check if PV Pool scale out was successful")
        backingstore_dict = edit_pv_backingstore.get(pv_backingstore.name)
        assert (
            backingstore_dict["spec"]["pvPool"]["numVolumes"] == pv_backingstore.vol_num
        ), "Scale out PV Pool failed. "
        logger.info("Scale out was successful")
