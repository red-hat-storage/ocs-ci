import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier2, tier3, bugzilla
from ocs_ci.ocs.bucket_utils import (
    wait_for_pv_backingstore,
    check_pv_backingstore_status,
)
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    Pod,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.constants import MIN_PV_BACKINGSTORE_SIZE_IN_GB
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.bucket_utils import (
    copy_random_individual_objects,
)

logger = logging.getLogger(__name__)
LOCAL_DIR_PATH = "/awsfiles"


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

        logger.info("Checking if backingstore went to SCALING state")
        sample = TimeoutSampler(
            timeout=60,
            sleep=5,
            func=check_pv_backingstore_status,
            backingstore_name=pv_backingstore.name,
            namespace=config.ENV_DATA["cluster_namespace"],
            desired_status="`SCALING`",
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Backing Store {pv_backingstore.name} never reached SCALING state"

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

    @pytest.mark.polarion_id("OCS-3932")
    @tier2
    @bugzilla("2064599")
    def test_pvpool_cpu_and_memory_modifications(
        self,
        awscli_pod_session,
        backingstore_factory,
        bucket_factory,
        test_directory_setup,
        mcg_obj_session,
    ):
        """
        Test to modify the CPU and Memory resource limits for BS and see if its reflecting
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
        bucket_name = bucket.name
        pv_backingstore = bucket.bucketclass.backingstores[0]
        pv_bs_name = pv_backingstore.name
        pv_pod_label = f"pool={pv_bs_name}"
        pv_pod_info = get_pods_having_label(
            label=pv_pod_label, namespace=config.ENV_DATA["cluster_namespace"]
        )[0]
        pv_pod_obj = Pod(**pv_pod_info)
        pv_pod_name = pv_pod_obj.name
        logger.info(f"Pod created for PV Backingstore {pv_bs_name}: {pv_pod_name}")
        new_cpu = "500m"
        new_mem = "500Mi"
        new_resource_patch = {
            "spec": {
                "pvPool": {
                    "resources": {
                        "limits": {
                            "cpu": f"{new_cpu}",
                            "memory": f"{new_mem}",
                        },
                        "requests": {
                            "cpu": f"{new_cpu}",
                            "memory": f"{new_mem}",
                        },
                    }
                }
            }
        }
        try:
            OCP(
                namespace=config.ENV_DATA["cluster_namespace"],
                kind="backingstore",
                resource_name=pv_bs_name,
            ).patch(params=json.dumps(new_resource_patch), format_type="merge")
        except CommandFailed as e:
            logger.error(f"[ERROR] Failed to patch: {e}")
        else:
            logger.info("Patched new resource limits")
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"], pod_names=[pv_pod_name]
        )
        pv_pod_ocp_obj = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="pod"
        ).get(resource_name=pv_pod_name)
        resource_dict = pv_pod_ocp_obj["spec"]["containers"][0]["resources"]
        assert (
            resource_dict["limits"]["cpu"] == new_cpu
            and resource_dict["limits"]["memory"] == new_mem
            and resource_dict["requests"]["cpu"] == new_cpu
            and resource_dict["requests"]["memory"] == new_mem
        ), "New resource modification in Backingstore is not reflected in PV Backingstore Pod!!"
        logger.info("Resource modification reflected in the PV Backingstore Pod!!")

        # push some data to the bucket
        file_dir = test_directory_setup.origin_dir
        copy_random_individual_objects(
            podobj=awscli_pod_session,
            file_dir=file_dir,
            target=f"s3://{bucket_name}",
            amount=1,
            s3_obj=OBC(bucket_name),
        )
