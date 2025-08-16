import json
import logging
import statistics

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    skipif_mcg_only,
    tier2,
    tier3,
    red_squad,
    runs_on_provider,
    mcg,
    fips_required,
    ignore_leftovers,
)
from ocs_ci.ocs.bucket_utils import (
    wait_for_pv_backingstore,
    check_pv_backingstore_status,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    Pod,
    wait_for_pods_to_be_running,
    get_pod_node,
    get_pod_logs,
    get_noobaa_core_pod,
)
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.constants import MIN_PV_BACKINGSTORE_SIZE_IN_GB, CEPHBLOCKPOOL_SC
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.bucket_utils import (
    copy_random_individual_objects,
)
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)
LOCAL_DIR_PATH = "/awsfiles"


@mcg
@red_squad
@runs_on_provider
@skipif_mcg_only
class TestPvPool:
    """
    Test pv pool related operations
    """

    @pytest.mark.polarion_id("OCS-2332")
    @tier3
    def test_write_to_full_bucket(
        self, mcg_obj_session, awscli_pod_session, bucket_class_factory, bucket_factory
    ):
        """
        Test to check the full capacity functionality of a pv based backing store.

        1. Create a bucket with pv based backing store
        2. Fill the bucket until writing fails due to no capacity
        3. Free up some space in the bucket
        4. Confirm that uploading is possible again now that there is space
        """
        # 1. Create a bucket with pv based backing store
        interface = "CLI"
        bucketclass_dict = {
            "interface": interface,
            "backingstore_dict": {
                "pv": [
                    (
                        1,
                        MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                        "ocs-storagecluster-ceph-rbd",
                        "100m",
                        "500Mi",
                        "100m",
                        "500Mi",
                    )
                ]
            },
        }
        bucket = bucket_factory(1, interface, bucketclass=bucketclass_dict)[0]

        # 2. Fill the bucket until writing fails due to no capacity
        uploaded_objs = []
        max_data_in_mb = MIN_PV_BACKINGSTORE_SIZE_IN_GB * 1024
        file_size_in_mb = 500

        for i in range(max_data_in_mb // file_size_in_mb):
            # Generate test file on the pod.
            awscli_pod_session.exec_cmd_on_pod(
                f"dd if=/dev/urandom of=/tmp/testfile bs=1M count={file_size_in_mb}"
            )
            try:
                # Try copying the file to S3.
                obj_name = f"testfile_{i}"
                awscli_pod_session.exec_s3_cmd_on_pod(
                    f"cp /tmp/testfile s3://{bucket.name}/{obj_name}",
                    mcg_obj_session,
                )
                uploaded_objs.append(obj_name)
            except CommandFailed:
                # Confirm that the failure was due to capacity being reached.
                assert check_pv_backingstore_status(
                    bucket.bucketclass.backingstores[0].name,
                    config.ENV_DATA["cluster_namespace"],
                    "`NO_CAPACITY`",
                ), "Failed to fill the bucket"
                break

        # 3. Free up some space in the bucket
        for obj in uploaded_objs[:3]:
            awscli_pod_session.exec_s3_cmd_on_pod(
                f"rm s3://{bucket.name}/{obj}", mcg_obj_session
            )

        # 4. Confirm that uploading is possible again now that there is space
        retry((CommandFailed,), tries=10, delay=30, backoff=1)(
            awscli_pod_session.exec_s3_cmd_on_pod
        )(
            f"cp /tmp/testfile s3://{bucket.name}/{uploaded_objs[0]}",
            mcg_obj_session,
        )

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

    @polarion_id("OCS-4929")
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {
                        "pv": [
                            (
                                3,
                                MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                                "ocs-storagecluster-ceph-rbd",
                            )
                        ]
                    },
                },
                marks=[
                    tier2,
                    pytest.mark.polarion_id("OCS-3932"),
                    pytest.mark.skipif_ocs_version("<4.11"),
                ],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {
                        "pv": [
                            (
                                3,
                                MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                                "ocs-storagecluster-ceph-rbd",
                                "300m",
                                "500Mi",
                                "400m",
                                "600Mi",
                            )
                        ]
                    },
                },
                marks=[
                    tier2,
                    pytest.mark.polarion_id("OCS-4643"),
                    pytest.mark.skipif_ocs_version("<4.12"),
                ],
            ),
        ],
    )
    def test_pvpool_resource_modifications(
        self,
        awscli_pod_session,
        backingstore_factory,
        bucket_factory,
        test_directory_setup,
        mcg_obj_session,
        bucketclass_dict,
    ):
        """
        Objective of the test are:
            1) See if the CLI options to add resource parameters works while creating
            Pv based backingstore.
            2) Modifying the backingstores resource, reflects in the pv based backingstore
            pods.

        """
        bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]
        bucket_name = bucket.name
        pv_backingstore = bucket.bucketclass.backingstores[0]
        pv_bs_name = pv_backingstore.name
        pv_pod_label = f"pool={pv_bs_name}"
        pv_pod_obj = list()
        for pod in get_pods_having_label(
            label=pv_pod_label, namespace=config.ENV_DATA["cluster_namespace"]
        ):
            pv_pod_obj.append(Pod(**pod))

        # verify there is no dummy access_key, secret_key messages are seen in the pv pool pod
        for pod in pv_pod_obj:
            pod_logs = get_pod_logs(pod_name=pod.name)
            assert ("access_key:" not in pod_logs) and (
                "secret_key:" not in pod_logs
            ), f"access_key or secret_key are logged in the pv pool pod {pod.name} logs"

        req_cpu = "400m"
        req_mem = "600Mi"
        lim_cpu = "500m"
        lim_mem = "700Mi"
        new_resource_patch = {
            "spec": {
                "pvPool": {
                    "resources": {
                        "limits": {
                            "cpu": f"{lim_cpu}",
                            "memory": f"{lim_mem}",
                        },
                        "requests": {
                            "cpu": f"{req_cpu}",
                            "memory": f"{req_mem}",
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
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=[pod.name for pod in pv_pod_obj],
        )

        for pod in pv_pod_obj:
            resource_dict = OCP(
                namespace=config.ENV_DATA["cluster_namespace"], kind="pod"
            ).get(resource_name=pod.name)["spec"]["containers"][0]["resources"]
            assert (
                resource_dict["limits"]["cpu"] == lim_cpu
                and resource_dict["limits"]["memory"] == lim_mem
                and resource_dict["requests"]["cpu"] == req_cpu
                and resource_dict["requests"]["memory"] == req_mem
            ), f"New resource modification in Backingstore is not reflected in PV Backingstore Pod {pod.name}!!"
        logger.info(
            f"Resource modification reflected in the PV Backingstore Pods {[pod.name for pod in pv_pod_obj]}!!"
        )

        # push some data to the bucket
        file_dir = test_directory_setup.origin_dir
        copy_random_individual_objects(
            podobj=awscli_pod_session,
            file_dir=file_dir,
            target=f"s3://{bucket_name}",
            amount=1,
            s3_obj=OBC(bucket_name),
        )

    @tier2
    @polarion_id("OCS-4862")
    def test_ephemeral_for_pv_bs(self, backingstore_factory):
        """
        Test if ephemeral storage on pv backingstore pod node is getting consumed
        """

        # create pv pool backingstore
        pv_backingstore = backingstore_factory(
            "OC",
            {"pv": [(1, MIN_PV_BACKINGSTORE_SIZE_IN_GB, CEPHBLOCKPOOL_SC)]},
        )[0]

        pv_bs_pod = Pod(
            **(
                get_pods_having_label(
                    label=f"pool={pv_backingstore.name}",
                    namespace=config.ENV_DATA["cluster_namespace"],
                )[0]
            )
        )

        # create some dummy data under /noobaa_storage mount point in pv pool bs pod
        pv_bs_pod.exec_sh_cmd_on_pod(
            command="cd /noobaa_storage && dd if=/dev/urandom of=test_object bs=512 count=1"
        )
        logger.info("Generated test_object under /noobaa_storage")

        pod_node = get_pod_node(pv_bs_pod).name
        logger.info(f"{pv_bs_pod.name} is scheduled on {pod_node}!")

        # check if the dummy data is also present in pv backingstore pod node ephemeral storage
        logger.info("Checking if the test_object present in ephemeral storage")
        base_dir = "/var/lib/kubelet/plugins/kubernetes.io/csi/openshift-storage.rbd.csi.ceph.com/"
        search_output = OCP().exec_oc_debug_cmd(
            node=pod_node, cmd_list=[f"find {base_dir} -name test_object"]
        )
        assert (
            "test_object" in search_output
        ), "Dummy data was not found in the node ephemeral storage!"

    @fips_required
    @tier2
    @polarion_id("OCS-5422")
    def test_pvpool_bs_in_fips(self, backingstore_factory):
        """
        Create PV pool based backingstore and make sure the backingstore doesn't
        goto Rejected phase on noobaa-core pod restarts

        """
        # create pv-pool backingstore
        pv_backingstore = backingstore_factory(
            "OC",
            {"pv": [(1, MIN_PV_BACKINGSTORE_SIZE_IN_GB, CEPHBLOCKPOOL_SC)]},
        )[0]

        # restart noobaa-core pod
        get_noobaa_core_pod().delete()

        # wait for about 10 mins to check if
        # the backingstore has reached Rejected state
        pv_bs_obj = OCP(
            kind=constants.BACKINGSTORE,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=pv_backingstore.name,
        )
        assert pv_bs_obj.wait_for_resource(
            condition="Ready", column="PHASE", timeout=600, sleep=5
        ), "Pv pool backingstore reached rejected phase after noobaa core pod restart"
        logger.info(
            "Pv pool backingstore didnt goto Rejected phase after noobaa-core pod restarts"
        )

    @tier2
    @ignore_leftovers
    @polarion_id("OCS-6552")
    def test_pv_pool_with_nfs(
        self, setup_nfs, bucket_factory, awscli_pod, test_directory_setup, mcg_obj
    ):
        """
        Test pv-pool backingstore creation using the NFS storageclass which doesn't support
        xattr.
            dfbug: https://issues.redhat.com/browse/DFBUGS-1114

        """
        # Create bucket based of pv-pool backingstores
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {
                "pv": [
                    (
                        3,
                        MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                        constants.NFS_SC_NAME,
                    )
                ]
            },
        }
        bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]

        # Write some data to the bucket
        write_random_test_objects_to_bucket(
            awscli_pod,
            bucket.name,
            test_directory_setup.origin_dir,
            amount=5,
            mcg_obj=mcg_obj,
        )

    @tier2
    @pytest.mark.parametrize(
        argnames=["pv_in_bs", "block_size", "block_count", "file_count"],
        argvalues=[
            pytest.param(
                *[
                    1,
                    "5K",
                    1,
                    25000,  # dataset contains 25000 small files of 5K each, 1 pv per backingstore
                ],
                marks=pytest.mark.polarion_id("OCS-6852"),
            ),
            pytest.param(
                *[
                    1,
                    "5K",
                    1,
                    2500,  # dataset contains 2500 small files of 5K each, 1 pv per backingstore
                ],
                marks=pytest.mark.polarion_id("OCS-6853"),
            ),
            pytest.param(
                *[
                    1,
                    "1M",
                    10,
                    20,  # dataset contains 20 medium files of 10MB each, 1 pv per backingstore
                ],
                marks=pytest.mark.polarion_id("OCS-6854"),
            ),
            pytest.param(
                *[
                    5,
                    "1M",
                    10,
                    20,  # dataset contains 20 medium files of 10MB each, 5 pvs per backingstore
                ],
                marks=pytest.mark.polarion_id("OCS-6855"),
            ),
            pytest.param(
                *[
                    10,
                    "1M",
                    10,
                    20,  # dataset contains 20 medium files of 10MB each, 10 pvs per backingstore
                ],
                marks=pytest.mark.polarion_id("OCS-6856"),
            ),
            pytest.param(
                *[
                    1,
                    "10M",
                    20,
                    10,  # dataset contains 10 big files of 200MB each, 1 pv per backingstore
                ],
                marks=pytest.mark.polarion_id("OCS-6857"),
            ),
            pytest.param(
                *[
                    1,
                    "10M",
                    10,
                    255,  # 255 files of 100 MB each are 25.5 GB which is 75% of 17GB*2=34GB
                ],
                marks=pytest.mark.polarion_id("OCS-6858"),
            ),
        ],
        ids=[
            "OnePV_ManySmall_Files",
            "OnePV_Small_Files",
            "OnePV_MediumFiles",
            "FivePV_Medium_Files",
            "TenPV_Medium_Files",
            "OnePV_Big_Files",
            "OnePV_Medium_Files_High_Usage",
        ],
    )
    def test_pv_data_dist(
        self,
        pv_in_bs,
        block_size,
        block_count,
        file_count,
        bucket_factory,
        awscli_pod_session,
        mcg_obj_session,
    ):
        """
        The test checks even distribution of the data written on bucket with 2 pv-backed backingstores,several pvs each.
            1) Create bucket with 2 pv-based backingstores, several pvs on each
            2) Write data to the bucket
            3) Verify that the data is distributed evenly among all the pvs.

        Args:
            pv_in_bs (int): Number of pvs on each backing store
            block_size (str): Size of each file block
            block_count (int): Number of blocks in each file. Product of 'block_size' and this parameter gives the
                size of each file to be written
            file_count (int): Number of files to write


        """

        pv_backingstore_specs = {
            "vol_num": pv_in_bs,
            "size": MIN_PV_BACKINGSTORE_SIZE_IN_GB,
            "storagecluster": CEPHBLOCKPOOL_SC,
            "req_cpu": "800m",
            "req_mem": "800Mi",
            "lim_cpu": "1000m",
            "lim_mem": "4000Mi",
        }
        bs_specs_tuple = tuple(pv_backingstore_specs.values())
        bucketclass_dict = {
            "interface": "CLI",
            "backingstore_dict": {
                "pv": [
                    bs_specs_tuple,  # first backingstore
                    bs_specs_tuple,  # second backingstore
                ]
            },
        }
        bucket = bucket_factory(amount=1, bucketclass=bucketclass_dict)[0]
        logger.info(
            f"The bucket with name {bucket.name} was successfully created on {pv_in_bs * 2} pvs."
        )

        base_path = "/tmp/datasets"
        block_size_int, block_size_char = int(block_size[:-1]), block_size[-1]

        # Cleanup the session scoped pod directory
        awscli_pod_session.exec_cmd_on_pod(f"rm -rf {base_path}")

        total_size_too_big = (block_size_char == "M") and (
            block_count * file_count >= 2000
        )

        if total_size_too_big:
            # Make individual dd and s3 cp calls for bigger datasets
            for i in range(file_count):
                awscli_pod_session.exec_cmd_on_pod(
                    f"dd if=/dev/urandom of=/tmp/testfile bs={block_size} count={block_count} status=none"
                )
                awscli_pod_session.exec_s3_cmd_on_pod(
                    f"cp /tmp/testfile s3://{bucket.name}/testfile_{i}",
                    mcg_obj_session,
                )
        else:
            # Upload smaller files with grouped dd and s3 sync calls,
            # but limit the batch size to support many small files
            batch_size = 1000
            full_batches_num = file_count // batch_size
            remainder = file_count % batch_size
            total_batches_num = full_batches_num + (1 if remainder else 0)

            for batch_i in range(total_batches_num):
                dataset_dir = f"{base_path}/batch_{batch_i}"
                file_count_in_batch = (
                    batch_size
                    if batch_i < full_batches_num
                    else (remainder or batch_size)
                )

                awscli_pod_session.exec_cmd_on_pod(
                    f"sh -ec 'mkdir -p {dataset_dir}; "
                    f"dd if=/dev/zero bs={block_size} of={dataset_dir}/bigfile "
                    f"count={block_count * file_count_in_batch} status=none; "
                    f"split -a 4 -b {block_size_int * block_count}{block_size_char.lower()} "
                    f"{dataset_dir}/bigfile {dataset_dir}/testfile_; "
                    f"rm {dataset_dir}/bigfile;'"
                )
                awscli_pod_session.exec_s3_cmd_on_pod(
                    f"sync {dataset_dir} s3://{bucket.name}/{batch_i}/ --no-progress --only-show-errors",
                    mcg_obj_session,
                )

        pv_pods_usage_list = list()
        for backing_store in bucket.bucketclass.backingstores:
            for pod in get_pods_having_label(
                label=f"pool={backing_store.name}",
                namespace=config.ENV_DATA["cluster_namespace"],
            ):
                pod_obj = Pod(**pod)
                df_res = pod_obj.exec_cmd_on_pod(command="df -kh /noobaa_storage/")
                logger.info(df_res)
                usage_str = df_res.split()[9]
                usage = int(usage_str[:-1])
                pv_pods_usage_list.append(usage)

        std_dev_st = statistics.stdev(pv_pods_usage_list)
        mean_st = statistics.mean(pv_pods_usage_list)
        st_dev_percent = (std_dev_st / mean_st) * 100
        logger.info(f"Standard deviation in percents is = {st_dev_percent}%")

        st_dev_percent_limit = 20
        assert (
            st_dev_percent < st_dev_percent_limit
        ), "The data distribution is not even among the pvs"
