import logging
import pytest
import time
import os

from ocs_ci.ocs import constants, scale_noobaa_lib
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
    skipif_managed_service,
    skipif_bm,
    skipif_external_mode,
    vsphere_platform_required,
    orange_squad,
    rgw,
)
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.utility import utils, templating
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)

# Noobaa storage class
sc_name = constants.DEFAULT_STORAGECLASS_RGW
# Number of scaled obc count
scale_obc_count = 100
# Number of obc creating by batch
num_obc_batch = 50
# Number of objects
num_objs = 150000
# Scale data file
log_path = ocsci_log_path()
obc_scaled_data_file = f"{log_path}/obc_scale_rgw_data_file.yaml"


@orange_squad
@rgw
@pre_upgrade
@vsphere_platform_required
@skipif_external_mode
@skipif_bm
@skipif_managed_service
@pytest.mark.polarion_id("OCS-3987")
def test_scale_obc_rgw_pre_upgrade(tmp_path, mcg_job_factory, timeout=60):
    """
    Create buckets with and without objects using RGW storage class before upgrade.
    Save scaled obc data in a file for post upgrade validation.
    Validate objects in buckets in post upgrade.

    """
    # Running hsbench to create buckets with objects before upgrade.
    #  PUT, GET and LIST objects of a bucket.
    namespace = scale_noobaa_lib.create_namespace()
    scale_noobaa_lib.hsbench_setup()
    scale_noobaa_lib.hsbench_io(
        namespace=namespace,
        num_obj=num_objs,
        num_bucket=10,
        object_size="100K",
        run_mode="cxipgl",
        result="result.csv",
        validate=True,
    )
    # Validate objects in bucket(s) after created
    scale_noobaa_lib.validate_bucket(
        num_objs,
        upgrade="pre_upgrade",
        result="result.csv",
        put=True,
        get=True,
        list_obj=True,
    )

    # Create OBC without I/O and ensure OBC in Bound state before upgrade
    obc_scaled_list = []
    log.info(f"Start creating  {scale_obc_count} " f"OBC in a batch of {num_obc_batch}")
    for i in range(int(scale_obc_count / num_obc_batch)):
        obc_dict_list = scale_noobaa_lib.construct_obc_creation_yaml_bulk_for_kube_job(
            no_of_obc=num_obc_batch,
            sc_name=sc_name,
            namespace=namespace,
        )
        # Create job profile
        job_file = ObjectConfFile(
            name="job_profile",
            obj_dict_list=obc_dict_list,
            project=namespace,
            tmp_path=tmp_path,
        )
        # Create kube_job
        job_file.create(namespace=namespace)
        time.sleep(timeout * 5)

        # Check all the OBCs reached Bound state
        obc_bound_list = scale_noobaa_lib.check_all_obc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file,
            namespace=namespace,
            no_of_obc=num_obc_batch,
        )
        obc_scaled_list.extend(obc_bound_list)

    log.info(
        f"Number of OBCs in scaled list: {len(obc_scaled_list)}",
    )

    # Write namespace, OBC data in a OBC_SCALE_DATA_FILE which
    # will be used during post_upgrade validation tests
    with open(obc_scaled_data_file, "a+") as w_obj:
        w_obj.write(str("# Scale Data File\n"))
        w_obj.write(str(f"NAMESPACE: {namespace}\n"))
        w_obj.write(str(f"OBC_SCALE_LIST: {obc_scaled_list}\n"))


@orange_squad
@rgw
@post_upgrade
@vsphere_platform_required
@skipif_external_mode
@skipif_bm
@skipif_managed_service
@pytest.mark.polarion_id("OCS-3988")
def test_scale_obc_rgw_post_upgrade():
    """
    Validate OBC scaled for post upgrade
    """

    # Get info from SCALE_DATA_FILE for validation
    if os.path.exists(obc_scaled_data_file):
        file_data = templating.load_yaml(obc_scaled_data_file)
        namespace = file_data.get("NAMESPACE")
        obc_scale_list = file_data.get("OBC_SCALE_LIST")
    else:
        raise FileNotFoundError

    # Check obc status in current namespace
    obc_bound_list, obc_not_bound_list = scale_noobaa_lib.check_all_obcs_status(
        namespace
    )

    # Check status of OBC scaled in pre-upgrade
    if not len(obc_bound_list) == len(obc_scale_list):
        raise UnexpectedBehaviour(
            f" OBC bound list count mismatch {len(obc_not_bound_list)} OBCs not in Bound state "
            f" OBCs not in Bound state {obc_not_bound_list}"
        )
    else:
        log.info(f" Expected all {len(obc_bound_list)} OBCs are in Bound state")

    # Validate existing objects in bucket(s)
    scale_noobaa_lib.validate_bucket(
        num_objs, upgrade="post_upgrade", result="result.csv", get=True, list_obj=True
    )

    # Delete objects in existing bucket
    scale_noobaa_lib.delete_object(bucket_name="bp01000000000000")

    # Delete existing bucket
    scale_noobaa_lib.delete_bucket(bucket_name="bp01000000000001")

    # Create new bucket with objects
    scale_noobaa_lib.hsbench_io(
        namespace=namespace,
        num_obj=num_objs,
        num_bucket=1,
        object_size="100K",
        run_mode="cxipgl",
        bucket_prefix="new",
        result="new_result.csv",
        validate=True,
    )
    # Verify new bucket with objects after create:
    scale_noobaa_lib.validate_bucket(
        num_objs,
        upgrade="post_upgrade",
        result="new_result.csv",
        put=True,
        get=True,
        list_obj=True,
    )

    # Delete object in new bucket
    scale_noobaa_lib.delete_object(bucket_name="new000000000000")

    # Check ceph health status
    utils.ceph_health_check()

    # Clean up all scaled obcs
    scale_noobaa_lib.cleanup(namespace=namespace, obc_list=obc_scale_list)

    # Cleanup hsbench resources
    scale_noobaa_lib.hsbench_cleanup()

    # Delete namespace
    scale_noobaa_lib.delete_namespace(namespace=namespace)
