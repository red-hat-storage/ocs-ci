import logging
import pytest
import time
import os

from ocs_ci.ocs import constants, scale_noobaa_lib
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
    skipif_bm,
    skipif_managed_service,
    skipif_external_mode,
    orange_squad,
    red_squad,
)
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.utility import utils, templating
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)

# Noobaa storage class
sc_name = constants.NOOBAA_SC
# Number of scaled obc count
scale_obc_count = 500
# Number of obc creating by batch
num_obc_batch = 50
# Scale data file
log_path = ocsci_log_path()
obc_scaled_data_file = f"{log_path}/obc_scale_data_file.yaml"


@orange_squad
@pre_upgrade
@skipif_external_mode
@skipif_bm
@skipif_managed_service
@red_squad
@pytest.mark.polarion_id("OCS-3987")
def test_scale_obc_pre_upgrade(tmp_path, timeout=60):
    """
    Create scaled MCG OBC using Noobaa storage class before upgrade
    Save scaled obc data in a file for post upgrade validation
    """
    namespace = scale_noobaa_lib.create_namespace()
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


@post_upgrade
@skipif_external_mode
@skipif_bm
@skipif_managed_service
@pytest.mark.polarion_id("OCS-3988")
@orange_squad
def test_scale_obc_post_upgrade():
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

    # Check ceph health status
    utils.ceph_health_check()

    # Clean up all scaled obc
    scale_noobaa_lib.cleanup(namespace=namespace, obc_list=obc_scale_list)

    # Delete namespace
    scale_noobaa_lib.delete_namespace(namespace=namespace)
