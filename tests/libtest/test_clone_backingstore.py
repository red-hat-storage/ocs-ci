from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.backingstore import clone_bs_dict_from_backingstore
from ocs_ci.framework.testlib import libtest


@libtest
def test_clone_backingstore(mcg_obj, backingstore_factory):
    """
    Test the functionality of clone_bs_dict_from_backingstore

    """
    platform_to_bucketclass_dicts = {
        constants.BACKINGSTORE_TYPE_AWS: {"aws": [(1, "eu-central-1")]},
        constants.BACKINGSTORE_TYPE_AZURE: {"azure": [(1, None)]},
        constants.BACKINGSTORE_TYPE_IBMCOS: {"ibmcos": [(1, None)]},
        constants.BACKINGSTORE_TYPE_PV_POOL: {
            "pv": [(1, 35, constants.DEFAULT_STORAGECLASS_RBD)]
        },
    }
    for type, bucketclass_dict in platform_to_bucketclass_dicts.items():
        prototype_backingstore = backingstore_factory("oc", bucketclass_dict)[0].name
        prototype_bs_dict = clone_bs_dict_from_backingstore(prototype_backingstore)
        clone_bs_name = backingstore_factory("oc", prototype_bs_dict)[0].name

        # Check the health of the clone
        mcg_obj.check_backingstore_state(clone_bs_name, constants.BS_OPTIMAL)

        # Check if type is the same as the prototype's
        clone_backingstore_data = OCP(
            kind="backingstore",
            resource_name=clone_bs_name,
        ).data
        assert clone_backingstore_data["spec"]["type"] == type
