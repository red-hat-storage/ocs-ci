from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.backingstore import clone_backingstore
from ocs_ci.framework.testlib import libtest


@libtest
def test_clone_backingstore(mcg_obj, backingstore_factory):
    platform_to_bucketclass_dicts = {
        constants.BACKINGSTORE_TYPE_AWS: {"aws": [(1, "eu-central-1")]},
        constants.BACKINGSTORE_TYPE_AZURE: {"azure": [(1, None)]},
        constants.BACKINGSTORE_TYPE_IBMCOS: {"ibmcos": [(1, None)]},
        constants.BACKINGSTORE_TYPE_PV_POOL: {
            "pv": [(1, 20, constants.DEFAULT_STORAGECLASS_RBD)]
        },
    }
    prototypes = []
    for type, bucketclass_dict in platform_to_bucketclass_dicts.items():
        backingstore_name = backingstore_factory("oc", bucketclass_dict)[0].name
        prototypes.append((backingstore_name, type))

    for prototype in prototypes:
        prototype_name, prototype_type = prototype
        clone_name = clone_backingstore(prototype_name, backingstore_factory, mcg_obj)

        clone_backingstore_data = OCP(
            kind="backingstore",
            resource_name=clone_name,
        ).data
        assert clone_backingstore_data["spec"]["type"] == prototype_type
        assert clone_backingstore_data["status"]["phase"] == "Ready"
