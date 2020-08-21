import logging

from ocs_ci.ocs.bucket_utils import (
    oc_create_aws_backingstore, oc_create_google_backingstore, oc_create_azure_backingstore,
    oc_create_s3comp_backingstore, oc_create_pv_backingstore, cli_create_aws_backingstore,
    cli_create_google_backingstore, cli_create_azure_backingstore, cli_create_s3comp_backingstore,
    cli_create_pv_backingstore
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from tests.helpers import create_unique_resource_name

log = logging.getLogger(__name__)


class BackingStore():
    """
    A class that represents BackingStore objects

    """
    def __init__(self, name, uls_name, secret_name=None):
        self.name = name
        self.uls_name = uls_name
        self.secret_name = secret_name

    def delete(self):
        log.info(f'Cleaning up backingstore {self.name}')

        OCP(
            namespace=config.ENV_DATA['cluster_namespace']
        ).exec_oc_cmd(
            command=f'delete backingstore {self.name}',
            out_yaml_format=False
        )

        log.info(
            f"Verifying whether backingstore {self.name} exists after deletion"
        )
        # Todo: implement deletion assertion


def backingstore_factory(request, cld_mgr, cloud_uls_factory):
    """
    Create a Backing Store factory.
    Calling this fixture creates a new Backing Store(s).

    Args:
        request (object): Pytest built-in fixture
        cld_mgr (CloudManager): Cloud Manager object containing all
            connections to clouds
        cloud_uls_factory: Factory for underlying storage creation

    Returns:
        func: Factory method - each call to this function creates
            a backingstore

    """
    created_backingstores = []

    cmdMap = {
        'oc': {
            'aws': oc_create_aws_backingstore,
            'google': oc_create_google_backingstore,
            'azure': oc_create_azure_backingstore,
            'ibmcos': oc_create_s3comp_backingstore,
            'pv': oc_create_pv_backingstore
        },
        'cli': {
            'aws': cli_create_aws_backingstore,
            'google': cli_create_google_backingstore,
            'azure': cli_create_azure_backingstore,
            'ibmcos': cli_create_s3comp_backingstore,
            'pv': cli_create_pv_backingstore
        }
    }

    def _create_backingstore(method, uls_dict):
        """
        Tracks creation and cleanup of all the backing stores that were created in the scope

        Args:
            method (str): String for selecting method of backing store creation (CLI/OC)
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            Cloud backing stores form - 'CloudName': [(amount, region), (amount, region)]
            i.e. - 'aws': [(3, us-west-1),(2, eu-west-2)]
            PV form - 'pv': [(amount, size_in_gb, storageclass), ...]
            i.e. - 'pv': [(3, 32, ocs-storagecluster-ceph-rbd),(2, 100, ocs-storagecluster-ceph-rbd)]

        Returns:
            list: A list of backingstore names.

        """
        if method.lower() not in cmdMap:
            raise RuntimeError(
                f'Invalid method type received: {method}. '
                f'available types: {", ".join(cmdMap.keys())}'
            )
        for cloud, uls_lst in uls_dict.items():
            for uls_tup in uls_lst:
                # Todo: Replace multiple .append calls, create names in advance, according to amountoc
                if cloud.lower() not in cmdMap[method.lower()]:
                    raise RuntimeError(
                        f'Invalid cloud type received: {cloud}. '
                        f'available types: {", ".join(cmdMap[method.lower()].keys())}'
                    )
                if cloud == 'pv':
                    vol_num, size, storage_class = uls_tup
                    backingstore_name = create_unique_resource_name(
                        resource_description='backingstore', resource_type=cloud.lower()
                    )
                    # removing characters from name (pod name length bellow 64 characters issue)
                    backingstore_name = backingstore_name[:-16]
                    created_backingstores.append(backingstore_name)
                    cmdMap[method.lower()][cloud.lower()](
                        backingstore_name, vol_num, size, storage_class
                    )
                else:
                    region = uls_tup[1]
                    # Todo: Verify that the given cloud has an initialized client
                    uls_dict = cloud_uls_factory({cloud: [uls_tup]})
                    for uls_name in uls_dict[cloud.lower()]:
                        backingstore_name = create_unique_resource_name(
                            resource_description='backingstore', resource_type=cloud.lower()
                        )
                        # removing characters from name (pod name length bellow 64 characters issue)
                        backingstore_name = backingstore_name[:-16]
                        created_backingstores.append(
                            BackingStore(
                                name=backingstore_name,
                                uls_name=uls_name
                            )
                        )
                        cmdMap[method.lower()][cloud.lower()](
                            cld_mgr, backingstore_name, uls_name, region
                        )
                        # Todo: Raise an exception in case the BS wasn't created

        return created_backingstores

    def backingstore_cleanup():
        for backingstore in created_backingstores:
            backingstore.delete()

    request.addfinalizer(backingstore_cleanup)

    return _create_backingstore
