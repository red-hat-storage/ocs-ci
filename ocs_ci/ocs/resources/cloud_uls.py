import logging

from ocs_ci.helpers.helpers import create_unique_resource_name

log = logging.getLogger(__name__)


def cloud_uls_factory(request, cld_mgr):
    """
    Create an Underlying Storage factory.
    Calling this fixture creates a new underlying storage(s).

    Args:
        request (object): Pytest built-in fixture
        cld_mgr (CloudManager): Cloud Manager object containing all connections to clouds

    Returns:
        func: Factory method - each call to this function creates
            an Underlying Storage factory

    """
    all_created_uls = {
        "aws": set(),
        "gcp": set(),
        "azure": set(),
        "ibmcos": set(),
        "rgw": set(),
    }
    try:
        ulsMap = {
            "aws": cld_mgr.aws_client,
            "gcp": cld_mgr.gcp_client,
            "azure": cld_mgr.azure_client,
            "ibmcos": cld_mgr.ibmcos_client,
        }
    except AttributeError as e:
        raise Exception(
            "{} was not initialized, "
            "please verify the needed credentials "
            "were set in auth.yaml".format(str(e).split("'")[3])
        )
    try:
        ulsMap["rgw"] = cld_mgr.rgw_client
    except AttributeError:
        log.info("RGW is not available and was not initialized")

    def _create_uls(uls_dict):
        """
        Creates and deletes all underlying storage that were created as part of the test

        Args:
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            each tuple contain amount as first parameter and region as second parameter.
            Cloud backing stores form - 'CloudName': [(amount, region), (amount, region)]
            i.e. - 'aws': [(3, us-west-1),(2, eu-west-2)]


        Returns:
            dict: A dictionary of cloud names as keys and uls names sets as value.

        """
        current_call_created_uls = {
            "aws": set(),
            "gcp": set(),
            "azure": set(),
            "ibmcos": set(),
            "rgw": set(),
        }

        for cloud, params in uls_dict.items():
            if cloud.lower() not in ulsMap:
                raise RuntimeError(
                    f"Invalid interface type received: {cloud}. "
                    f'available types: {", ".join(ulsMap.keys())}'
                )
            log.info(f"Creating uls for cloud {cloud.lower()}")
            for amount, region in params:
                for _ in range(amount):
                    uls_name = create_unique_resource_name(
                        resource_description="uls", resource_type=cloud.lower()
                    )
                    try:
                        ulsMap[cloud.lower()].create_uls(uls_name, region)
                    except AttributeError as e:
                        raise Exception(
                            f"{cloud} was initialized as None, "
                            "please verify the needed credentials were set in auth.yaml,\n"
                            "or refer to the getting started guide"
                            "(https://ocs-ci.readthedocs.io/en/latest/docs/"
                            "getting_started.html#authentication-config)"
                        ).with_traceback(e.__traceback__)
                    all_created_uls[cloud].add(uls_name)
                    current_call_created_uls[cloud.lower()].add(uls_name)

            return current_call_created_uls

    def uls_cleanup():
        for cloud, uls_set in all_created_uls.items():
            client = ulsMap.get(cloud)
            if client is not None:
                all_existing_uls = client.get_all_uls_names()
                for uls in uls_set:
                    if uls in all_existing_uls:
                        log.info(f"Cleaning up uls {uls}")
                        client.delete_uls(uls)
                    else:
                        log.warning(f"Underlying Storage {uls} not found.")

    request.addfinalizer(uls_cleanup)

    return _create_uls
