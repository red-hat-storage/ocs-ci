"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/helpers.py
Insertion point: end of file
Description: Get the first StorageConsumer resource name from the provider cluster

Review and merge this into ocs_ci/helpers/helpers.py before running the test.
"""

def get_storage_consumer_name(provider_index):
    """
    Get the first StorageConsumer resource name on the provider cluster.

    Args:
        provider_index (int): The config index for the provider cluster.

    Returns:
        str: The name of the StorageConsumer resource.

    Raises:
        pytest.skip: If no StorageConsumer resources are found.
    """
    import pytest
    from ocs_ci.framework import config
    from ocs_ci.ocs import ocp

    with config.RunWithConfigContext(provider_index):
        storage_consumer_ocp = ocp.OCP(
            kind="StorageConsumer",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        consumers = storage_consumer_ocp.get().get("items", [])
        if not consumers:
            pytest.skip("No StorageConsumer resources found on provider")
        consumer_name = consumers[0]["metadata"]["name"]
        logger.info(f"Found StorageConsumer: {consumer_name}")
        return consumer_name
