from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.framework.testlib import managed_service_required
from ocs_ci.ocs.managedservice import update_pull_secret, update_non_ga_version


@libtest
@managed_service_required
def test_update_catalog_source():
    """
    Perform update of catalogsource, deployer and pull secret to match versions
    defined in configuration.
    """
    update_pull_secret()
    update_non_ga_version()
