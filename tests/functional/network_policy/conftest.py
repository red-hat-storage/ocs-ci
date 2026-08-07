import logging
import os
import tempfile

import pytest

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.helpers.network_policy_helpers import get_all_network_policies
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.templating import dump_data_to_temp_yaml


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def network_policies_present():
    """
    Session-scoped fixture that verifies NetworkPolicies exist
    in the cluster namespace. Skips all tests if none are found.

    Returns:
        list: List of NetworkPolicy resource dicts.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    policies = get_all_network_policies(namespace)
    if not policies:
        pytest.skip(
            f"No NetworkPolicies found in {namespace}. "
            f"Skipping network policy tests."
        )
    return policies


@pytest.fixture()
def foreign_namespace(request):
    """
    Create a temporary namespace for negative (blocked traffic) testing.
    Namespace is cleaned up after the test.

    Returns:
        OCP: Project object for the foreign namespace.
    """
    ns_name = create_unique_resource_name("netpol-test", "namespace")
    proj_obj = helpers.create_project(project_name=ns_name)

    def finalizer():
        try:
            proj_obj.delete_project(proj_obj.namespace)
            proj_obj.wait_for_delete(proj_obj.namespace, timeout=120)
        except Exception as ex:
            logger.warning(f"Failed to clean up namespace {proj_obj.namespace}: {ex}")

    request.addfinalizer(finalizer)
    return proj_obj


@pytest.fixture()
def test_pod_in_foreign_ns(request, foreign_namespace):
    """
    Create a UBI test pod in the foreign namespace for connectivity
    testing. Pod has restricted PodSecurity context.

    Args:
        foreign_namespace: The foreign namespace fixture.

    Returns:
        tuple: (pod_name, namespace) of the test pod.
    """
    ns = foreign_namespace.namespace
    pod_name = "netpol-test-pod"
    pod_data = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": ns,
        },
        "spec": {
            "containers": [
                {
                    "name": "test",
                    "image": "registry.access.redhat.com/ubi9/ubi:9.4",
                    "command": ["sleep", "3600"],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "runAsNonRoot": True,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                }
            ],
        },
    }

    ocp_obj = OCP(kind=constants.POD, namespace=ns)

    def finalizer():
        try:
            ocp_obj.delete(resource_name=pod_name)
        except Exception:
            logger.warning(f"Failed to delete test pod {pod_name}")

    request.addfinalizer(finalizer)

    tmp = tempfile.NamedTemporaryFile(
        mode="w+", prefix="netpol_test_pod_", suffix=".yaml", delete=False
    )
    try:
        dump_data_to_temp_yaml(pod_data, tmp.name)
        tmp.close()
        ocp_obj.exec_oc_cmd(f"apply -f {tmp.name}")
    finally:
        os.unlink(tmp.name)

    ocp_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_name=pod_name,
        timeout=120,
    )

    return pod_name, ns
