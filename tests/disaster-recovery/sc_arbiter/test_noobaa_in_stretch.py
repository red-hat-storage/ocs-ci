import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_by_label_count,
    get_pods_having_label,
    Pod,
    get_pod_node,
)
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    polarion_id,
    tier3,
    red_squad,
    mcg,
    stretchcluster_required,
)

logger = logging.getLogger(__name__)


@pytest.fixture()
def setup_nb_endpoint():
    nb_endpoint_dep = OCP(
        kind="Deployment",
        resource_name="noobaa-endpoint",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    nb_endpoint_dep_data = nb_endpoint_dep.get()
    if nb_endpoint_dep_data["spec"]["replicas"] != 2:
        old_replicas = nb_endpoint_dep_data["spec"]["replicas"]
        modify_deployment_replica_count(nb_endpoint_dep.resource_name, replica_count=2)

    def teardown():
        modify_deployment_replica_count(
            nb_endpoint_dep.resource_name, replica_count=old_replicas
        )

    return nb_endpoint_dep_data


@tier3
@mcg
@red_squad
@stretchcluster_required
@bugzilla("2109101")
@polarion_id("OCS-5406")
def test_nb_endpoint_topology_spread_constraints(setup_nb_endpoint):

    nb_endpoint_dep_data = setup_nb_endpoint
    topology_spread_constraint = nb_endpoint_dep_data["spec"]["template"]["spec"][
        "topologySpreadConstraints"
    ][0]
    assert (
        topology_spread_constraint["labelSelector"]["matchLabels"]["noobaa-s3"]
        == "noobaa"
        and topology_spread_constraint["maxSkew"] == 1
        and topology_spread_constraint["nodeTaintsPolicy"] == "Honor"
        and topology_spread_constraint["topologyKey"] == "kubernetes.io/hostname"
        and topology_spread_constraint["whenUnsatisfiable"] == "ScheduleAnyway"
    ), f"Topology spread constraints are not set as expected: \n {topology_spread_constraint}"
    logger.info("Topology spread constraints are set correctly")

    wait_for_pods_by_label_count(constants.NOOBAA_ENDPOINT_POD_LABEL, exptected_count=2)

    nb_endpoint_pods = [
        Pod(**pod)
        for pod in get_pods_having_label(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
    ]
    zone_label = None
    for pod in nb_endpoint_pods:
        node_obj = get_pod_node(pod)
        node_labels = node_obj.get()["metadata"]["labels"]
        if "topology.kubernetes.io/zone: data-1" in node_labels and zone_label is None:
            zone_label = "topology.kubernetes.io/zone: data-1"
        elif (
            "topology.kubernetes.io/zone: data-2" in node_labels and zone_label is None
        ):
            zone_label = "topology.kubernetes.io/zone: data-2"

        if zone_label:
            assert (
                zone_label not in node_labels
            ), "Endpoints are not spread equally across the zones"
            logger.info("Nooba endpoint pods are equally distributed")
