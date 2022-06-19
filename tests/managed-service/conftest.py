import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import ocp


logger = logging.getLogger(__name__)


@pytest.fixture(scope="function", autouse=True)
def get_consumer_clusters():
    logger.info("Get Consumer Clusters on setup")
    consumer_clusters = list()
    for index in range(config.nclusters):
        if config.clusters[index].ENV_DATA["cluster_type"] == "consumer":
            consumer_clusters.append(index)
    config.index_consumer_clusters = consumer_clusters


@pytest.fixture(scope="function")
def teardown_project_ms_factory(request):
    return teardown_project_factory_ms_fixture(request)


def teardown_project_factory_ms_fixture(request):
    """
    Tearing down a project that was created during the test
    To use this factory, you'll need to pass 'teardown_project_factory' to your test
    function and call it in your test when a new project was created and you
    want it to be removed in teardown phase:
    def test_example(self, teardown_project_factory):
        project_obj = create_project(project_name="xyz")
        teardown_project_factory(project_obj)
    """
    instances = []

    def factory(resource_obj):
        """
        Args:
            resource_obj (OCP object or list of OCP objects) : Object to teardown after the test

        """
        if isinstance(resource_obj, list):
            instances.extend(resource_obj)
        else:
            instances.append(resource_obj)

    def finalizer():
        delete_projects(instances)

    request.addfinalizer(finalizer)
    return factory


def delete_projects(instances):
    """
    Delete the project

    instances (list): list of OCP objects (kind is Project)

    """
    for index in config.index_consumer_clusters:
        config.switch_ctx(index)
        for instance in instances:
            try:
                ocp_event = ocp.OCP(kind="Event", namespace=instance.namespace)
                events = ocp_event.get()
                event_count = len(events["items"])
                warn_event_count = 0
                for event in events["items"]:
                    if event["type"] == "Warning":
                        warn_event_count += 1
                logger.info(
                    (
                        "There were %d events in %s namespace before it's"
                        " removal (out of which %d were of type Warning)."
                        " For a full dump of this event list, see DEBUG logs."
                    ),
                    event_count,
                    instance.namespace,
                    warn_event_count,
                )
            except Exception:
                # we don't want any problem to disrupt the teardown itself
                logger.exception("Failed to get events for project %s", instance.namespace)
            ocp.switch_to_default_rook_cluster_project()
            instance.delete(resource_name=instance.namespace)
            instance.wait_for_delete(instance.namespace, timeout=300)
