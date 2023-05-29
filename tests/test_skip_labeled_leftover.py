import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftover_label,
)
from ocs_ci.ocs import constants

from ocs_ci.utility import (
    templating,
)

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import BaseTest

log = logging.getLogger(__name__)


@ignore_leftover_label("test-label=test-value")
class TestClassWithMark(BaseTest):
    def test_labeled_leftover_pod_with_mark(self):
        """
        Creates a pod without removing it

        """
        create_labeled_pod(pod_name="marked")


class TestClassWithoutMark(BaseTest):
    def test_labeled_leftover_pod_without_mark(self):
        """
        Creates a pod without removing it

        """
        create_labeled_pod(pod_name="not-marked")


def create_labeled_pod(pod_name):
    """
    Creates a labeled pod

    """
    java_pod_dict = templating.load_yaml(constants.JAVA_SDK_S3_POD_YAML)
    java_pod_dict["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    java_pod_dict["metadata"]["name"] = pod_name

    # Note that the given label here matches the label in line 19
    java_pod_dict["metadata"]["labels"]["test-label"] = "test-value"

    helpers.create_resource(**java_pod_dict)
