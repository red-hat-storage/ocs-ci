"""
General Deployment object
"""
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import get_pods_having_label

log = logging.getLogger(__name__)


class Deployment(OCS):
    """
    A basic Deployment kind resource
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        kwargs:
            See parent class for kwargs information
        """
        super(Deployment, self).__init__(**kwargs)

    @property
    def pods(self):
        """
        Returns list of pods of the Deployment resource

        Returns:
            list: Deployment's pods
        """
        selectors = self.data.get("spec").get("selector").get("matchLabels")
        selectors = [f"{key}={selectors[key]}" for key in selectors.keys()]
        selectors_string = ",".join(selectors)
        return get_pods_having_label(selectors_string, self.namespace)

    @property
    def replicas(self):
        """
        Returns number of replicas for the deployment

        Returns:
            int: Number of replicas
        """
        return self.data.get("status").get("replicas")

    @property
    def revision(self):
        """
        Returns revision of a Deployment resource

        Returns:
            str: revision number
        """
        return (
            self.data.get("metadata")
            .get("annotations")
            .get(constants.REVISION_ANNOTATION)
        )

    def scale(self, replicas, resource_name=None):
        """
        Scale deployment to required number of replicas

        Args:
            replicas (int): number of required replicas
            resource_name (str): name of resouce to querry the revision
        """
        resource_name = resource_name or self.resource_name
        cmd = b"scale --replicas {replicas} {self.kind}/{resource_name}"
        self.ocp.exec_oc_cmd(cmd)

    def set_revision(self, revision, resource_name=None):
        """
        Set revision to a Deployment or a similar resource that supports kubernetes revisions.

        Args:
            revision (int): revision number of the resource
            resource_name (str): name of resouce to querry the revision
        """
        resource_name = resource_name or self.resource_name
        cmd = b"rollout undo {self.kind}/{resource_name} --to-revision={revision}"
        self.ocp.exec_oc_cmd(cmd)


def get_deployments_having_label(label, namespace):
    """
    Fetches deployment resources with given label in given namespace

    Args:
        label (str): label which deployments might have
        namespace (str): Namespace in which to be looked up

    Return:
        list: deployment OCP instances
    """
    ocp_deployment = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
    deployments = ocp_deployment.get(selector=label).get("items")
    deployments = [Deployment(**osd) for osd in deployments]
    return deployments


def get_osd_deployments(osd_label=constants.OSD_APP_LABEL, namespace=None):
    """
    Fetches info about osd deployments in the cluster

    Args:
        osd_label (str): label associated with osd deployments
            (default: defaults.OSD_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: defaults.ROOK_CLUSTER_NAMESPACE)

    Returns:
        list: OSD deployment OCS instances
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    osds = get_deployments_having_label(osd_label, namespace)
    return osds
