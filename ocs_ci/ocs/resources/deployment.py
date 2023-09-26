"""
General Deployment object
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


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
        return [
            Pod(**pod_data)
            for pod_data in get_pods_having_label(selectors_string, self.namespace)
        ]

    @property
    def replicas(self):
        """
        Returns number of replicas for the deployment as defined in its spec

        Returns:
            int: Number of replicas
        """
        self.reload()
        return self.data.get("spec").get("replicas")

    @property
    def available_replicas(self):
        """
        Returns number of available replicas for the deployment

        Returns:
            int: Number of replicas
        """
        self.reload()
        return self.data.get("status").get("availableReplicas", 0)

    @property
    def revision(self):
        """
        Returns revision of a Deployment resource

        Returns:
            str: revision number
        """
        self.reload()
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
        resource_name = resource_name or self.name
        cmd = f"scale --replicas {replicas} {self.kind}/{resource_name}"
        self.ocp.exec_oc_cmd(cmd)

    def set_revision(self, revision, resource_name=None):
        """
        Set revision to a Deployment or a similar resource that supports kubernetes revisions.

        Args:
            revision (int): revision number of the resource
            resource_name (str): name of resouce to querry the revision
        """
        resource_name = resource_name or self.name
        cmd = f"rollout undo {self.kind}/{resource_name} --to-revision={revision}"
        self.ocp.exec_oc_cmd(cmd)

    def wait_for_available_replicas(self, timeout=15, sleep=3):
        """
        Wait for number of available replicas reach number of desired replicas.

        Args:
            timeout (int): Timeout in seconds
            sleep (int): Sleep interval in seconds
        """
        desired_replicas = self.replicas
        if desired_replicas is None:
            logger.warning("Number of desired replicas is missing. Trying to reload.")
            for _ in TimeoutSampler(30, 5, func=self.reload):
                desired_replicas = self.data.get("status").get("replicas")
                if desired_replicas is not None:
                    break
        logger.info(
            f"Waiting for deployment {self.name} to reach "
            f"desired number of replicas ({desired_replicas})"
        )

        def _get_available_replicas():
            nonlocal desired_replicas
            available_replicas = self.available_replicas
            logger.info(
                f"Deployment/{self.name}: {available_replicas} of {desired_replicas} available"
            )
            return available_replicas

        for available_replicas in TimeoutSampler(360, 2, func=_get_available_replicas):
            if available_replicas == desired_replicas:
                logger.info(
                    f"Deployment {self.name} reached "
                    f"desired number of replicas ({available_replicas})"
                )
                break


def get_deployments_having_label(label, namespace):
    """
    Fetches deployment resources with given label in given namespace

    Args:
        label (str): label which deployments might have
        namespace (str): Namespace in which to be looked up

    Returns:
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
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list: OSD deployment OCS instances
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    osds = get_deployments_having_label(osd_label, namespace)
    return osds


def get_mon_deployments(mon_label=constants.MON_APP_LABEL, namespace=None):
    """
    Fetches info about mon deployments in the cluster

    Args:
        mon_label (str): label associated with mon deployments
            (default: defaults.MON_APP_LABEL)
        namespace (str): Namespace in which ceph cluster lives
            (default: config.ENV_DATA["cluster_namespace"])

    Returns:
        list: Mon deployment OCS instances
    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    mons = get_deployments_having_label(mon_label, namespace)
    return mons
