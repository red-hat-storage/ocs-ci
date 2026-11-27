"""
This module installs IBM Storage Scale (CNSA) via OLM.

CNSA is what actually backs FA-MDR: the ``scale.spectrum.ibm.com`` CRDs
(Cluster, Daemon, Filesystem, LocalDisk, FilesystemRecoveryData) that
:mod:`ocs_ci.deployment.fusion_access_storage` provisions and verifies all come
from ``ibm-spectrum-scale-operator``.

Both packages live in the FDF catalog, ``isf-data-foundation-catalog``. Nothing
new had to be added for any of it: the catalog has a template and constants
already (``FDF_CATSRC_CR``, ``FDF_CATALOG_NAME``, ``FDF_CATSRC_IMAGE_PATH``,
used by :func:`ocs_ci.helpers.dr_helpers.setup_fdf_catsrc_for_hub`) and the
generic templates under ``ocs_ci/templates/operators`` cover the namespace,
OperatorGroup and Subscription.

Deployment flow:
  1. Mirror the IBM registries, when the catalog is a pre-release build.
  2. Make sure the FDF CatalogSource is on the cluster and READY, creating it
     from the existing template when a full FDF deployment has not already put
     it there.
  3. Install ``cnsa-dependencies`` (brings in kernel-module-management, which
     builds the GPFS kmod).
  4. Install ``ibm-spectrum-scale-operator``.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.operators import Operator
from ocs_ci.utility.utils import TimeoutSampler, wait_for_machineconfigpool_status

logger = logging.getLogger(__name__)

# What odf-operator stamps on the OperatorGroup it owns.
ODF_MANAGED_OPERATOR_GROUP_LABEL = "odf.openshift.io/managed-by-odf-operator"


class ScaleCatalogOperator(Operator):
    """
    Shared base for the two packages served by the FDF catalog.

    ``channel_key`` names the DEPLOYMENT config key that overrides
    ``default_channel``, so a different catalog build can be driven from a conf
    without touching the code.
    """

    catalog_name = constants.FDF_CATALOG_NAME
    namespace = constants.IBM_STORAGE_SCALE_NAMESPACE
    default_channel: str = None
    channel_key: str = None

    def __init__(self):
        # Operator.__init__ is deliberately not called. It rewrites catalog_name
        # to "<name>-unreleased-catalog" whenever the OCP version is not GA,
        # which is the right move for the Red Hat FBC operators it was written
        # for but leaves a Subscription pointing at a catalog nobody creates
        # when the packages come from a vendor catalog like this one.
        if not self.name:
            raise ValueError("Child class must define attribute `name`")

    def get_channel(self):
        """
        Return the subscription channel.

        Returns:
            str: the channel from DEPLOYMENT config, else ``default_channel``

        """
        return config.DEPLOYMENT.get(self.channel_key, self.default_channel)

    def subscription_exists(self):
        """
        Check whether something already subscribed to this package.

        Returns:
            bool: True if a Subscription for ``self.name`` is present

        """
        # The fully qualified kind, because a managed cluster imported into ACM
        # also has an apps.open-cluster-management.io Subscription.
        ocp = OCP(kind=constants.SUBSCRIPTION_COREOS, namespace=self.namespace)
        try:
            items = ocp.get(dont_raise=True) or {}
        except CommandFailed:
            return False
        return any(
            item.get("spec", {}).get("name") == self.name
            for item in items.get("items", [])
        )

    def create_subscription(self):
        """
        Subscribe to the package, unless something already did.

        On a Fusion Data Foundation cluster ``odf-operator`` creates both
        ibm-spectrum-scale Subscriptions at FDF install time. Applying ours over
        the top would rewrite the channel it picked, so leave an existing
        Subscription alone and let the CSV wait below confirm it worked.
        """
        if self.subscription_exists():
            logger.info(
                f"A Subscription for '{self.name}' already exists in "
                f"'{self.namespace}', skipping creation"
            )
            return
        super().create_subscription()

    def _customize_post_deployment_steps(self):
        """
        Wait for the operator CSV to reach the Succeeded phase.
        """
        logger.info(f"Waiting for the {self.name} CSV to appear")
        for csv in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=self.name,
            namespace=self.namespace,
        ):
            if csv:
                break
        csv_name = csv[0]["metadata"]["name"]
        logger.info(f"Found CSV '{csv_name}' - waiting for Succeeded phase")
        CSV(resource_name=csv_name, namespace=self.namespace).wait_for_phase(
            phase="Succeeded", timeout=1800
        )
        logger.info(f"CSV '{csv_name}' reached Succeeded phase")
        self._wait_for_operator_pods(csv_name)

    def _wait_for_operator_pods(self, csv_name):
        """
        Make sure the CSV's operator Deployments are actually running.

        A Succeeded CSV does not mean there is an operator. Some
        ibm-spectrum-scale bundles ship
        ``ibm-spectrum-scale-controller-manager`` with ``replicas: 0``, and OLM
        treats a Deployment that wants no pods as satisfied - the CSV goes
        Succeeded with nothing running. Everything downstream then fails a long
        way from the cause: the LUN group wizard finds no nodes to offer, and
        the ``scale.spectrum.ibm.com`` CRs it creates are never reconciled.

        The CSV is what gets patched, not the Deployment. OLM owns the
        Deployment and reverts a direct scale within seconds.

        Args:
            csv_name (str): Name of the CSV to check

        """
        ocp_csv = OCP(kind=constants.CLUSTER_SERVICE_VERSION, namespace=self.namespace)
        deployments = (
            ocp_csv.get(resource_name=csv_name)["spec"]["install"]["spec"].get(
                "deployments"
            )
            or []
        )
        for index, deployment in enumerate(deployments):
            if deployment["spec"].get("replicas") != 0:
                continue
            logger.warning(
                f"CSV '{csv_name}' declares Deployment "
                f"'{deployment['name']}' with replicas 0, so the operator never "
                "starts. Patching the CSV to 1."
            )
            ocp_csv.patch(
                resource_name=csv_name,
                params=(
                    '[{"op": "replace", "path": "/spec/install/spec/deployments/'
                    f'{index}/spec/replicas", "value": 1}}]'
                ),
                format_type="json",
            )

        ocp_deployment = OCP(kind=constants.DEPLOYMENT, namespace=self.namespace)
        for deployment in deployments:
            name = deployment["name"]
            logger.info(f"Waiting for Deployment '{name}' to have an available pod")
            ocp_deployment.wait_for_resource(
                condition="1",
                resource_name=name,
                column="AVAILABLE",
                timeout=600,
                sleep=10,
            )
            logger.info(f"Deployment '{name}' is available")


class CNSADependenciesOperator(ScaleCatalogOperator):
    """
    ``cnsa-dependencies``, the bundle that pulls in kernel-module-management.
    """

    name = constants.CNSA_DEPENDENCIES_OPERATOR_NAME
    default_channel = constants.CNSA_DEPENDENCIES_CHANNEL
    channel_key = "cnsa_dependencies_channel"


class IBMStorageScaleOperator(ScaleCatalogOperator):
    """
    ``ibm-spectrum-scale-operator``, which owns the scale.spectrum.ibm.com CRDs.
    """

    name = constants.IBM_STORAGE_SCALE_OPERATOR_NAME
    default_channel = constants.IBM_STORAGE_SCALE_CHANNEL
    channel_key = "ibm_storage_scale_channel"


def get_scale_catalog_image():
    """
    Build the image reference for the FDF CatalogSource.

    ``fdf_pre_release_registry`` and ``fdf_image_tag`` are the keys the Fusion
    Data Foundation deployment already uses, so a cluster that gets CNSA this
    way and a cluster that gets it through FDF end up on the same catalog build.

    Returns:
        str: the catalog image, empty when no tag is configured

    """
    image_tag = config.DEPLOYMENT.get("fdf_image_tag")
    if not image_tag:
        return ""
    registry = config.DEPLOYMENT.get("fdf_pre_release_registry")
    if config.DEPLOYMENT.get("fdf_pre_release") and registry:
        image_path = f"{registry.rstrip('/')}/{constants.FDF_CATALOG_NAME}"
    else:
        image_path = constants.FDF_CATSRC_IMAGE_PATH
    return f"{image_path}:{image_tag}"


def setup_catalog_image_mirrors():
    """
    Mirror the IBM registries a pre-release catalog build needs.

    A pre-release catalog serves bundles whose operand images are still
    addressed as ``icr.io/cpopen/...``, but those digests only exist in the
    pre-release registry. Without the mirrors the CatalogSource itself comes up
    READY and the Subscription resolves, and then the operator pod sits in
    ImagePullBackOff on ``manifest unknown`` - the failure surfaces two steps
    away from its cause.

    The mirror definitions are not written here. They are extracted from the
    catalog image itself by the Fusion Data Foundation deployment, so a cluster
    that gets CNSA this way mirrors exactly what a cluster running full FDF
    mirrors.

    Applying these rewrites /etc/containers/registries.conf, which rolls the
    MachineConfigPools, so this waits for the nodes to come back before the
    Subscriptions are created.
    """
    if not config.DEPLOYMENT.get("fdf_pre_release"):
        logger.info("Not a pre-release catalog, no image mirrors needed")
        return

    from ocs_ci.deployment.fusion_data_foundation import FusionDataFoundationDeployment

    logger.info("Creating the image mirror sets for the pre-release catalog")
    fdf = FusionDataFoundationDeployment()
    fdf.create_image_tag_mirror_set()
    fdf.create_image_digest_mirror_set()
    wait_for_machineconfigpool_status("all", timeout=1800)


def setup_scale_catalog_source():
    """
    Make sure the FDF CatalogSource is on the cluster and READY.

    A cluster that already has Fusion Data Foundation has this catalog, and it
    is left exactly as FDF built it - Fusion reconciles the object, so a
    re-apply here would either be reverted or move the cluster to a different
    catalog build behind FDF's back. Only when the catalog is absent is it
    created from ``FDF_CATSRC_CR``, which is all CNSA needs: the operators come
    out of the catalog, not out of FDF.

    The image mirrors are set up either way. An existing catalog says nothing
    about them - it can have been created by hand, or by a run that stopped
    before FDF itself went on.

    Raises:
        ValueError: If the CatalogSource is absent and no ``fdf_image_tag`` is
            configured to create it from.

    """
    setup_catalog_image_mirrors()
    catalog_source = CatalogSource(
        resource_name=constants.FDF_CATALOG_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    if catalog_source.is_exist():
        logger.info(
            f"CatalogSource '{constants.FDF_CATALOG_NAME}' already exists, "
            "leaving it as it is"
        )
    else:
        catalog_image = get_scale_catalog_image()
        if not catalog_image:
            raise ValueError(
                f"CatalogSource '{constants.FDF_CATALOG_NAME}' is not present in "
                f"'{constants.MARKETPLACE_NAMESPACE}' and "
                "DEPLOYMENT['fdf_image_tag'] is not set, so it cannot be created. "
                "Set fdf_image_tag, or deploy FDF on this cluster first."
            )
        logger.info(f"Creating CatalogSource '{constants.FDF_CATALOG_NAME}'")
        catsrc_data = templating.load_yaml(constants.FDF_CATSRC_CR)
        catsrc_data["spec"]["image"] = catalog_image
        OCS(**catsrc_data).create()
    logger.info(f"Waiting for CatalogSource '{constants.FDF_CATALOG_NAME}' to be READY")
    catalog_source.wait_for_state("READY")


def reconcile_operator_groups():
    """
    Leave exactly one OperatorGroup in the IBM Storage Scale namespace.

    ``odf-operator`` creates ``ibm-spectrum-scale-operator-group`` as soon as
    Fusion Data Foundation is installed, and it does so unconditionally - it
    does not look for an existing OperatorGroup and does not back off when it
    finds one. So on a cluster where this deployment ran before FDF did, the
    namespace ends up holding two, and OLM then refuses to install anything
    into it:

        Failed | TooManyOperatorGroups | csv created in namespace with
        multiple operatorgroups, can't pick one automatically

    The odf-operator one is the one to keep - it is reconciled, so deleting it
    only brings it straight back. Anything else goes, and so do the CSVs that
    already failed on the ambiguity, because OLM leaves those Failed forever
    rather than retrying them once the namespace is unambiguous again.

    Getting the deleted CSVs reinstalled is :func:`recover_missing_csvs`.
    """
    ocp_og = OCP(kind="operatorgroup", namespace=constants.IBM_STORAGE_SCALE_NAMESPACE)
    groups = (ocp_og.get(dont_raise=True) or {}).get("items", [])
    if len(groups) < 2:
        return

    names = [group["metadata"]["name"] for group in groups]
    logger.warning(
        f"Namespace '{constants.IBM_STORAGE_SCALE_NAMESPACE}' has "
        f"{len(groups)} OperatorGroups: {names}. OLM cannot install into it "
        "until only one is left."
    )
    managed = [
        group
        for group in groups
        if ODF_MANAGED_OPERATOR_GROUP_LABEL in (group["metadata"].get("labels") or {})
    ]
    keep = (managed or groups)[0]["metadata"]["name"]
    logger.info(f"Keeping OperatorGroup '{keep}'")
    for name in names:
        if name != keep:
            logger.info(f"Deleting duplicate OperatorGroup '{name}'")
            ocp_og.delete(resource_name=name)

    ocp_csv = OCP(kind=constants.CLUSTER_SERVICE_VERSION, namespace=ocp_og.namespace)
    for csv in (ocp_csv.get(dont_raise=True) or {}).get("items", []):
        if csv.get("status", {}).get("reason") == "TooManyOperatorGroups":
            name = csv["metadata"]["name"]
            logger.info(f"Deleting CSV '{name}', failed on the duplicate OperatorGroup")
            ocp_csv.delete(resource_name=name)


def recover_missing_csvs():
    """
    Rebuild a Subscription whose CSV has gone missing.

    OLM keys installation off the InstallPlan, not off the CSV. Once an
    InstallPlan reaches phase Complete, deleting the CSV it produced does not
    bring it back - the Subscription parks in ``UpgradePending`` forever, and
    waiting for that CSV times out with nothing in the namespace to show for
    it. That is the state left behind by the duplicate-OperatorGroup cleanup
    in :func:`reconcile_operator_groups`, and by any CSV deleted by hand to
    force a reinstall.

    Deleting the completed InstallPlan is not enough on its own: the
    Subscription keeps ``status.installPlanRef`` pointing at the object that
    is now gone and never resolves again. The Subscription has to go too, so
    the subscribe step below recreates it from scratch - and for packages
    subscribed by ``odf-operator`` rather than by us, so that it recreates its
    own.

    Only Complete InstallPlans are considered, so an install that is still in
    flight - where the CSV is legitimately not there yet - is left alone.
    """
    namespace = constants.IBM_STORAGE_SCALE_NAMESPACE
    ocp_csv = OCP(kind=constants.CLUSTER_SERVICE_VERSION, namespace=namespace)
    present = {
        csv["metadata"]["name"]
        for csv in (ocp_csv.get(dont_raise=True) or {}).get("items", [])
    }

    ocp_sub = OCP(kind=constants.SUBSCRIPTION_COREOS, namespace=namespace)
    stale_subscriptions = {}
    for subscription in (ocp_sub.get(dont_raise=True) or {}).get("items", []):
        status = subscription.get("status", {})
        csv_name = status.get("currentCSV") or status.get("installedCSV")
        if csv_name and csv_name not in present:
            name = subscription["metadata"]["name"]
            logger.warning(
                f"Subscription '{name}' points at CSV '{csv_name}', which is not "
                f"in '{namespace}'"
            )
            stale_subscriptions[name] = csv_name
    if not stale_subscriptions:
        return

    missing = set(stale_subscriptions.values())
    ocp_ip = OCP(kind="InstallPlan", namespace=namespace)
    for install_plan in (ocp_ip.get(dont_raise=True) or {}).get("items", []):
        if install_plan.get("status", {}).get("phase") != "Complete":
            continue
        if not missing.intersection(
            install_plan.get("spec", {}).get("clusterServiceVersionNames", [])
        ):
            continue
        name = install_plan["metadata"]["name"]
        logger.info(f"Deleting completed InstallPlan '{name}'")
        ocp_ip.delete(resource_name=name)

    for name in stale_subscriptions:
        logger.info(f"Deleting Subscription '{name}' so OLM resolves it again")
        ocp_sub.delete(resource_name=name)


def deploy_ibm_storage_scale():
    """
    Install CNSA on the current cluster.

    cnsa-dependencies goes first: ibm-spectrum-scale-operator needs the kernel
    module management operator it brings in before the GPFS kmod can be built.
    """
    setup_scale_catalog_source()
    reconcile_operator_groups()
    recover_missing_csvs()
    logger.info("Installing cnsa-dependencies")
    CNSADependenciesOperator().deploy()
    logger.info("Installing ibm-spectrum-scale-operator")
    IBMStorageScaleOperator().deploy()
    logger.info("IBM Storage Scale (CNSA) installed")
