import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.fusionaccess_dr_helpers import (
    APP_TYPE_APPSET_VM,
    APP_TYPE_BUSYBOX_SUBSCRIPTION,
    APP_TYPE_DISCOVERED_RECIPE,
    APP_TYPE_DISCOVERED_VM,
    FusionAccessDRApp,
)
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out the Fusion Access Metro-DR tests when the
    cluster under test is not a Metro-DR cluster.

    Args:
        items: list of collected tests

    """
    if config.MULTICLUSTER.get("multicluster_mode") == constants.MDR_MODE:
        return

    for item in items.copy():
        if "fusion-access/metro-dr" in str(item.fspath):
            logger.debug(
                f"Test {item} is removed from the collected items. "
                f"Test runs only on Metro-DR clusters"
            )
            items.remove(item)


@pytest.fixture()
def fusionaccess_dr_workloads(
    cnv_dr_workload,
    discovered_apps_dr_workload_cnv,
    discovered_apps_dr_workload,
    dr_workload,
):
    """
    Deploy every application type used by the Fusion Access Metro-DR tests.

    A Fusion Access Metro-DR test case has to exercise ApplicationSet based CNV
    VMs, Discovered Apps based CNV VMs, Recipe protected Discovered Apps and a
    plain busybox Deployment application at the same time. This fixture wraps
    the four workload fixtures that deploy them and returns the deployed
    workloads as a single list of :class:`FusionAccessDRApp` objects, so test
    cases can drive all of them through one uniform interface.

    Teardown of the deployed workloads is handled by the wrapped fixtures.

    """

    def factory(
        num_of_appset_push_vm=0,
        num_of_appset_pull_vm=1,
        num_of_discovered_vm=1,
        num_of_recipe_apps=1,
        num_of_busybox_apps=1,
        vm_type=constants.VM_VOLUME_PVC,
        pvc_interface=constants.CEPHBLOCKPOOL,
    ):
        """
        Args:
            num_of_appset_push_vm (int): Number of CNV VMs deployed as push
                model ApplicationSet applications
            num_of_appset_pull_vm (int): Number of CNV VMs deployed as pull
                model ApplicationSet applications
            num_of_discovered_vm (int): Number of CNV VMs deployed as
                Discovered Apps
            num_of_recipe_apps (int): Number of Recipe protected Discovered
                Apps workloads
            num_of_busybox_apps (int): Number of busybox Deployment
                (Subscription based) applications
            vm_type (str): CNV VM volume type, one of the ``VM_VOLUME_*``
                constants
            pvc_interface (str): 'CephBlockPool' or 'CephFileSystem', decides
                whether RBD or CephFS backed resources are created

        Returns:
            list: FusionAccessDRApp objects, one per deployed application

        """
        apps = []

        num_of_appset_vm = num_of_appset_push_vm + num_of_appset_pull_vm
        if num_of_appset_vm:
            logger.info(
                f"Deploying {num_of_appset_vm} ApplicationSet based CNV VM "
                f"workloads of type {vm_type}"
            )
            appset_vm_workloads = cnv_dr_workload(
                num_of_vm_subscription=0,
                num_of_vm_appset_push=num_of_appset_push_vm,
                num_of_vm_appset_pull=num_of_appset_pull_vm,
                vm_type=vm_type,
            )
            apps.extend(
                FusionAccessDRApp(workload, APP_TYPE_APPSET_VM)
                for workload in appset_vm_workloads
            )

        if num_of_discovered_vm:
            logger.info(
                f"Deploying {num_of_discovered_vm} Discovered Apps based CNV VM "
                f"workloads of type {vm_type}"
            )
            discovered_vm_workloads = discovered_apps_dr_workload_cnv(
                pvc_vm=num_of_discovered_vm, vm_type=vm_type
            )
            apps.extend(
                FusionAccessDRApp(workload, APP_TYPE_DISCOVERED_VM)
                for workload in discovered_vm_workloads
            )

        if num_of_recipe_apps:
            logger.info(
                f"Deploying {num_of_recipe_apps} Recipe protected Discovered Apps "
                f"workloads"
            )
            recipe_workloads = discovered_apps_dr_workload(
                kubeobject=0,
                recipe=num_of_recipe_apps,
                pvc_interface=pvc_interface,
            )
            apps.extend(
                FusionAccessDRApp(workload, APP_TYPE_DISCOVERED_RECIPE)
                for workload in recipe_workloads
            )

        if num_of_busybox_apps:
            logger.info(
                f"Deploying {num_of_busybox_apps} busybox Deployment applications"
            )
            busybox_workloads = dr_workload(
                num_of_subscription=num_of_busybox_apps,
                num_of_appset=0,
                pvc_interface=pvc_interface,
            )
            apps.extend(
                FusionAccessDRApp(workload, APP_TYPE_BUSYBOX_SUBSCRIPTION)
                for workload in busybox_workloads
            )

        logger.info(f"Deployed Fusion Access DR applications: {apps}")
        return apps

    return factory
