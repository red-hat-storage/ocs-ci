# -*- coding: utf8 -*-


import logging

from ocs_ci.deployment.azure import AZUREIPI
from ocs_ci.framework import config
from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import azure_platform_required


logger = logging.getLogger(__name__)


@libtest
@azure_platform_required
def test_assumptions():
    """
    Check basic consistency in platform handling.
    """
    assert config.ENV_DATA["platform"] == constants.AZURE_PLATFORM


@libtest
@azure_platform_required
def test_azure_cluster_resource_group_loading():
    """
    Check that no exception is raised during loading of Azure cluster resource
    group, and that it's value is not None.
    """
    azure_depl = AZUREIPI()
    assert azure_depl.azure_util.cluster_resource_group is not None


@libtest
@azure_platform_required
def test_azure_service_principal_credentials_loading():
    """
    Check that no exception is raised during loading of Azure credentials,
    and that the credentials are not None.
    """
    azure_depl = AZUREIPI()
    assert azure_depl.azure_util.credentials is not None


@libtest
@azure_platform_required
def test_check_cluster_existence():
    """
    Simple test of Azure check_cluster_existence() method implementation.
    Invalid clustername should be evaluated as False, while current cluster
    name should result in True (obviously current cluster name exists).
    """
    azure_depl = AZUREIPI()
    assert not azure_depl.check_cluster_existence("an_invalid_clustername000")
    assert azure_depl.check_cluster_existence(azure_depl.cluster_name)
    assert azure_depl.check_cluster_existence(azure_depl.cluster_name[:5])


@libtest
@azure_platform_required
def test_get_vm_names():
    """
    Test of Azure get_vm_names() method implementation.
    OCS cluster must have at-least 3 worker and 3 master nodes.
    """
    azure_depl = AZUREIPI()
    vm_name = azure_depl.azure_util.get_vm_names()
    logger.info(f"vm names are: {vm_name}")
    master_vms = [master_vm for master_vm in vm_name if "master" in master_vm]
    assert len(master_vms) >= 3
    worker_vms = [worker_vm for worker_vm in vm_name if "worker" in worker_vm]
    assert len(worker_vms) >= 3


@libtest
@azure_platform_required
def test_get_vm_power_status():
    """
    Test of Azure get_vm_power_status() method implementation.
    VM  of healthy OCS Cluster has 'running' status by default.
    """
    azure_depl = AZUREIPI()
    vm_names = azure_depl.azure_util.get_vm_names()
    logger.info(f"vm names are: {vm_names}")
    status = azure_depl.azure_util.get_vm_power_status(vm_names[0])
    assert "running" == status, f"Status of {vm_names[0]} is {status}"
