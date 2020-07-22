"""
Install plan related functionalities
"""
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import NoInstallPlanForApproveFoundException
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


class InstallPlan(OCP):
    """
    This class represent InstallPlan and contains all the related
    functionality.
    """

    def __init__(
        self, resource_name="", namespace=None, *args, **kwargs
    ):
        """
        Initializer function for InstallPlan class

        Args:
            resource_name (str): Name of install plan
            namespace (str): Namespace of install plan

        """
        super(InstallPlan, self).__init__(
            resource_name=resource_name, namespace=namespace,
            kind='InstallPlan', *args, **kwargs,
        )

    def approve(self):
        """
        Approve install plan.
        """
        self.check_name_is_specified()
        self.patch(params='{"spec": {"approved": true}}', format_type='merge')


def get_install_plans_for_approve(namespace, raise_exception=False):
    """
    Get all install plans for approve

    Args:
        namespace (str): namespace of CSV
        raise_exception (bool): True if the function should raise the exception
            when no install plan found for approve.

    Returns:
        list: found install plans for approve

    Raises:
        NoInstallPlanForApproveFoundException: in case raise_exception is True
            and no install plan for approve found.

    """

    install_plan = InstallPlan(namespace=namespace)
    install_plans = install_plan.get()['items']
    install_plans_for_approve = [
        InstallPlan(ip['metadata']['name'], namespace) for ip
        in install_plans if ip['spec']['approved'] is False
    ]
    if raise_exception and not install_plans_for_approve:
        raise NoInstallPlanForApproveFoundException(
            f"No install plan for approve found in namespace {namespace}"
        )
    return install_plans_for_approve


def wait_for_install_plan_and_approve(namespace, timeout=960):
    """
    Wait for install plans ready for approve and approve them.

    Args:
        namespace (str): namespace of install plan.
        timeout (int): timeout in seconds.

    Raises:
        TimeoutExpiredError: in case no install plan found in specified
            timeout.

    """
    sampler = TimeoutSampler(
        timeout, sleep=10, func=get_install_plans_for_approve,
        namespace=namespace, raise_exception=True
    )
    for install_plans in sampler:
        if install_plans:
            for install_plan in install_plans:
                install_plan.approve()
            return


def get_install_plans_count(namespace=None):
    """
    Returns number of installPlans.

    Returns:
        int: Number of install plans found.

    """
    install_plans = InstallPlan(namespace=namespace).get()
    install_plans_count = len(
        install_plans.get('items', [])
    )
    logger.info(f"Number of installPlans: {install_plans_count}")
    return install_plans_count
