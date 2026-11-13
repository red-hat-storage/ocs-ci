"""
Install plan related functionalities
"""

import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    NoInstallPlanForApproveFoundException,
    InstallPlanRecoveryFailedException,
    CommandFailed,
    TimeoutExpiredError,
)
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)

# InstallPlan status.phase values reported by OLM
INSTALL_PLAN_PHASE_COMPLETE = "Complete"
INSTALL_PLAN_PHASE_FAILED = "Failed"


class InstallPlan(OCP):
    """
    This class represent InstallPlan and contains all the related
    functionality.
    """

    def __init__(self, resource_name="", namespace=None, *args, **kwargs):
        """
        Initializer function for InstallPlan class

        Args:
            resource_name (str): Name of install plan
            namespace (str): Namespace of install plan

        """
        super(InstallPlan, self).__init__(
            resource_name=resource_name,
            namespace=namespace,
            kind="InstallPlan",
            *args,
            **kwargs,
        )

    def approve(self):
        """
        Approve install plan.
        """
        self.check_name_is_specified()
        self.patch(params='{"spec": {"approved": true}}', format_type="merge")

    def get_phase(self):
        """
        Get the current status.phase of the install plan.

        Returns:
            str: install plan phase (e.g. "Complete", "Failed", "Installing")
                or None if the install plan no longer exists or has no phase
                reported yet.

        """
        self.check_name_is_specified()
        try:
            return self.get().get("status", {}).get("phase")
        except CommandFailed:
            return None


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
    install_plans = install_plan.get()["items"]
    install_plans_for_approve = [
        InstallPlan(ip["metadata"]["name"], namespace)
        for ip in install_plans
        if ip["spec"]["approved"] is False
    ]
    if raise_exception and not install_plans_for_approve:
        raise NoInstallPlanForApproveFoundException(
            f"No install plan for approve found in namespace {namespace}"
        )
    return install_plans_for_approve


def wait_for_install_plans_to_complete(install_plans, timeout=600):
    """
    Wait for the given install plans to leave their transient phases and report
    the ones that ended in Failed phase.

    OLM marks an install plan as Failed atomically (e.g. when a webhook has no
    endpoints while the operator pod is being upgraded) and does not retry it on
    its own. This helper lets callers detect that failure so they can recover.

    Args:
        install_plans (list): InstallPlan objects to monitor.
        timeout (int): timeout in seconds to wait for completion.

    Returns:
        list: InstallPlan objects that ended in Failed phase (empty list if all
            completed successfully).

    """
    pending = {ip.resource_name: ip for ip in install_plans}
    failed = {}
    try:
        for _ in TimeoutSampler(timeout, sleep=15, func=lambda: pending):
            for name in list(pending):
                phase = pending[name].get_phase()
                if phase == INSTALL_PLAN_PHASE_COMPLETE:
                    logger.info(f"Install plan {name} completed successfully")
                    pending.pop(name)
                elif phase == INSTALL_PLAN_PHASE_FAILED:
                    logger.warning(f"Install plan {name} is in Failed phase")
                    failed[name] = pending.pop(name)
            if not pending:
                break
    except TimeoutExpiredError:
        logger.warning(
            f"Timed out waiting for install plans to complete. Still pending: "
            f"{list(pending)}"
        )
    return list(failed.values())


def wait_for_install_plan_and_approve(
    namespace,
    timeout=960,
    recover_failed_install_plan=False,
    max_recovery_attempts=3,
    recovery_timeout=600,
):
    """
    Wait for install plans ready for approve and approve them.

    When recover_failed_install_plan is enabled, the approved install plans are
    monitored and, if any ends in Failed phase, it is deleted so that OLM
    regenerates a fresh install plan which is then approved again. This works
    around transient failures during z-stream upgrades where the odf-operator
    webhook momentarily has no endpoints while its pod is being upgraded.

    Args:
        namespace (str): namespace of install plan.
        timeout (int): timeout in seconds to wait for an install plan to appear.
        recover_failed_install_plan (bool): if True, delete and re-approve install
            plans that end up in Failed phase.
        max_recovery_attempts (int): maximum number of delete/re-approve attempts
            when recover_failed_install_plan is enabled.
        recovery_timeout (int): timeout in seconds to wait for approved install
            plans to complete before checking for failures.

    Raises:
        TimeoutExpiredError: in case no install plan found in specified timeout.
        InstallPlanRecoveryFailedException: in case install plans keep ending in
            Failed phase after max_recovery_attempts.

    """
    for attempt in range(1, max_recovery_attempts + 1):
        sampler = TimeoutSampler(
            timeout,
            sleep=10,
            func=get_install_plans_for_approve,
            namespace=namespace,
            raise_exception=True,
        )
        approved_install_plans = []
        for install_plans in sampler:
            if install_plans:
                for install_plan in install_plans:
                    install_plan.approve()
                    approved_install_plans.append(install_plan)
                break

        if not recover_failed_install_plan:
            return

        failed_install_plans = wait_for_install_plans_to_complete(
            approved_install_plans, timeout=recovery_timeout
        )
        if not failed_install_plans:
            logger.info("All approved install plans completed successfully")
            return

        failed_names = [ip.resource_name for ip in failed_install_plans]
        logger.warning(
            f"Install plan(s) {failed_names} ended in Failed phase "
            f"(attempt {attempt}/{max_recovery_attempts}). Deleting them so OLM "
            "regenerates a new install plan for re-approval."
        )
        for install_plan in failed_install_plans:
            install_plan.delete(resource_name=install_plan.resource_name)

    raise InstallPlanRecoveryFailedException(
        f"Install plan(s) in namespace {namespace} kept ending in Failed phase "
        f"after {max_recovery_attempts} recovery attempts"
    )
