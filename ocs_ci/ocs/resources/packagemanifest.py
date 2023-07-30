"""
Package manifest related functionalities
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CSVNotFound,
    ChannelNotFound,
    NoInstallPlanForApproveFoundException,
    ResourceNotFoundError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.install_plan import InstallPlan
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler


log = logging.getLogger(__name__)


class PackageManifest(OCP):
    """
    This class represent PackageManifest and contains all related methods.
    """

    def __init__(
        self,
        resource_name="",
        namespace=constants.MARKETPLACE_NAMESPACE,
        install_plan_namespace=None,
        subscription_plan_approval="Automatic",
        **kwargs,
    ):
        """
        Initializer function for PackageManifest class

        Args:
            resource_name (str): Name of package manifest
            namespace (str): Namespace of package manifest
            install_plan_namespace (str): install_plan_namespace
            subscription_plan_approval (str): subscription plan approval:
                Automatic or Manual

        """
        self.install_plan_namespace = install_plan_namespace or (
            config.ENV_DATA.get("cluster_namespace")
        )
        self.subscription_plan_approval = subscription_plan_approval
        super(PackageManifest, self).__init__(
            namespace=namespace,
            resource_name=resource_name,
            kind="packagemanifest",
            **kwargs,
        )

    @retry(ResourceNotFoundError, tries=10, delay=10, backoff=1)
    def get(self, **kwargs):
        """
        Overloaded get method from OCP class.

        Raises:
            ResourceNotFoundError: In case the selector and resource_name
                specified and no such resource found.
        """
        resource_name = kwargs.get("resource_name", "")
        resource_name = resource_name if resource_name else self.resource_name
        selector = kwargs.get("selector")
        selector = selector if selector else self.selector

        data = super(PackageManifest, self).get(**kwargs)
        if isinstance(data, dict) and (data.get("kind") == "List"):
            items = data["items"]
            data_len = len(items)
            if data_len == 0 and selector and resource_name:
                raise ResourceNotFoundError(
                    f"Requested packageManifest: {resource_name} with "
                    f"selector: {selector} not found!"
                )
            if data_len == 1:
                return items[0]
            if data_len > 1 and resource_name:
                items_match_name = [
                    i for i in items if i["metadata"]["name"] == resource_name
                ]
                if len(items_match_name) == 1:
                    return items_match_name[0]
                if len(items_match_name) == 0:
                    raise ResourceNotFoundError(
                        f"Requested packageManifest: {resource_name} with "
                        f"selector: {selector} not found!"
                    )
                else:
                    return items_match_name
        return data

    @retry((CommandFailed), tries=100, delay=5, backoff=1)
    def get_default_channel(self):
        """
        Returns default channel for package manifest

        Returns:
            str: default channel name

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_name_is_specified()
        try:
            return self.data["status"]["defaultChannel"]
        except KeyError as ex:
            log.error(
                "Can't get default channel for package manifest. "
                "Value of self.data attribute: %s",
                str(self.data),
            )
            raise ex

    def get_channels(self):
        """
        Returns channels for package manifest

        Returns:
            list: available channels for package manifest

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_name_is_specified()
        try:
            return self.data["status"]["channels"]
        except KeyError as ex:
            log.error(
                "Can't get channels for package manifest. "
                "Value of self.data attribute: %s",
                str(self.data),
            )
            raise ex

    def get_current_csv(self, channel=None, csv_pattern=constants.OCS_CSV_PREFIX):
        """
        Returns current csv for default or specified channel

        Args:
            channel (str): Channel of the CSV
            csv_pattern (str): CSV name pattern - needed for manual subscription
                plan

        Returns:
            str: Current CSV name

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.
            ChannelNotFound: in case the required channel doesn't exist.

        """
        self.check_name_is_specified()
        channel = channel if channel else self.get_default_channel()
        channels = self.get_channels()
        if self.subscription_plan_approval == "Manual":
            try:
                return self.get_installed_csv_from_install_plans(
                    pattern=csv_pattern,
                )
            except NoInstallPlanForApproveFoundException:
                log.debug(
                    "All install plans approved, continue to get the CSV name "
                    "from the packageManifest"
                )
            except CSVNotFound:
                log.warning(
                    "No CSV found from any installPlan, continue to get the CSV"
                    " name from the packageManifest"
                )
        for _channel in channels:
            if _channel["name"] == channel:
                return _channel["currentCSV"]
        channel_names = [_channel["name"] for _channel in channels]
        raise ChannelNotFound(
            f"Channel: {channel} not found in available channels: " f"{channel_names}"
        )

    def get_installed_csv_from_install_plans(self, pattern):
        """
        Get currently installed CSV out latest approved install plans.

        Args:
            patter (str): pattern of CSV name to look for.

        Raises:
            CSVNotFound: In case no CSV found from approved install plans.
            NoInstallPlanForApproveFoundException: In case no install plan
                for approve found.

        """
        install_plan = InstallPlan(namespace=self.install_plan_namespace)
        install_plans = install_plan.get()["items"]
        not_approved_install_plans = [
            ip for ip in install_plans if not ip["spec"]["approved"]
        ]
        if not not_approved_install_plans:
            raise NoInstallPlanForApproveFoundException(
                "No insall plan for approve found!"
            )
        sorted_install_plans = sorted(
            install_plans,
            key=lambda ip: ip["metadata"]["creationTimestamp"],
            reverse=True,
        )
        for ip in sorted_install_plans:
            for csv_name in ip["spec"]["clusterServiceVersionNames"]:
                if pattern in csv_name and ip["spec"]["approved"]:
                    return csv_name
        raise CSVNotFound("No CSV found from approved install plans")

    def wait_for_resource(
        self,
        resource_name="",
        timeout=60,
        sleep=3,
        label=None,
    ):
        """
        Wait for a packagemanifest exists.

        Args:
            resource_name (str): The name of the resource to wait for.
                If not specified the self.resource_name will be used. At least
                on of those has to be set!
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.
            TimeoutExpiredError: in case the resource not found in timeout

        """
        log.info(
            f"Waiting for a resource(s) of kind {self._kind}"
            f" identified by name '{resource_name}'"
        )
        resource_name = resource_name if resource_name else self.resource_name
        self.check_name_is_specified(resource_name)

        for sample in TimeoutSampler(timeout=timeout, sleep=sleep, func=self.get):
            if sample.get("metadata", {}).get("name") == resource_name:
                log.info(f"package manifest {resource_name} found!")
                return
            log.info(f"package manifest {resource_name} not found!")


def get_selector_for_ocs_operator():
    """
    This is the helper function which returns selector for package manifest.
    It's needed because of conflict with live content and multiple package
    manifests with the ocs-operator name. In case we are using internal builds
    we label catalog source or operator source and using the same selector for
    package manifest.

    Returns:
        str: Selector for package manifest if we are on internal
            builds, otherwise it returns None
    """
    catalog_source = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
        selector=constants.OPERATOR_INTERNAL_SELECTOR,
    )
    try:
        cs_data = catalog_source.get()
        if cs_data["items"]:
            return constants.OPERATOR_INTERNAL_SELECTOR
    except CommandFailed:
        log.info("Internal catalog source not found!")
    operator_source = OCP(
        kind="OperatorSource",
        resource_name=constants.OPERATOR_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    try:
        operator_source.get()
        return constants.OPERATOR_INTERNAL_SELECTOR
    except CommandFailed:
        log.info("Catalog source not found!")
