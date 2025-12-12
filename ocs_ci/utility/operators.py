"""
Operators utility functions
"""

# Properly order all imports

from ocs_ci.deployment.disconnected import prune_and_mirror_index_image
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.framework import config
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.exceptions import ResourceNotFoundError, CommandFailed
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.version import (
    get_semantic_ocp_version_from_config,
    get_ocp_ga_version,
    get_ocp_version,
    get_semantic_version,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import wait_for_machineconfigpool_status

import logging
import os
import yaml
from distutils.version import LooseVersion
from tempfile import NamedTemporaryFile


logger = logging.getLogger(__name__)


# Operators related constants
UNRELEASED_OPERATORS_FBC_IMAGE = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"
OPERATORS_TEMPLATES_DIR = os.path.join(constants.TEMPLATE_DIR, "operators")
UNRELEASED_DEFAULT_MIRROR = (
    "quay.io/redhat-user-workloads/ocp-art-tenant/art-images-share"
)


class Operator:
    # expected to be overridden by child classes
    name: str = None
    catalog_name: str = constants.OPERATOR_CATALOG_SOURCE_NAME
    unreleased_catalog_image: str = UNRELEASED_OPERATORS_FBC_IMAGE
    unreleased_catalog_image_tag: str = None
    namespace: str = None
    disconnected_required_packages: list[str] = []
    """
    Get the list of related images from the unreleased catalog image
    Example:
    ```
    oras discover --format \
        json quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:ocp__4.21__kubernetes-nmstate-rhel9-operator \
        | jq -r '.referrers[] | select(.artifactType == "application/vnd.konflux-ci.attached-artifact") \
        | .digest' | xargs -I {} oras pull \
        quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc@{} && cat related-images.json \
        | jq -r '.[]' | sed 's/@sha256:.*//'
    ```
    """
    unreleased_images: list[str] = []
    unreleased_mirror: str = UNRELEASED_DEFAULT_MIRROR

    def __init__(self, create_catalog: bool = False):
        """
        Initialize the operator

        Args:
            create_catalog (bool): whether to create the catalog if necessary for the operator
        """
        if not self.name:
            raise ValueError("Child class must define attribute `name`")
        if not self.catalog_name:
            raise ValueError("Child class must define attribute `catalog_name`")
        if not self.namespace:
            raise ValueError("Child class must define attribute `namespace`")
        ocp_version = get_semantic_ocp_version_from_config()
        ocp_ga_version = get_ocp_ga_version(ocp_version)
        available = self.is_available()
        if not ocp_ga_version or not available:
            self.set_unreleased_catalog()
            if create_catalog:
                self.create_catalog()

    @property
    def unreleased_catalog_full_image(self):
        if self.unreleased_catalog_image_tag and self.unreleased_catalog_image:
            return (
                f"{self.unreleased_catalog_image}:{self.unreleased_catalog_image_tag}"
            )
        raise ValueError(
            "Child class must define attribute `unreleased_catalog_image_tag` and `unreleased_catalog_image`"
        )

    @property
    def catalog_selector(self):
        return f"catalog={self.catalog_name}"

    @property
    def unreleased_idms_name(self):
        return f"{self.name}-unreleased-idms"

    @property
    def unreleased_catalog_name(self):
        return f"{self.name}-unreleased-catalog"

    def set_unreleased_catalog(self):
        if not self.unreleased_catalog_name:
            raise ValueError(
                "Child class must define attribute `unreleased_catalog_name`"
            )
        self.catalog_name = self.unreleased_catalog_name

    def get_idms_data(self):
        if not self.unreleased_images:
            raise ValueError(
                "Child class must define attribute `unreleased_images_source_mirrors`"
            )
        unreleased_catalog_idms_data = templating.load_yaml(
            os.path.join(
                OPERATORS_TEMPLATES_DIR, "unreleased-operator-catalog-idms.yaml"
            )
        )
        unreleased_catalog_idms_data["metadata"]["name"] = self.catalog_name
        unreleased_catalog_idms_data["spec"]["imageDigestMirrors"] = []
        for image in self.unreleased_images:
            unreleased_catalog_idms_data["spec"]["imageDigestMirrors"].append(
                {
                    "mirrors": [self.unreleased_mirror],
                    "source": image,
                }
            )
        return unreleased_catalog_idms_data

    def create_idms_for_unreleased_catalog(self):
        idms_data = self.get_idms_data()
        unreleased_catalog_idms_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-unreleased-catalog", delete=False
        )
        with open(unreleased_catalog_idms_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(idms_data))
        run_cmd(f"oc apply -f {unreleased_catalog_idms_data_yaml.name}")

    def create_catalog(self):
        """
        Create a catalog for the operator
        """
        if config.DEPLOYMENT.get("disconnected"):
            self.create_disconnected_catalog()
        else:
            self.create_unreleased_catalog()

    def _create_catalog(self, image: str, name: str):
        """
        Create a catalog for the operator

        Args:
            image (str): the image to use for the catalog
            name (str): the name of the catalog
        """
        catalog_data = templating.load_yaml(
            os.path.join(OPERATORS_TEMPLATES_DIR, "unreleased-operator-catalog.yaml")
        )
        catalog_data["spec"]["image"] = image
        catalog_data["metadata"]["name"] = name
        catalog_data_yaml = NamedTemporaryFile(
            mode="w+", prefix=f"{self.name}-catalog", delete=False
        )
        with open(catalog_data_yaml.name, "w") as fd:
            fd.write(yaml.dump(catalog_data))
        run_cmd(f"oc apply -f {catalog_data_yaml.name}")
        wait_for_machineconfigpool_status("all", force_delete_pods=False)

    def create_unreleased_catalog(self):
        """
        Create the unreleased catalog for the operator
        """
        if not self.unreleased_catalog_name:
            raise ValueError(
                "Child class must define attribute `unreleased_catalog_name`"
            )
        if not self.unreleased_catalog_image:
            raise ValueError(
                "Child class must define attribute `unreleased_catalog_image`"
            )
        self.create_idms_for_unreleased_catalog()
        self._create_catalog(
            self.unreleased_catalog_full_image, self.unreleased_catalog_name
        )

    def create_disconnected_catalog(self):
        # in case of disconnected environment, we have to mirror all the
        # optional_operators images
        idms = self.get_idms_data()
        mirrored_index_image = (
            f"{config.DEPLOYMENT['mirror_registry']}/"
            f"{self.unreleased_catalog_full_image.split('/', 1)[-1]}"
        )
        prune_and_mirror_index_image(
            self.unreleased_catalog_full_image,
            mirrored_index_image,
            self.disconnected_required_packages,
            idms=idms,
        )
        self._create_catalog(mirrored_index_image, self.unreleased_catalog_name)

    def is_available(self):
        """
        Get the version of the operator

        Returns:
            bool: True if the operator is available, False otherwise

        """
        package_manifest = PackageManifest(
            resource_name=self.name,
            selector=f"catalog={self.catalog_name}",
        )
        try:
            package_manifest.get()
            return True
        except ResourceNotFoundError:
            return False

    @retry(CommandFailed, 5, 30, 1)
    def get_channel(self):
        """
        Get the channel to use for installing the operator

        Returns:
            str: operator channel

        """
        ocp_version = get_ocp_version()
        selector = self.catalog_selector
        # Retrieve available channels for LSO
        package_manifest = PackageManifest(resource_name=self.name, selector=selector)
        channels = package_manifest.get_channels()

        versions = []
        stable_channel_found = False
        for channel in channels:
            if ocp_version == channel["name"]:
                return ocp_version
            else:
                if channel["name"] != "stable":
                    versions.append(LooseVersion(channel["name"]))
                else:
                    logger.debug(f"channel with name {channel['name']} found")
                    stable_channel_found = True
                    stable_channel_full_version = channel["currentCSVDesc"]["version"]
                    stable_channel_version = get_semantic_version(
                        stable_channel_full_version, only_major_minor=True
                    )

        # Ensure versions are sorted
        versions.sort()
        sorted_versions = [v.vstring for v in versions]

        if len(sorted_versions) >= 1:
            # Use latest channel
            if stable_channel_found:
                if stable_channel_version > get_semantic_version(sorted_versions[-1]):
                    return "stable"
                else:
                    return sorted_versions[-1]
        else:
            return channels[-1]["name"]


class NMStateOperator(Operator):
    def __init__(self, create_catalog: bool = False):
        self.name = constants.NMSTATE_OPERATOR
        ocp_version = get_semantic_ocp_version_from_config()
        self.unreleased_catalog_image_tag: str = (
            f"ocp__{ocp_version}__kubernetes-nmstate-rhel9-operator"
        )
        self.unreleased_images = [
            "registry.redhat.io/openshift4/kubernetes-nmstate-operator-bundle",
            "registry.redhat.io/openshift4/kubernetes-nmstate-rhel9-operator",
            "registry.redhat.io/openshift4/nmstate-console-plugin-rhel9",
            "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9",
            "registry.redhat.io/openshift4/ose-kubernetes-nmstate-handler-rhel9",
        ]
        self.namespace = constants.NMSTATE_NAMESPACE
        self.disconnected_required_packages = [
            "kubernetes-nmstate-operator",
        ]
        super().__init__(create_catalog)


class LocalStorageOperator(Operator):
    def __init__(self, create_catalog: bool = False):
        self.name = constants.LOCAL_STORAGE_OPERATOR_NAME
        ocp_version = get_semantic_ocp_version_from_config()
        self.unreleased_catalog_image_tag: str = (
            f"ocp__{ocp_version}__ose-local-storage-rhel9-operator"
        )
        self.unreleased_images = [
            "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9",
            "registry.redhat.io/openshift4/ose-local-storage-diskmaker-rhel9",
            "registry.redhat.io/openshift4/ose-local-storage-mustgather-rhel9",
            "registry.redhat.io/openshift4/ose-local-storage-operator-bundle",
            "registry.redhat.io/openshift4/ose-local-storage-rhel9-operator",
        ]
        self.disconnected_required_packages = [
            "local-storage-operator",
        ]
        self.namespace = constants.LOCAL_STORAGE_NAMESPACE
        super().__init__(create_catalog)
