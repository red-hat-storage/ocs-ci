"""
General OCS object
"""
import logging
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import get_images, OCP
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.utility import templating, utils

log = logging.getLogger(__name__)


class OCS(object):
    """
    Base OCSClass
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        Args:
            kwargs (dict):
                1) For existing resource, use OCP.reload() to get the
                resource's dictionary and use it to pass as **kwargs
                2) For new resource, use yaml files templates under
                /templates/CSI like:
                obj_dict = load_yaml(
                    os.path.join(
                        TEMPLATE_DIR, "some_resource.yaml"
                        )
                    )
        """
        self.data = kwargs
        self._api_version = self.data.get('api_version')
        self._kind = self.data.get('kind')
        self._namespace = None
        if 'metadata' in self.data:
            self._namespace = self.data.get('metadata').get('namespace')
            self._name = self.data.get('metadata').get('name')
        self.ocp = OCP(
            api_version=self._api_version, kind=self.kind,
            namespace=self._namespace
        )
        with tempfile.NamedTemporaryFile(
            mode='w+', prefix=self._kind, delete=False
        ) as temp_file_info:
            self.temp_yaml = temp_file_info.name
        # This _is_delete flag is set to True if the delete method was called
        # on object of this class and was successfull.
        self._is_deleted = False

    @property
    def api_version(self):
        return self._api_version

    @property
    def kind(self):
        return self._kind

    @property
    def namespace(self):
        return self._namespace

    @property
    def name(self):
        return self._name

    @property
    def is_deleted(self):
        return self._is_deleted

    def reload(self):
        """
        Reloading the OCS instance with the new information from its actual
        data.
        After creating a resource from a yaml file, the actual yaml file is
        being changed and more information about the resource is added.
        """
        self.data = self.get()
        self.__init__(**self.data)

    def get(self, out_yaml_format=True):
        return self.ocp.get(
            resource_name=self.name, out_yaml_format=out_yaml_format
        )

    def status(self):
        return self.ocp.get_resource(self.name, 'STATUS')

    def describe(self):
        return self.ocp.describe(resource_name=self.name)

    def create(self, do_reload=True):
        log.info(f"Adding {self.kind} with name {self.name}")
        templating.dump_data_to_temp_yaml(self.data, self.temp_yaml)
        status = self.ocp.create(yaml_file=self.temp_yaml)
        if do_reload:
            self.reload()
        return status

    def delete(self, wait=True, force=False):
        """
        Delete the OCS object if its not already deleted
        (using the internal is_deleted flag)

        Args:
            wait (bool): Wait for object to be deleted
            force (bool): Force delete object

        Returns:
            bool: True if deleted, False otherwise

        """
        # Avoid accidental delete of default storageclass and secret
        if (
            self.name == constants.DEFAULT_STORAGECLASS_CEPHFS
            or self.name == constants.DEFAULT_STORAGECLASS_RBD
        ):
            log.info("Attempt to delete default Secret or StorageClass")
            return

        if self._is_deleted:
            log.info(
                f"Attempt to remove resource: {self.name} which is"
                f"already deleted! Skipping delete of this resource!"
            )
            result = True
        else:
            result = self.ocp.delete(
                resource_name=self.name, wait=wait, force=force
            )
            self._is_deleted = True
        return result

    def apply(self, **data):
        with open(self.temp_yaml, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml), (
            f"Failed to apply changes {data}"
        )
        self.reload()

    def add_label(self, label):
        """
        Addss a new label

        Args:
            label (str): New label to be assigned for this pod
                E.g: "label=app='rook-ceph-mds'"
        """
        status = self.ocp.add_label(resource_name=self.name, label=label)
        self.reload()
        return status

    def delete_temp_yaml_file(self):
        utils.delete_file(self.temp_yaml)

    def __getstate__(self):
        """
        unset attributes for serializing the object
        """
        self_dict = self.__dict__
        del self.temp_yaml
        return self_dict

    def __setstate__(self, d):
        """
        reset attributes for serializing the object
        """
        self.temp_yaml = None
        self.__dict__.update(d)


def get_version_info(namespace=None):
    operator_selector = get_selector_for_ocs_operator()
    package_manifest = PackageManifest(
        resource_name=defaults.OCS_OPERATOR_NAME, selector=operator_selector,
    )
    channel = config.DEPLOYMENT.get('ocs_csv_channel')
    csv_name = package_manifest.get_current_csv(channel)
    csv_pre = CSV(
        resource_name=csv_name,
        namespace=namespace
    )
    info = get_images(csv_pre.get())
    return info


def get_job_obj(name, namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Get the job instance for the given job name

    Args:
        name (str): The name of the job
        namespace (str): The namespace to look in

    Returns:
        OCS: A job OCS instance
    """
    ocp_obj = OCP(kind=constants.JOB, namespace=namespace)
    ocp_dict = ocp_obj.get(resource_name=name)
    return OCS(**ocp_dict)


def get_ocs_csv():
    """
    Get the OCS CSV object

    Returns:
        CSV: OCS CSV object

    """
    namespace = config.ENV_DATA['cluster_namespace']
    operator_selector = get_selector_for_ocs_operator()
    ocs_package_manifest = PackageManifest(
        resource_name=defaults.OCS_OPERATOR_NAME, selector=operator_selector,
    )
    channel = config.DEPLOYMENT.get('ocs_csv_channel')
    ocs_csv_name = ocs_package_manifest.get_current_csv(channel=channel)
    ocs_csv = CSV(
        resource_name=ocs_csv_name, namespace=namespace
    )
    log.info(f"Check if OCS operator: {ocs_csv_name} is in Succeeded phase.")
    ocs_csv.wait_for_phase(phase="Succeeded", timeout=600)
    return ocs_csv


def check_if_cluster_was_upgraded():
    """
    Check whether the OCS cluster went through upgrade

    Returns:
        bool: True if the OCS cluster went through upgrade, False otherwise

    """
    return True if 'replaces' in get_ocs_csv().get().get('spec') else False
