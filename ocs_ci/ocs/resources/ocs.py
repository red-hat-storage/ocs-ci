"""
General OCS object
"""
import logging
import yaml
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility import utils
from ocs_ci.utility import templating

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
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix=self._kind, delete=False
        )
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

    def describe(self):
        return self.ocp.describe(resource_name=self.name)

    def create(self, do_reload=True):
        log.info(f"Adding {self.kind} with name {self.name}")
        templating.dump_data_to_temp_yaml(self.data, self.temp_yaml.name)
        status = self.ocp.create(yaml_file=self.temp_yaml.name)
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
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml.name), (
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
        utils.delete_file(self.temp_yaml.name)


def ocs_install_verification(timeout=0):
    """
    Perform steps necessary to verify a successful OCS installation

    Args:
        timeout (int): Number of seconds for timeout which will be used in the
            checks used in this function.

    """
    log.info("Verifying OCS installation")
    namespace = config.ENV_DATA['cluster_namespace']

    # Verify Local Storage CSV is in Succeeded phase
    log.info("Verifying Local Storage CSV")
    # There is BZ opened:
    # https://bugzilla.redhat.com/show_bug.cgi?id=1770183
    # which makes this check problematic as current CSV is not the currently
    # installed.
    local_storage_csvs = get_csvs_start_with_prefix(
        csv_prefix=constants.LOCAL_STORAGE_CSV_PREFIX,
        namespace=namespace,
    )
    assert len(local_storage_csvs) == 1, (
        f"There are more than one local storage CSVs: {local_storage_csvs}"
    )
    local_storage_name = local_storage_csvs[0]['metadata']['name']
    local_storage_csv = CSV(
        resource_name=local_storage_name, namespace=namespace
    )
    local_storage_csv.wait_for_phase("Succeeded", timeout=timeout)

    # Verify OCS CSV is in Succeeded phase
    log.info("verifying ocs csv")
    ocs_package_manifest = PackageManifest(
        resource_name=defaults.OCS_OPERATOR_NAME
    )
    ocs_csv_name = ocs_package_manifest.get_current_csv()
    ocs_csv = CSV(
        resource_name=ocs_csv_name, namespace=namespace
    )
    assert ocs_csv.check_phase(phase="Succeeded"), (
        "OCS CSV is not in Succeeded phase!"
    )

    # Verify OCS Cluster Service (ocs-storagecluster) is Ready
    log.info("Verifying OCS Cluster service")
    storage_cluster = OCP(kind='StorageCluster', namespace=namespace)
    storage_clusters = storage_cluster.get()
    for item in storage_clusters['items']:
        name = item['metadata']['name']
        log.info("Checking status of %s", name)
        assert item['status']['phase'] == 'Ready', (
            f"StorageCluster {name} not 'Ready'"
        )

    # Verify pods in running state and proper counts
    log.info("Verifying pod states and counts")
    pod = OCP(
        kind=constants.POD, namespace=namespace
    )
    # ocs-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OCS_OPERATOR_LABEL,
        timeout=timeout
    )
    # rook-ceph-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OPERATOR_LABEL,
        timeout=timeout
    )
    # noobaa
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.NOOBAA_APP_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # local-storage-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.LOCAL_STORAGE_OPERATOR_LABEL,
        timeout=timeout
    )
    # mons
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MON_APP_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-cephfsplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-cephfsplugin-provisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # csi-rbdplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-rbdplugin-profisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # osds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # mgr
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MGR_APP_LABEL,
        timeout=timeout
    )
    # mds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MDS_APP_LABEL,
        resource_count=2,
        timeout=timeout
    )

    # Verify ceph health
    log.info("Verifying ceph health")
    assert utils.ceph_health_check(namespace=namespace)

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    log.info("Verifying storage classes")
    storage_class = OCP(
        kind=constants.STORAGECLASS, namespace=namespace
    )
    storage_cluster_name = config.ENV_DATA['storage_cluster_name']
    required_storage_classes = {
        f'{storage_cluster_name}-cephfs',
        f'{storage_cluster_name}-ceph-rbd'
    }
    storage_classes = storage_class.get()
    storage_class_names = {
        item['metadata']['name'] for item in storage_classes['items']
    }
    assert required_storage_classes.issubset(storage_class_names)

    # Verify OSD's are distributed
    log.info("Verifying OSD's are distributed evenly across worker nodes")
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    osds = ocp_pod_obj.get(selector=constants.OSD_APP_LABEL)['items']
    node_names = [osd['spec']['nodeName'] for osd in osds]
    for node in node_names:
        assert not node_names.count(node) > 1, (
            "OSD's are not distributed evenly across worker nodes"
        )
