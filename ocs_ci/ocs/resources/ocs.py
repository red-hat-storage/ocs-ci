"""
General OCS object
"""
import logging
import yaml
import tempfile
from jsonschema import validate

from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP, get_images
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
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
        # Avoid accidental delete of default storageclass and secret
        if (
            self.name == constants.DEFAULT_STORAGECLASS_CEPHFS
            or self.name == constants.DEFAULT_STORAGECLASS_RBD
        ):
            log.info(f"Attempt to delete default Secret or StorageClass")
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


def get_version_info(namespace=None):
    operator_selector = get_selector_for_ocs_operator()
    package_manifest = PackageManifest(
        resource_name=defaults.OCS_OPERATOR_NAME, selector=operator_selector,
    )
    csv_name = package_manifest.get_current_csv()
    csv_pre = CSV(
        resource_name=csv_name,
        namespace=namespace
    )
    info = get_images(csv_pre.get())
    return info


def ocs_install_verification(timeout=600, skip_osd_distribution_check=False):
    """
    Perform steps necessary to verify a successful OCS installation

    Args:
        timeout (int): Number of seconds for timeout which will be used in the
            checks used in this function.
        skip_osd_distribution_check (bool): If true skip the check for osd
            distribution.

    """
    from ocs_ci.ocs.node import get_typed_nodes
    from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
    from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_all_pods
    number_of_worker_nodes = len(get_typed_nodes())
    namespace = config.ENV_DATA['cluster_namespace']
    log.info("Verifying OCS installation")

    # Verify OCS CSV is in Succeeded phase
    log.info("verifying ocs csv")
    operator_selector = get_selector_for_ocs_operator()
    ocs_package_manifest = PackageManifest(
        resource_name=defaults.OCS_OPERATOR_NAME, selector=operator_selector,
    )
    ocs_csv_name = ocs_package_manifest.get_current_csv()
    ocs_csv = CSV(
        resource_name=ocs_csv_name, namespace=namespace
    )
    log.info(f"Check if OCS operator: {ocs_csv_name} is in Succeeded phase.")
    ocs_csv.wait_for_phase(phase="Succeeded", timeout=timeout)

    # Verify OCS Cluster Service (ocs-storagecluster) is Ready
    storage_cluster_name = config.ENV_DATA['storage_cluster_name']
    log.info("Verifying status of storage cluster: %s", storage_cluster_name)
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=namespace,
    )
    log.info(
        f"Check if StorageCluster: {storage_cluster_name} is in"
        f"Succeeded phase"
    )
    storage_cluster.wait_for_phase(phase='Ready', timeout=timeout)

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
        resource_count=number_of_worker_nodes,
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
        resource_count=number_of_worker_nodes,
        timeout=timeout
    )
    # csi-rbdplugin-provisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # osds
    osd_count = (
        int(storage_cluster.data['spec']['storageDeviceSets'][0]['count'])
        * int(storage_cluster.data['spec']['storageDeviceSets'][0]['replica'])
    )
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=osd_count,
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

    # rgw check only for VmWare
    if config.ENV_DATA.get('platform') == constants.VSPHERE_PLATFORM:
        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.RGW_APP_LABEL,
            resource_count=1,
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
    if not skip_osd_distribution_check:
        log.info("Verifying OSD's are distributed evenly across worker nodes")
        ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
        osds = ocp_pod_obj.get(selector=constants.OSD_APP_LABEL)['items']
        node_names = [osd['spec']['nodeName'] for osd in osds]
        for node in node_names:
            assert not node_names.count(node) > 1, (
                "OSD's are not distributed evenly across worker nodes"
            )

    # Verify that CSI driver object contains provisioner names
    log.info("Verifying CSI driver object contains provisioner names.")
    csi_driver = OCP(kind="CSIDriver")
    assert {defaults.CEPHFS_PROVISIONER, defaults.RBD_PROVISIONER} == (
        {item['metadata']['name'] for item in csi_driver.get()['items']}
    )

    # Verify node and provisioner secret names in storage class
    log.info("Verifying node and provisioner secret names in storage class.")
    sc_rbd = storage_class.get(
        resource_name=constants.DEFAULT_STORAGECLASS_RBD
    )
    sc_cephfs = storage_class.get(
        resource_name=constants.DEFAULT_STORAGECLASS_CEPHFS
    )
    assert sc_rbd['parameters']['csi.storage.k8s.io/node-stage-secret-name'] == constants.RBD_NODE_SECRET
    assert sc_rbd['parameters']['csi.storage.k8s.io/provisioner-secret-name'] == constants.RBD_PROVISIONER_SECRET
    assert sc_cephfs['parameters']['csi.storage.k8s.io/node-stage-secret-name'] == constants.CEPHFS_NODE_SECRET
    assert sc_cephfs['parameters']['csi.storage.k8s.io/provisioner-secret-name'] == constants.CEPHFS_PROVISIONER_SECRET
    log.info("Verified node and provisioner secret names in storage class.")

    # Verify ceph osd tree output
    log.info(
        "Verifying ceph osd tree output and checking for device set PVC names "
        "in the output."
    )
    deviceset_pvcs = [pvc.name for pvc in get_deviceset_pvcs()]
    ct_pod = get_ceph_tools_pod()
    osd_tree = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd tree', format='json')
    schemas = {
        'root': constants.OSD_TREE_ROOT,
        'rack': constants.OSD_TREE_RACK,
        'host': constants.OSD_TREE_HOST,
        'osd': constants.OSD_TREE_OSD,
        'region': constants.OSD_TREE_REGION,
        'zone': constants.OSD_TREE_ZONE
    }
    schemas['host']['properties']['name'] = {'enum': deviceset_pvcs}
    for item in osd_tree['nodes']:
        validate(instance=item, schema=schemas[item['type']])
        if item['type'] == 'host':
            deviceset_pvcs.remove(item['name'])
    assert not deviceset_pvcs, (
        f"These device set PVCs are not given in ceph osd tree output "
        f"- {deviceset_pvcs}"
    )
    log.info(
        "Verified ceph osd tree output. Device set PVC names are given in the "
        "output."
    )

    # TODO: Verify ceph osd tree output have osd listed as ssd
    # TODO: Verify ceph osd tree output have zone or rack based on AZ

    # Verify CSI snapshotter sidecar container is not present
    log.info("Verifying CSI snapshotter is not present.")
    provisioner_pods = get_all_pods(
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        selector=[
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL
        ]
    )
    for pod_obj in provisioner_pods:
        pod_info = pod_obj.get()
        for container, image in get_images(data=pod_info).items():
            assert ('snapshot' not in container) and ('snapshot' not in image), (
                f"Snapshot container is present in {pod_obj.name} pod. "
                f"Container {container}. Image {image}"
            )
    assert {'name': 'CSI_ENABLE_SNAPSHOTTER', 'value': 'false'} in (
        ocs_csv.get()['spec']['install']['spec']['deployments'][0]['spec']['template']['spec']['containers'][0]['env']
    ), "CSI_ENABLE_SNAPSHOTTER value is not set to 'false'."
    log.info("Verified: CSI snapshotter is not present.")

    # Verify pool crush rule is with "type": "zone"
    if utils.get_az_count() == 3:
        log.info("Verifying pool crush rule is with type: zone")
        crush_dump = ct_pod.exec_ceph_cmd(
            ceph_cmd='ceph osd crush dump', format=''
        )
        pool_names = [
            constants.METADATA_POOL, constants.DEFAULT_BLOCKPOOL,
            constants.DATA_POOL
        ]
        crush_rules = [rule for rule in crush_dump['rules'] if rule['rule_name'] in pool_names]
        for crush_rule in crush_rules:
            assert [
                item for item in crush_rule['steps'] if item.get('type') == 'zone'
            ], f"{crush_rule['rule_name']} is not with type as zone"
        log.info("Verified - pool crush rule is with type: zone")
