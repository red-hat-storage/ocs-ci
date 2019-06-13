"""
General OCS object
"""
import logging
import yaml
import tempfile
from ocs.ocp import OCP
from utility import utils
from utility import templating

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
                obj_dict = load_yaml_to_dict(
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

    def create(self):
        log.info(f"Adding {self.kind} with name {self.name}")
        templating.dump_dict_to_temp_yaml(self.data, self.temp_yaml.name)
        status = self.ocp.create(yaml_file=self.temp_yaml.name)
        self.reload()
        return status

    def delete(self, wait=True):
        return self.ocp.delete(resource_name=self.name, wait=wait)

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
