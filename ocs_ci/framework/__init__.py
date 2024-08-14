"""
Avoid already-imported warning cause of we are importing this package from
run wrapper for loading config.

You can see documentation here:
https://docs.pytest.org/en/latest/reference.html
under section PYTEST_DONT_REWRITE
"""

# Use the new python 3.7 dataclass decorator, which provides an object similar
# to a namedtuple, but allows type enforcement and defining methods.
import os
import yaml
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from ocs_ci.ocs.exceptions import ClusterNotFoundException

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(THIS_DIR, "conf/default_config.yaml")

logger = logging.getLogger(__name__)


@dataclass
class Config:
    AUTH: dict = field(default_factory=dict)
    DEPLOYMENT: dict = field(default_factory=dict)
    ENV_DATA: dict = field(default_factory=dict)
    EXTERNAL_MODE: dict = field(default_factory=dict)
    REPORTING: dict = field(default_factory=dict)
    RUN: dict = field(default_factory=dict)
    UPGRADE: dict = field(default_factory=dict)
    FLEXY: dict = field(default_factory=dict)
    UI_SELENIUM: dict = field(default_factory=dict)
    PERF: dict = field(default_factory=dict)
    COMPONENTS: dict = field(default_factory=dict)
    # Used for multicluster only
    MULTICLUSTER: dict = field(default_factory=dict)

    def __post_init__(self):
        self.reset()

    def reset(self):
        """
        Clear all configuration data and load defaults
        """
        for f in fields(self):
            setattr(self, f.name, f.default_factory())
        self.update(self.get_defaults())

    def get_defaults(self):
        """
        Return a fresh copy of the default configuration
        """
        with open(DEFAULT_CONFIG_PATH) as file_stream:
            return {
                k: (v if v is not None else {})
                for (k, v) in yaml.safe_load(file_stream).items()
            }

    def update(self, user_dict: dict):
        """
        Override configuration items with items in user_dict, without wiping
        out non-overridden items
        """
        field_names = [f.name for f in fields(self)]
        if user_dict is None:
            return
        for k, v in user_dict.items():
            if k not in field_names:
                raise ValueError(
                    f"{k} is not a valid config section. "
                    f"Valid sections: {field_names}"
                )
            if v is None:
                continue
            section = getattr(self, k)
            merge_dict(section, v)

    def to_dict(self):
        # We don't use dataclasses.asdict() here, because that function appears
        # to create copies of fields - meaning changes to the return value of
        # this method will not be reflected in the field themselves.
        field_names = [f.name for f in fields(self)]
        return {name: getattr(self, name) for name in field_names}


def merge_dict(orig: dict, new: dict) -> dict:
    """
    Update a dict recursively, with values from 'new' being merged into 'orig'.

    Args:
        orig (dict): The object that will receive the update
        new  (dict): The object which is the source of the update

    Example::

            orig = {
                'dict': {'one': 1, 'two': 2},
                'list': [1, 2],
                'string': 's',
            }
            new = {
                'dict': {'one': 'one', 'three': 3},
                'list': [0],
                'string': 'x',
            }
            merge_dict(orig, new) ->
            {
                'dict': {'one': 'one', 'two': 2, 'three': 3}
                'list': [0],
                'string', 'x',
            }

    """
    for k, v in new.items():
        if isinstance(orig, Mapping):
            if isinstance(v, Mapping):
                r = merge_dict(orig.get(k, dict()), v)
                orig[k] = r
            else:
                orig[k] = v
        else:
            orig = {k: v}
    return orig


class MultiClusterConfig:
    # This class wraps Config() objects so that we can handle
    # multiple cluster contexts
    def __init__(self):
        # Holds all cluster's Config() object
        self.clusters = list()
        # This member always points to current cluster's Config() object
        self.cluster_ctx = None
        self.nclusters = 1
        # Index for current cluster in context
        self.cur_index = 0
        self.multicluster = False
        # A list of lists which holds CLI args clusterwise
        self.multicluster_args = list()
        self.multicluster_common_args = list()
        # Points to cluster config objects which holds ACM cluster conf
        # Applicable only if we are deploying ACM cluster
        self.acm_index = None
        self.single_cluster_default = True
        self._single_cluster_init_cluster_configs()

    @property
    def default_cluster_ctx(self):
        """
        Get the default cluster context.
        The default cluster context will be defined by the default index as defined in the
        'ENV DATA' param 'default_cluster_context_index'

        Returns:
            ocs_ci.framework.Config: The default cluster context

        """
        # Get the default index. If not found, the default value is 0
        default_index = self.cluster_ctx.ENV_DATA.get(
            "default_cluster_context_index", 0
        )
        return self.clusters[default_index]

    def _single_cluster_init_cluster_configs(self):
        self.clusters.insert(0, Config())
        self.cluster_ctx = self.clusters[0]
        self.attr_init()
        self._refresh_ctx()

    def init_cluster_configs(self):
        if self.nclusters > 1:
            # reset if any single cluster object is present from init
            self.clusters.clear()
            for i in range(self.nclusters):
                self.clusters.insert(i, Config())
                self.clusters[i].MULTICLUSTER["multicluster_index"] = i
            self.cluster_ctx = self.clusters[0]
            self.attr_init()
            self._refresh_ctx()
            self.single_cluster_default = False

    def attr_init(self):
        self.attr_list = [attr for attr in self.cluster_ctx.__dataclass_fields__.keys()]

    def update(self, user_dict):
        self.cluster_ctx.update(user_dict)
        self._refresh_ctx()

    def reset(self):
        self.cluster_ctx.reset()
        self._refresh_ctx()

    def get_defaults(self):
        return self.cluster_ctx.get_defaults()

    def reset_ctx(self):
        self.cluster_ctx = self.clusters[0]
        self._refresh_ctx()

    def _refresh_ctx(self):
        [
            self.__setattr__(attr, self.cluster_ctx.__getattribute__(attr))
            for attr in self.attr_list
        ]
        self.to_dict = self.cluster_ctx.to_dict
        if self.RUN.get("kubeconfig"):
            os.environ["KUBECONFIG"] = self.RUN.get("kubeconfig")

    def switch_ctx(self, index=0):
        self.cluster_ctx = self.clusters[index]
        self.cur_index = index
        self._refresh_ctx()
        # Log the switch after changing the current index
        logger.info(f"Switched to cluster: {self.current_cluster_name()}")

    def switch_acm_ctx(self):
        self.switch_ctx(self.get_active_acm_index())

    def get_active_acm_index(self):
        """
        Retrieve the active ACM cluster index.

        Returns:
            int: The multicluster_index of the active ACM cluster config.

        """
        for cluster in self.clusters:
            if cluster.MULTICLUSTER["active_acm_cluster"]:
                return cluster.MULTICLUSTER["multicluster_index"]
        # if no active cluster is found, designate one
        return self.designate_active_acm_cluster()

    def designate_active_acm_cluster(self):
        """
        Set one of the ACM clusters as the active ACM cluster. This is done in
        the event that none of the ACM clusters are set as active.

        Returns:
            int: The multicluster index of the newly designated active ACM cluster

        """
        for cluster in self.clusters:
            if cluster.MULTICLUSTER["acm_cluster"]:
                cluster.MULTICLUSTER["active_acm_cluster"] = True
                return cluster.MULTICLUSTER["multicluster_index"]

    def switch_default_cluster_ctx(self):
        # We can check any conf for default_cluster_context_index
        # because its a common arg
        self.switch_ctx(self.cluster_ctx.ENV_DATA["default_cluster_context_index"])

    def get_provider_index(self):
        """
        Get the provider cluster index

        Returns:
            int: the provider cluster index

        Raises:
            ClusterNotFoundException: In case it didn't find the provider cluster

        """
        for i, cluster in enumerate(self.clusters):
            if cluster.ENV_DATA["cluster_type"] == "provider":
                return i

        raise ClusterNotFoundException("Didn't find the provider cluster")

    def get_consumer_indexes_list(self):
        """
        Get the consumer cluster indexes

        Returns:
            list: the consumer cluster indexes

        Raises:
            ClusterNotFoundException: In case it didn't find any consumer cluster

        """
        consumer_indexes_list = []
        for i, cluster in enumerate(self.clusters):
            if cluster.ENV_DATA["cluster_type"] in ["consumer", "hci_client"]:
                consumer_indexes_list.append(i)

        if not consumer_indexes_list:
            raise ClusterNotFoundException("Didn't find any consumer cluster")

        return consumer_indexes_list

    def get_cluster_index_by_name(self, cluster_name):
        """
        Get the cluster index by the cluster name

        Returns:
            int: The cluster index by the cluster name

        Raises:
            ClusterNotFoundException: In case it didn't find the cluster

        """
        for i, cluster in enumerate(self.clusters):
            if cluster.ENV_DATA["cluster_name"] == cluster_name:
                return i

        raise ClusterNotFoundException(f"Didn't find the cluster '{cluster_name}' ")

    def switch_to_provider(self):
        """
        Switch to the provider cluster

        Raises:
            ClusterNotFoundException: In case it didn't find the provider cluster

        """
        self.switch_ctx(self.get_provider_index())

    def switch_to_consumer(self, num_of_consumer=0):
        """
        Switch to one of the consumer clusters

        Args:
             num_of_consumer (int): The cluster index to switch to. The default consumer number
                is 0 - which means it will switch to the first consumer.
                1 - is the second, 2 - is the third, and so on.
        Raises:
            ClusterNotFoundException: In case it didn't find the consumer cluster

        """
        self.switch_ctx(self.get_consumer_indexes_list()[num_of_consumer])

    def switch_to_cluster_by_name(self, cluster_name):
        """
        Switch to the cluster by the cluster name

        Args:
            cluster_name (str): The cluster name to switch to

        Raises:
            ClusterNotFoundException: In case it didn't find the cluster

        """
        self.switch_ctx(self.get_cluster_index_by_name(cluster_name))

    def current_cluster_name(self):
        """
        Get the Cluster name of the current context

        Returns:
            str: The cluster name which is stored as str in config (None if key not exist)

        """
        return self.ENV_DATA.get("cluster_name")

    def is_provider_exist(self):
        """
        Check if the provider cluster exists in the clusters

        Returns:
            bool: True, if the provider cluster exists in the clusters. False, otherwise.

        """
        cluster_types = [cluster.ENV_DATA["cluster_type"] for cluster in self.clusters]
        return "provider" in cluster_types

    def is_consumer_exist(self):
        """
        Check if the consumer cluster exists in the clusters

        Returns:
            bool: True, if the consumer cluster exists in the clusters. False, otherwise.

        """
        cluster_types = [cluster.ENV_DATA["cluster_type"] for cluster in self.clusters]
        return "consumer" in cluster_types

    def hci_client_exist(self):
        """
        Check if the hci_client cluster exists in the clusters

        Returns:
            bool: True, if the hci_client cluster exists in the clusters. False, otherwise.

        """
        cluster_types = [cluster.ENV_DATA["cluster_type"] for cluster in self.clusters]
        return "hci_client" in cluster_types

    def hci_provider_exist(self):
        """
        Check if the provider cluster exists in the clusters

        Returns:
            bool: True, if the provider cluster exists in the clusters. False, otherwise.

        """
        cluster_types = [cluster.ENV_DATA["cluster_type"] for cluster in self.clusters]
        return "provider" in cluster_types

    def is_cluster_type_exist(self, cluster_type):
        """
        Check if the given cluster type exists in the clusters

        Args:
            cluster_type (str): The cluster type

        Returns:
            bool: True, if the given cluster type exists in the clusters. False, otherwise.

        """
        cluster_types = [cluster.ENV_DATA["cluster_type"] for cluster in self.clusters]
        return cluster_type in cluster_types

    def get_cluster_type_indices_list(self, cluster_type):
        """
        Get the cluster type indices

        Returns:
            list: the cluster type indices

        Raises:
            ClusterNotFoundException: In case it didn't find any cluster with the cluster type

        """
        cluster_type_indices_list = []
        for i, cluster in enumerate(self.clusters):
            if cluster.ENV_DATA["cluster_type"] == cluster_type:
                cluster_type_indices_list.append(i)

        if not cluster_type_indices_list:
            raise ClusterNotFoundException(
                f"Didn't find any cluster with the cluster type '{cluster_type}'"
            )

        return cluster_type_indices_list

    def switch_to_cluster_by_cluster_type(self, cluster_type, num_of_cluster=0):
        """
        Switch to the cluster with the given cluster type

        Args:
            cluster_type (str): The cluster type
            num_of_cluster (int): The cluster index to switch to. The default cluster number
                is 0 - which means it will switch to the first cluster.
                1 - is the second, 2 - is the third, and so on.
        Raises:
            ClusterNotFoundException: In case it didn't find any cluster with the cluster type

        """
        self.switch_ctx(
            self.get_cluster_type_indices_list(cluster_type)[num_of_cluster]
        )

    class RunWithConfigContext(object):
        def __init__(self, config_index):
            self.original_config_index = config.cur_index
            self.config_index = config_index

        def __enter__(self):
            config.switch_ctx(self.config_index)
            return self

        def __exit__(self, exc_type, exc_value, exc_traceback):
            config.switch_ctx(self.original_config_index)

    class RunWithAcmConfigContext(RunWithConfigContext):
        def __init__(self):
            from ocs_ci.ocs.utils import get_all_acm_indexes

            acm_index = get_all_acm_indexes()[0]
            super().__init__(acm_index)

    class RunWithPrimaryConfigContext(RunWithConfigContext):
        def __init__(self):
            from ocs_ci.ocs.utils import get_primary_cluster_config

            primary_config = get_primary_cluster_config()
            primary_index = primary_config.MULTICLUSTER.get("multicluster_index")
            super().__init__(primary_index)

    def insert_cluster_config(self, index, new_config):
        """
        Insert a new cluster configuration at the given index

        Args:
            index (int): The index at which to insert the new configuration
            new_config (Config): The new configuration to insert

        """
        self.clusters.insert(index, new_config)
        self.nclusters += 1
        self._refresh_ctx()

    def remove_cluster(self, index):
        """
        Remove the cluster at the given index

        Args:
            index (int): The index of the cluster to remove
        """
        self.clusters.pop(index)
        self.nclusters -= 1
        self._refresh_ctx()

    def remove_cluster_by_name(self, cluster_name):
        """
        Remove the cluster by the cluster name

        Args:
            cluster_name (str): The cluster name to remove

        Raises:
            ClusterNotFoundException: In case it didn't find the cluster

        """
        self.remove_cluster(self.get_cluster_index_by_name(cluster_name))


config = MultiClusterConfig()


class GlobalVariables:
    # Test time report
    TIMEREPORT_DICT: dict = dict()
