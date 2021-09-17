"""
Avoid already-imported warning cause of we are importing this package from
run wrapper for loading config.

You can see documentation here:
https://docs.pytest.org/en/latest/reference.html
under section PYTEST_DONT_REWRITE
"""
# Use the new python 3.7 dataclass decorator, which provides an object similar
# to a namedtuple, but allows type enforcement and defining methods.
import collections
import os
import yaml
import logging
from dataclasses import dataclass, field, fields

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
        if isinstance(orig, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = merge_dict(orig.get(k, dict()), v)
                orig[k] = r
            else:
                orig[k] = v
        else:
            orig = {k: v}
    return orig


class MultiClusterConfig():
    def __init__(self):
        self.clusters = list()
        self.cluster_ctx = None
        self.nclusters = 1
        # Index for current cluster in context
        self.cur_index = 0
        self.multicluster = False

    def initclusterconfigs(self):
        for i in range(self.nclusters):
            self.clusters.append(Config())
        self.cluster_ctx = self.clusters[0]
        self._refresh_ctx()

    def _refresh_ctx(self):
        self.AUTH = self.cluster_ctx.AUTH
        self.ENV_DATA = self.cluster_ctx.ENV_DATA
        self.DEPLOYMENT = self.cluster_ctx.DEPLOYMENT
        self.EXTERNAL_MODE = self.cluster_ctx.EXTERNAL_MODE
        self.REPORTING = self.cluster_ctx.REPORTING
        self.RUN = self.cluster_ctx.RUN
        self.UPGRADE = self.cluster_ctx.UPGRADE
        self.FLEXY = self.cluster_ctx.FLEXY
        self.UI_SELENIUM = self.cluster_ctx.UI_SELENIUM
        self.PERF = self.cluster_ctx.PERF
        self.COMPONENTS = self.cluster_ctx.COMPONENTS
        self.to_dict = self.cluster_ctx.to_dict
        self.update = self.cluster_ctx.update
        if self.RUN.get("kubeconfig"):
            logger.info("SWITCHING KUBECONFIG")
            os.environ["KUBECONFIG"] = self.RUN.get("kubeconfig")

    def switch_ctx(self, index):
        self.cluster_ctx = self.clusters[index]
        self.cur_index = index
        self._refresh_ctx()


config = MultiClusterConfig()
