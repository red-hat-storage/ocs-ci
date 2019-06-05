import logging

from tests import helpers
from ocs import defaults
logger = logging.getLogger(__name__)


class Test1:
    def test_1(self):
        ceph_fs_dict = defaults.CEPHFILESYSTEM_DICT.copy()
        fs_name = helpers.create_unique_resource_name('test', 'fs')
        ceph_fs_dict['metadata']['name'] = fs_name
        fs_obj = helpers.create_resource(**ceph_fs_dict, wait=False)
