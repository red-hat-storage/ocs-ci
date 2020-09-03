import logging

from ocs_ci.framework.pytest_customization.marks import (
    tier2, tier3
)

logger = logging.getLogger(__name__)

class TestPvPool:
    """
    Test pv pool related operations
    """
    def test_write_to_full_bucket(self, bucket_factory, bucket_class_factory):
        bucketclass = bucket_class_factory({
                    'interface': 'OC',
                    'backingstores': {
                        'pv': [(1, 17, 'ocs-storagecluster-ceph-rbd')]
                    }
        })
        bucket_factory(1, 'OC', bucketclass=bucketclass)

