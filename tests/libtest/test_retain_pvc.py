from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs import constants, resources


@libtest
def test_main(teardown_factory, pvc_factory):
    sc = resources.ocs.OCS(
        kind=constants.STORAGECLASS,
        metadata={'name': 'ocs-storagecluster-ceph-rbd'}
    )
    sc.reload()
    sc.data['reclaimPolicy'] = constants.RECLAIM_POLICY_RETAIN
    sc.data['metadata']['name'] += '-retain'
    sc._name = sc.data['metadata']['name']
    sc.create()
    teardown_factory(sc)
    pvc_factory(storageclass=sc)
