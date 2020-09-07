import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import sync_object_directory, retrieve_test_objects_to_pod, \
    verify_s3_object_integrity, craft_s3_command, wait_for_pv_backingstore
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)
LOCAL_DIR_PATH = '/awsfiles'


class TestPvPool:
    """
    Test pv pool related operations
    """

    def test_write_to_full_bucket(self, bucket_factory, bucket_class_factory, mcg_obj_session,
                                  awscli_pod_session):
        bucketclass = bucket_class_factory({
            'interface': 'OC',
            'backingstore_dict': {
                'pv': [(1, 17, 'ocs-storagecluster-ceph-rbd')]
            }
        })
        bucket = bucket_factory(1, 'OC', bucketclass=bucketclass.name)[0]

        for i in range(1, 36):
            # add some data to the first pod
            awscli_pod_session.exec_cmd_on_pod(
                'dd if=/dev/urandom of=/tmp/testfile bs=1M count=500'
            )
            awscli_pod_session.exec_cmd_on_pod(
                craft_s3_command(
                    f"cp /tmp/testfile s3://{bucket.name}/testfile{i}",
                    mcg_obj_session
                ),
                out_yaml_format=False,
                secrets=[
                    mcg_obj_session.access_key_id,
                    mcg_obj_session.access_key,
                    mcg_obj_session.s3_endpoint
                ]
            )
            awscli_pod_session.exec_cmd_on_pod(
                'rm -f /tmp/testfile'
            )

    def test_pv_scale_out(self, backingstore_factory):
        pv_backingstore = backingstore_factory(
            'OC', {'pv': [(1, 17, 'ocs-storagecluster-ceph-rbd')]}
        )[0]

        logger.info(f'Scaling out PV Pool {pv_backingstore.name}')
        pv_backingstore.vol_num += 1
        edit_pv_backingstore = OCP(kind='BackingStore', namespace=config.ENV_DATA['cluster_namespace'])
        params = f'{{"spec":{{"pvPool":{{"numVolumes":{pv_backingstore.vol_num}}}}}}}'
        edit_pv_backingstore.patch(resource_name=pv_backingstore.name, params=params, format_type='merge')
        logger.info('Waiting for backingstore to return to OPTIMAL state')
        wait_for_pv_backingstore(pv_backingstore.name, config.ENV_DATA['cluster_namespace'])

        logger.info('Check if PV Pool scale out was successful')
        backingstore_dict = edit_pv_backingstore.get(pv_backingstore.name)
        assert backingstore_dict['spec']['pvPool']['numVolumes'] == pv_backingstore.vol_num, 'Scale out PV Pool failed. '
        logger.info('Scale out was successful')
