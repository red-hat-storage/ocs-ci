from ceph.utils import hard_reboot
import time
import logging
log = logging.getLogger(__name__)


def run(**kw):
    log.info("Starting CEPH-9470")
    mirror1 = kw.get('test_data')['mirror1']
    mirror2 = kw.get('test_data')['mirror2']
    config = kw.get('config')
    osd_cred = config.get('osp_cred')
    poolname = mirror1.random_string() + '9470pool'
    imagename = mirror1.random_string() + '9470image'
    imagespec = poolname + '/' + imagename
    mirror1.create_pool(poolname=poolname)
    mirror2.create_pool(poolname=poolname)
    mirror1.create_image(imagespec=imagespec, size=config.get('imagesize'))
    mirror1.config_mirror(mirror2, poolname=poolname, mode='pool')
    mirror2.wait_for_status(poolname=poolname, images_pattern=1)
    mirror1.benchwrite(imagespec=imagespec, io=config.get('io-total'))
    mirror2.wait_for_replay_complete(imagespec=imagespec)
    mirror1.wait_for_status(imagespec=imagespec, state_pattern='up+unknown')
    mirror2.wait_for_status(imagespec=imagespec, state_pattern='up+unknown')
    hard_reboot(osd_cred, name='ceph-rbd1')
    mirror2.promote(imagespec=imagespec, force=True)
    mirror2.benchwrite(imagespec=imagespec, io=config.get('io-total'))
    time.sleep(50)
    mirror2.demote(imagespec=imagespec)
    mirror2.resync(imagespec=imagespec)
    time.sleep(1000)
    mirror2.wait_for_status(imagespec=imagespec, state_pattern='up+unknown')
    mirror1.wait_for_status(imagespec=imagespec, state_pattern='up+unknown')
    mirror1.benchwrite(imagespec=imagespec, io=config.get('io-total'))
    time.sleep(10)
    rc = mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
    if rc == 0:
        mirror1.delete_pool(poolname=poolname)
        mirror2.delete_pool(poolname=poolname)
        return 0
    else:
        return 1
