import logging
import time

from ceph.parallel import parallel
from ceph.utils import hard_reboot

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Starting CEPH-9474")
    mirror1 = kw.get('test_data')['mirror1']
    mirror2 = kw.get('test_data')['mirror2']
    config = kw.get('config')
    osd_cred = config.get('osp_cred')
    poolname = mirror1.random_string() + '9474pool'
    imagename = mirror1.random_string() + '9474image'
    imagespec = poolname + '/' + imagename

    mirror1.create_pool(poolname=poolname)
    mirror2.create_pool(poolname=poolname)
    mirror1.create_image(imagespec=imagespec, size=config.get('imagesize'))
    mirror1.config_mirror(mirror2, poolname=poolname, mode='pool')
    mirror2.wait_for_status(poolname=poolname, images_pattern=1)
    mirror1.wait_for_status(imagespec=imagespec, state_pattern='up+stopped')
    mirror2.wait_for_status(imagespec=imagespec, state_pattern='up+replaying')

    with parallel() as p:
        p.spawn(mirror1.benchwrite, imagespec=imagespec,
                io=config.get('io-total'))
        p.spawn(hard_reboot, osd_cred, name='ceph-rbd2')
    time.sleep(10)
    rc = mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
    if rc == 0:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0
    else:
        return 1
