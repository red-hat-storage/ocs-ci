import logging
import traceback
import hashlib
import os
import time
import random
import json

from ceph.rados_utils import RadosHelper
from ceph.parallel import parallel

logger = logging.getLogger(__name__)
log=logger

def get_ms_type(osd, osds, Helper):
    '''check what's the default messenger'''
    tosd=Helper.get_osd_obj(osd, osds)
    probe_ms="sudo ceph --admin-daemon /var/run/ceph/ceph-osd.{oid}.asok config show --format json".format(
            oid=osd
            )
    (out,err)=tosd.exec_command(cmd=probe_ms)
    outbuf=out.read()
    log.info(outbuf)
    jconfig=json.loads(outbuf)
    return jconfig['ms_type']

def run(**kw):
    """
    CEPH-11538:
    Check for default messenger i.e. async messenger
    swith b/w simple and async messenger

    1. By default 3.x wil have async messenger, anything below
    will have simple messenger
    2. add ms_type = async for enabling async and check io
    3. add ms_type=simple for enabling simple and check io
    """
    log.info("Running CEPH-11538")
    log.info(run.__doc__)
    ceph_nodes=kw.get('ceph_nodes')
    config=kw.get('config')
    mons=[]
    osds=[]

    role='mon'
    for mnode in ceph_nodes:
        if mnode.role==role:
            mons.append(mnode)

    role='osd'
    for osd in ceph_nodes:
        if osd.role==role:
            osds.append(osd)

    ctrlr=mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format( \
                        cmon=ctrlr.hostname))
    Helper=RadosHelper(ctrlr, config, log)

    '''crete a pool for io'''
    pname="mscheck_{rand}".format(rand=random.randint(0,10000))
    Helper.create_pool(pname, 1)
    log.info("pool {pname} create".format(pname=pname))
    time.sleep(5)
    cmd="osd map {pname} {obj} --format json".format(
            pname=pname,
            obj="obj1"
            )
    (out,err)=Helper.raw_cluster_cmd(cmd)
    outbuf=out.read()
    log.info(outbuf)
    cmdout=json.loads(outbuf)
    targt_osd=cmdout['up'][0]


    '''check what's the default messenger'''
    mstype=get_ms_type(targt_osd, osds, Helper)
    if mstype!="async":
        log.error("default on luminous should be async but we have {mstype}".format(mstype=mstype))
        return 1

    '''switch to simple and do IO'''
    inject_osd="tell osd.* injectargs --ms_type simple"
    (out,err)=Helper.raw_cluster_cmd(inject_osd)
    log.info(out.read())

    time.sleep(4)
    '''check whether ms_type changed'''
    mstype=get_ms_type(targt_osd, osds, Helper)
    if "simple"==mstype:
        log.info("successfull changed to simple")
    else:
        log.error("failed to change the ms_type to simple")
        return 1

    '''change ms_type back to async'''
    inject_osd="tell osd.* injectargs --ms_type async"
    (out,err)=Helper.raw_cluster_cmd(inject_osd)
    log.info(out.read())
    time.sleep(4)
    '''check whether ms_type changed'''
    mstype=get_ms_type(targt_osd, osds, Helper)
    if "async"==mstype:
        log.info("successfull changed to async")
    else:
        log.error("failed to change the ms_type to async")
        return 1
    putobj="sudo rados -p {pool} put {obj} {path}".format(
            pool=pname, obj="obj1", path="/etc/hosts"
            )
    return 0
