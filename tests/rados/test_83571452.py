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

def run(**kw):
    """
    CEPH-83571452 RADOS:
    Corrupt snap info of an object and run
    list-inconsistent-snapset
    Steps:
        1. create a replica 3 pool
	2. take few pool snaps with writes on objects b/w every snap
	3. chose NON-PRIMARY osd and bring it down
	4. go to backend and using ceph-object-store tool corrupt the snapset of the object
	5. run deep-scrub on the pg
	6. check rados list-inconsistent-pg <pool>
	7. rados list-inconsistent-snapset <pg>
    """
    log.info("Running CEPH-83571452")
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

    """create a replica pool"""
    pname="snapcorrupt_{rand}".format(rand=random.randint(0,10000))
    try:
        Helper.create_pool(pname, 1)
        log.info("Pool {pname} created".format(pname=pname))
    except Exception as E:
        log.error("Failed to create pool")
        log.error(traceback.format_exc())
        return 1
    time.sleep(5)

    """Get the target PG,osd for corruption operation"""
    oname="UNIQUEOBJECT{i}".format(i=random.randint(0,10000))
    cmd="osd map {pname} {obj} --format json".format(pname=pname, obj=oname)
    (out, err)=Helper.raw_cluster_cmd(cmd)
    outbuf=out.read()
    log.info(outbuf)
    cmdout=json.loads(outbuf)
    targt_pg=cmdout['pgid']
    targt_osd=cmdout['up'][random.randint(1,2)]
    '''write data and take snaps'''
    putobj="sudo rados -p {pool} put {obj} {path}".format(
            pool=pname, obj=oname, path="/etc/hosts"
            )
    for i in range(10):
        (out,err)=ctrlr.exec_command(cmd=putobj)
        snapcmd="sudo rados mksnap -p {pool} {sname}".format(
                 pool=pname, sname="snap"+str(i)
                 )
        (out,err)=ctrlr.exec_command(cmd=snapcmd)
        log.info("put {obj}, snap {snap}".format(
                    obj=oname, snap="snap"+str(i)
            ))
    '''
    Goto destination osd, stop the osd
    use ceph-objectstore-tool to corrupt
    snap info
    '''
    ctx=Helper.get_osd_obj(targt_osd, osds)
    Helper.kill_osd(targt_osd, "SIGTERM", osds)
    time.sleep(10)
    slist_cmd="sudo ceph-objectstore-tool --data-path \
            /var/lib/ceph/osd/ceph-{id} --journal-path \
            /var/lib/ceph/osd/ceph-{id}/journal \
            --head --op list {obj}".format(id=targt_osd,
                    obj=oname)
    (out, err)=ctx.exec_command(cmd=slist_cmd)
    outbuf=out.read()
    log.info(outbuf)

    corrupt_cmd="sudo ceph-objectstore-tool --data-path \
            /var/lib/ceph/osd/ceph-{id} --journal-path \
            /var/lib/ceph/osd/ceph-{id}/journal \
            {outbuf} clear-snapset corrupt".format(id=targt_osd,outbuf="'"+(outbuf)+"'")
    (out, err)=ctx.exec_command(cmd=corrupt_cmd)
    outbuf=out.read()
    log.info(outbuf)

    Helper.revive_osd(targt_osd, osds)
    time.sleep(10)
    run_scrub="pg deep-scrub {pgid}".format(pgid=targt_pg)
    (out, err)=Helper.raw_cluster_cmd(run_scrub)
    outbuf=out.read()
    log.info(outbuf)

    while 'HEALTH_ERR' and 'active+clean+inconsistent' not in outbuf:
        status="-s --format json"
        (out, err)=Helper.raw_cluster_cmd(status)
        outbuf=out.read()
    log.info("HEALTH_ERR found as expected")
    log.info("inconsistent foud as expected")

    timeout=10
    found=0
    while timeout:
        incon_pg="sudo rados list-inconsistent-pg {pname}".format(pname=pname)
        (out, err)=ctrlr.exec_command(cmd=incon_pg)
        outbuf=out.read()
        log.info(outbuf)
        if targt_pg not in outbuf:
            time.sleep(1)
            timeout=timeout-1
        else:
            found=1
            break
    if timeout==0 and found==0:
            log.error("pg not listed as inconsistent")
            return 1

    timeout=10
    found=0
    while timeout:
        incon_snap="sudo rados list-inconsistent-snapset {pg}".format(pg=targt_pg)
        (out, err)=ctrlr.exec_command(cmd=incon_snap)
        outbuf=out.read()
        log.info(outbuf)
        if oname not in outbuf:
            time.sleep(1)
            timeout=timeout-1
        else:
            found=1
            break
    if timeout==0 and found==0:
        log.error("object is not listed in inconsistent snap")
        return 1

    return 0
