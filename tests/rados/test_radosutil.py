import logging
from ceph.rados_utils import RadosHelper
import traceback

logger = logging.getLogger(__name__)
log = logger

def run(**kw):
	log.info("Running radoslib test")  
	ceph_nodes = kw.get('ceph_nodes')
	config = kw.get('config')

	mons = []
	osds = [] 
	role = 'mon'
	for mnode in ceph_nodes:
		if mnode.role == role:
			mons.append(mnode)
	for osd in ceph_nodes:
		if osd.role == 'osd':
			osds.append(osd)

	idx = 0
	mon = mons[idx]
	print mon.hostname

	Helper = RadosHelper(mon, config,
			    log)	

	"""	try:
		Helper.create_pool("blabla1",4)
		log.info("poll created successfully")
	except:
		log.error("pool creation failed")
		return 1
	
	try:
		pri_osd=Helper.get_pg_primary("new", 0)
		print pri_osd	
	except:
		return 1

	try:
		osdhost=Helper.get_osd_host(0)
		print osdhost 
	except:
		log.error("getting osd host failed")
		return 1
	
	ret=1
	try:
		log.info("TRYING KILL")
		ret=Helper.kill_osd(1, "SIGTERM", osds)
		log.info("ret={ret}".format(ret=ret))
	finally:
		return ret	

	try:
		ret=Helper.is_up(1)
		if ret:
			log.info("UP")
		else:
			log.info("DOWN")
		return ret
	except:
		log.error("staus check failed")
		return 1
	"""
	
	try:
		ret=Helper.revive_osd(1, osds)
		return ret
	except Exception as e:
		log.error("revive failed")
		log.error(traceback.format_exc())
		return 1
