import json
import logging
import random
import time
import traceback

logger = logging.getLogger(__name__)


class RadosHelper:
    def __init__(self, mon, config=None, log=None,
                 cluster='ceph'):
        self.mon = mon
        self.config = config
        if log:
            self.log = lambda x: log.info(x)
        self.num_pools = self.get_num_pools()
        self.cluster = cluster
        pools = self.list_pools()
        self.pools = {}
        for pool in pools:
            self.pools[pool] = self.get_pool_property(pool, 'pg_num')

    def raw_cluster_cmd(self, *args):
        """
        :return: (stdout, stderr)
        """
        ceph_args = [
            'sudo',
            'ceph',
            '--cluster',
            self.cluster,
        ]

        ceph_args.extend(args)
        print(ceph_args)
        clstr_cmd = " ".join(str(x) for x in ceph_args)
        print(clstr_cmd)
        (stdout, stderr) = self.mon.exec_command(cmd=clstr_cmd)
        return stdout, stderr

    def get_num_pools(self):
        """
        :returns: number of pools in the
                cluster
           """
        '''TODO'''

    def get_osd_dump_json(self):
        """
        osd dump --format=json converted to a python object
        :returns: the python object
        """
        (out, err) = self.raw_cluster_cmd('osd', 'dump', '--format=json')
        print(type(out))
        outbuf = out.read().decode()
        return json.loads('\n'.join(outbuf.split('\n')[1:]))

    def create_pool(self, pool_name, pg_num=16,
                    erasure_code_profile_name=None,
                    min_size=None,
                    erasure_code_use_overwrites=False):
        """
        Create a pool named from the pool_name parameter.
        :param pool_name: name of the pool being created.
        :param pg_num: initial number of pgs.
        :param erasure_code_profile_name: if set and !None create an
            erasure coded pool using the profile
        :param erasure_code_use_overwrites: if true, allow overwrites
        """
        assert isinstance(pool_name, str)
        assert isinstance(pg_num, int)
        assert pool_name not in self.pools
        self.log("creating pool_name %s" % (pool_name,))
        if erasure_code_profile_name:
            self.raw_cluster_cmd('osd', 'pool', 'create',
                                 pool_name, str(pg_num), str(pg_num),
                                 'erasure', erasure_code_profile_name)
        else:
            self.raw_cluster_cmd('osd', 'pool', 'create',
                                 pool_name, str(pg_num))
        if min_size is not None:
            self.raw_cluster_cmd(
                'osd', 'pool', 'set', pool_name,
                'min_size',
                str(min_size))
        if erasure_code_use_overwrites:
            self.raw_cluster_cmd(
                'osd', 'pool', 'set', pool_name,
                'allow_ec_overwrites',
                'true')
        self.raw_cluster_cmd(
            'osd', 'pool', 'application', 'enable',
            pool_name, 'rados', '--yes-i-really-mean-it',
        )
        self.pools[pool_name] = pg_num
        time.sleep(1)

    def list_pools(self):
        """
        list all pool names
        """
        osd_dump = self.get_osd_dump_json()
        self.log(osd_dump['pools'])
        return [str(i['pool_name']) for i in osd_dump['pools']]

    def get_pool_property(self, pool_name, prop):
        """
        :param pool_name: pool
        :param prop: property to be checked.
        :returns: property as an int value.
        """
        assert isinstance(pool_name, str)
        assert isinstance(prop, str)
        (output, err) = self.raw_cluster_cmd(
            'osd',
            'pool',
            'get',
            pool_name,
            prop)
        outbuf = output.read().decode()
        return int(outbuf.split()[1])

    def get_pool_dump(self, pool):
        """
        get the osd dump part of a pool
        """
        osd_dump = self.get_osd_dump_json()
        for i in osd_dump['pools']:
            if i['pool_name'] == pool:
                return i
        assert False

    def get_pool_num(self, pool):
        """
        get number for pool (e.g., data -> 2)
        """
        return int(self.get_pool_dump(pool)['pool'])

    def get_pgid(self, pool, pgnum):
        """
        :param pool: pool name
        :param pgnum: pg number
        :returns: a string representing this pg.
        """
        poolnum = self.get_pool_num(pool)
        pg_str = "{poolnum}.{pgnum}".format(
            poolnum=poolnum,
            pgnum=pgnum)
        return pg_str

    def get_pg_primary(self, pool, pgnum):
        """
        get primary for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        (output, err) = self.raw_cluster_cmd("pg", "map", pg_str, '--format=json')
        outbuf = output.read().decode()
        j = json.loads('\n'.join(outbuf.split('\n')[1:]))
        return int(j['acting'][0])
        assert False

    def get_pg_random(self, pool, pgnum):
        """
        get random osd for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        (output, err) = self.raw_cluster_cmd("pg", "map", pg_str, '--format=json')
        outbuf = output.read().decode()
        j = json.loads('\n'.join(outbuf.split('\n')[1:]))
        return int(j['acting'][random.randint(0, len(j['acting']) - 1)])
        assert False

    def kill_osd(self, osd_node, osd_service):
        """
        :params: id , type of signal, list of osd objects
            type: "SIGKILL", "SIGTERM", "SIGHUP" etc.
        :returns: 1 or 0
        """
        self.log("Inside KILL_OSD")
        kill_cmd = 'sudo systemctl stop {osd_service}'.format(osd_service=osd_service)
        self.log("kill cmd will be run on {osd}".format(osd=osd_node.hostname))
        print(kill_cmd)
        try:
            osd_node.exec_command(cmd=kill_cmd)
            return 0
        except Exception:
            self.log("failed to kill osd")
            self.log(traceback.format_exc())
            return 1

    def is_up(self, osd_id):
        """
        :return 1 if up, 0 if down
        """
        (output, err) = self.raw_cluster_cmd("osd", "dump", '--format=json')
        outbuf = output.read().decode()
        jbuf = json.loads(outbuf)
        self.log(jbuf)

        for osd in jbuf['osds']:
            if osd_id == osd['osd']:
                return osd['up']

    def revive_osd(self, osd_node, osd_service):
        """
        :returns: 0 if revive success,1 if fail
        """
        # if self.is_up(osd_id):
        #     return 0
        if osd_node:
            revive_cmd = 'sudo systemctl start {osd_service}'.format(
                osd_service=osd_service)
            print(revive_cmd)
            try:
                osd_node.exec_command(cmd=revive_cmd)
                return 0
            except Exception:
                self.log("failed to revive")
                self.log(traceback.format_exc())
                return 1
        return 1

    def get_mgr_proxy_container(self, node, docker_image, proxy_container='mgr_proxy'):
        """
        Returns mgr dummy container to access containerized storage
        Args:
            node (ceph.ceph.CephNode): ceph node
            docker_image(str): repository/image:tag

        Returns:
            ceph.ceph.CephDemon: mgr object
        """
        out, err = node.exec_command(cmd='sudo docker inspect {container}'.format(container=proxy_container),
                                     check_ec=False)
        if err.read():
            node.exec_command(
                cmd='sudo /usr/bin/docker-current run -d --rm --net=host --privileged=true --pid=host --memory=1839m '
                    '--cpu-quota=100000 -v /dev:/dev -v /etc/localtime:/etc/localtime:ro -v '
                    '/var/lib/ceph:/var/lib/ceph:z '
                    '-v /etc/ceph:/etc/ceph:z -v /var/run/ceph:/var/run/ceph:z -e CEPH_DAEMON=MGR  '
                    '--name={container} {docker_image}'.format(container=proxy_container, docker_image=docker_image))
            mgr_object = node.create_ceph_object('mgr')
            mgr_object.containerized = True
            mgr_object.container_name = proxy_container
        else:
            mgr_object = \
                [mgr_object for mgr_object in node.get_ceph_objects('mgr') if
                 mgr_object.containerized and mgr_object.container_name == proxy_container][0]

        return mgr_object
