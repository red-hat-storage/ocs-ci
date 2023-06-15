import json
import logging
import os
import random
import time
import traceback
import tempfile

from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


class RadosHelper:
    def __init__(self, mon, config=None, log=None, cluster="ceph"):
        self.mon = mon
        self.config = config
        if log:
            self.log = lambda x: log.info(x)
        self.num_pools = self.get_num_pools()
        self.cluster = cluster
        pools = self.list_pools()
        self.pools = {}
        for pool in pools:
            self.pools[pool] = self.get_pool_property(pool, "pg_num")

    def raw_cluster_cmd(self, *args):
        """
        :return: (stdout, stderr)
        """
        ceph_args = [
            "sudo",
            "ceph",
            "--cluster",
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
        """TODO"""

    def get_osd_dump_json(self):
        """
        osd dump --format=json converted to a python object
        :returns: the python object
        """
        (out, err) = self.raw_cluster_cmd("osd", "dump", "--format=json")
        print(type(out))
        outbuf = out.read().decode()
        return json.loads("\n".join(outbuf.split("\n")[1:]))

    def create_pool(
        self,
        pool_name,
        pg_num=16,
        erasure_code_profile_name=None,
        min_size=None,
        erasure_code_use_overwrites=False,
    ):
        """
        Create a pool named from the pool_name parameter.

        Args:
            pool_name (str): name of the pool being created.
            pg_num (int): initial number of pgs.
            erasure_code_profile_name (str): if set and !None create an
                erasure coded pool using the profile
            min_size (int): minimum size
            erasure_code_use_overwrites (bool): if True, allow overwrites

        """
        assert isinstance(pool_name, str)
        assert isinstance(pg_num, int)
        assert pool_name not in self.pools
        self.log("creating pool_name %s" % (pool_name,))
        if erasure_code_profile_name:
            self.raw_cluster_cmd(
                "osd",
                "pool",
                "create",
                pool_name,
                str(pg_num),
                str(pg_num),
                "erasure",
                erasure_code_profile_name,
            )
        else:
            self.raw_cluster_cmd("osd", "pool", "create", pool_name, str(pg_num))
        if min_size is not None:
            self.raw_cluster_cmd(
                "osd", "pool", "set", pool_name, "min_size", str(min_size)
            )
        if erasure_code_use_overwrites:
            self.raw_cluster_cmd(
                "osd", "pool", "set", pool_name, "allow_ec_overwrites", "true"
            )
        self.raw_cluster_cmd(
            "osd",
            "pool",
            "application",
            "enable",
            pool_name,
            "rados",
            "--yes-i-really-mean-it",
        )
        self.pools[pool_name] = pg_num
        time.sleep(1)

    def list_pools(self):
        """
        list all pool names
        """
        osd_dump = self.get_osd_dump_json()
        self.log(osd_dump["pools"])
        return [str(i["pool_name"]) for i in osd_dump["pools"]]

    def get_pool_property(self, pool_name, prop):
        """
        :param pool_name: pool
        :param prop: property to be checked.
        :returns: property as an int value.
        """
        assert isinstance(pool_name, str)
        assert isinstance(prop, str)
        (output, err) = self.raw_cluster_cmd("osd", "pool", "get", pool_name, prop)
        outbuf = output.read().decode()
        return int(outbuf.split()[1])

    def get_pool_dump(self, pool):
        """
        get the osd dump part of a pool
        """
        osd_dump = self.get_osd_dump_json()
        for i in osd_dump["pools"]:
            if i["pool_name"] == pool:
                return i
        assert False

    def get_pool_num(self, pool):
        """
        get number for pool (e.g., data -> 2)
        """
        return int(self.get_pool_dump(pool)["pool"])

    def get_pgid(self, pool, pgnum):
        """
        :param pool: pool name
        :param pgnum: pg number
        :returns: a string representing this pg.
        """
        poolnum = self.get_pool_num(pool)
        pg_str = "{poolnum}.{pgnum}".format(poolnum=poolnum, pgnum=pgnum)
        return pg_str

    def get_pg_primary(self, pool, pgnum):
        """
        get primary for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        (output, err) = self.raw_cluster_cmd("pg", "map", pg_str, "--format=json")
        outbuf = output.read().decode()
        j = json.loads("\n".join(outbuf.split("\n")[1:]))
        return int(j["acting"][0])
        assert False

    def get_pg_random(self, pool, pgnum):
        """
        get random osd for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        (output, err) = self.raw_cluster_cmd("pg", "map", pg_str, "--format=json")
        outbuf = output.read().decode()
        j = json.loads("\n".join(outbuf.split("\n")[1:]))
        return int(j["acting"][random.randint(0, len(j["acting"]) - 1)])
        assert False

    def kill_osd(self, osd_node, osd_service):
        """
        :params: id , type of signal, list of osd objects
            type: "SIGKILL", "SIGTERM", "SIGHUP" etc.
        :returns: 1 or 0
        """
        self.log("Inside KILL_OSD")
        kill_cmd = "sudo systemctl stop {osd_service}".format(osd_service=osd_service)
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
        (output, err) = self.raw_cluster_cmd("osd", "dump", "--format=json")
        outbuf = output.read().decode()
        jbuf = json.loads(outbuf)
        self.log(jbuf)

        for osd in jbuf["osds"]:
            if osd_id == osd["osd"]:
                return osd["up"]

    def revive_osd(self, osd_node, osd_service):
        """
        :returns: 0 if revive success,1 if fail
        """
        # if self.is_up(osd_id):
        #     return 0
        if osd_node:
            revive_cmd = "sudo systemctl start {osd_service}".format(
                osd_service=osd_service
            )
            print(revive_cmd)
            try:
                osd_node.exec_command(cmd=revive_cmd)
                return 0
            except Exception:
                self.log("failed to revive")
                self.log(traceback.format_exc())
                return 1
        return 1

    def get_mgr_proxy_container(self, node, docker_image, proxy_container="mgr_proxy"):
        """
        Returns mgr dummy container to access containerized storage
        Args:
            node (ceph.ceph.CephNode): ceph node
            docker_image(str): repository/image:tag

        Returns:
            ceph.ceph.CephDemon: mgr object
        """
        out, err = node.exec_command(
            cmd="sudo docker inspect {container}".format(container=proxy_container),
            check_ec=False,
        )
        if err.read():
            node.exec_command(
                cmd="sudo /usr/bin/docker-current run -d --rm --net=host --privileged=true --pid=host --memory=1839m "
                "--cpu-quota=100000 -v /dev:/dev -v /etc/localtime:/etc/localtime:ro -v "
                "/var/lib/ceph:/var/lib/ceph:z "
                "-v /etc/ceph:/etc/ceph:z -v /var/run/ceph:/var/run/ceph:z -e CEPH_DAEMON=MGR  "
                "--name={container} {docker_image}".format(
                    container=proxy_container, docker_image=docker_image
                )
            )
            mgr_object = node.create_ceph_object("mgr")
            mgr_object.containerized = True
            mgr_object.container_name = proxy_container
        else:
            mgr_object = [
                mgr_object
                for mgr_object in node.get_ceph_objects("mgr")
                if mgr_object.containerized
                and mgr_object.container_name == proxy_container
            ][0]

        return mgr_object


def corrupt_pg(osd_deployment, pool_name, pool_object):
    """
    Rewrite given object in a ceph pool with /etc/shadow file.

    Args:
        osd_deployment (object): OSD deployment object where PG will be corrupted
        pool_name (str): name of ceph pool to be corrupted
        pool_object (str): name of object to be corrupted
    """
    osd_pod = osd_deployment.pods[0]
    osd_data = osd_pod.get()
    osd_id = osd_data["metadata"]["labels"]["ceph-osd-id"]

    for i_container in osd_data["spec"]["initContainers"]:
        if i_container["name"] == "expand-bluefs":
            bluefs_container = i_container
            break
    if not bluefs_container:
        raise ValueError("expand-bluefs container is missing")
    ceph_image = bluefs_container["image"]
    bridge_name = bluefs_container["volumeMounts"][0]["name"]

    ct_pod = pod.get_ceph_tools_pod()
    logger.info("Setting osd noout flag")
    ct_pod.exec_ceph_cmd("ceph osd set noout")
    logger.info("Setting osd noscrub flag")
    ct_pod.exec_ceph_cmd("ceph osd set noscrub")
    logger.info("Setting osd nodeep-scrub flag")
    ct_pod.exec_ceph_cmd("ceph osd set nodeep-scrub")

    logger.info(f"Looking for Placement Group ID with {pool_object} object")
    pgid = ct_pod.exec_ceph_cmd(f"ceph osd map {pool_name} {pool_object}")["pgid"]
    logger.info(f"Found Placement Group ID: {pgid}")

    # Update osd deployment with an initContainer that breaks the pool before
    # the ceph daemon is loaded.
    patch_change = (
        '[{"op": "add", "path": "/spec/template/spec/initContainers/-", "value": '
        f'{{ "args": ["--data-path", "/var/lib/ceph/osd/ceph-{osd_id}", "--pgid", '
        f'"{pgid}", "{pool_object}", "set-bytes", "/etc/shadow", "--no-mon-config"], '
        f'"command": [ "ceph-objectstore-tool" ], "image": "{ceph_image}", "imagePullPolicy": '
        '"IfNotPresent", "name": "corrupt-pg", "securityContext": {"privileged": true, '
        f'"runAsUser": 0}}, "volumeMounts": [{{"mountPath": "/var/lib/ceph/osd/ceph-0", '
        f'"name": "{bridge_name}", "subPath": "ceph-0"}}]}}}}]'
    )
    osd_deployment.ocp.patch(
        resource_name=osd_deployment.name, params=patch_change, format_type="json"
    )
    ct_pod.exec_ceph_cmd(f"ceph pg deep-scrub {pgid}")


def inject_corrupted_dups_into_pg_via_cot(
    osd_deployments, pgid, injected_dups_file_name_prefix="text"
):
    """
    Inject corrupted dups into a pg via COT

    Args:
        osd_deployments (OCS): List of OSD deployment OCS instances
        pgid (str): pgid for a pool eg: '1.55'
        injected_dups_file_name_prefix (str): File name prefix for injecting dups

    """
    # Create a text.json file with dup entries in it
    txt = (
        '[{"reqid": "client.4177.0:0", "version": "111\'999999999", "user_version": "0", '
        '"generate": "7000", "return_code": "0"},]'
    )
    tmpfile = tempfile.NamedTemporaryFile(
        prefix=f"{injected_dups_file_name_prefix}", suffix=".json", delete=False
    )
    with open(tmpfile.name, "w") as f:
        f.write(txt)
    # Copy the dups entries file to the osd running node and inject corrupted dups into the pg via COT
    for deployment in osd_deployments:
        osd_pod = deployment.pods[0]
        osd_id = osd_pod.labels["ceph-osd-id"]
        logger.info(
            f"Inject corrupted dups into the pgid:{pgid} for osd:{osd_id} using json file data /n {txt}"
        )
        target_path = f"/tmp/{os.path.basename(tmpfile.name)}"
        osd_pod.copy_to_pod_cat(tmpfile.name, target_path)
        osd_pod.exec_sh_cmd_on_pod(
            f"CEPH_ARGS='--no_mon_config --osd_pg_log_dups_tracked=999999999999' "
            f"ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-"
            f"{osd_id} --pgid {pgid} --op pg-log-inject-dups --file {target_path}",
            shell=True,
        )


def get_pg_log_dups_count_via_cot(osd_deployments, pgid):
    """
    Get the pg log dup entries count via COT

    Args:
        osd_deployments (OCS): List of OSD deployment OCS instances
        pgid (str): pgid for a pool eg: '1.55'

    Return:
        list: List of total number of pg dups per osd

    """
    osd_pg_log_dups = []
    for deployment in osd_deployments:
        osd_pod = deployment.pods[0]
        osd_id = osd_pod.labels["ceph-osd-id"]
        logger.info(
            f"Get the pg dup entries count injected into pgid:{pgid} for osd:{osd_id}"
        )
        osd_pod.exec_sh_cmd_on_pod(
            f"CEPH_ARGS='--no_mon_config --osd_pg_log_dups_tracked=999999999999' "
            f"ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-"
            f"{osd_id} --op log --pgid {pgid}  > /var/log/ceph/pg_log_{pgid}.txt"
        )
        logger.info(
            "Copy current pg log dups file to local folder and parse dups number"
        )
        temp_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix=f"pg_log_{pgid}", delete=True
        )
        osd_pod.copy_from_pod_oc_exec(
            target_path=temp_file.name, src_path=f"/var/log/ceph/pg_log_{pgid}.txt"
        )
        res = exec_cmd(
            f"cat {temp_file.name} | jq  '(.pg_log_t.log|length),(.pg_log_t.dups|length)'",
            shell=True,
        )
        total_dups = int(res.stdout.decode("utf-8").split("\n")[1])
        osd_pg_log_dups.append(total_dups)

    return osd_pg_log_dups
