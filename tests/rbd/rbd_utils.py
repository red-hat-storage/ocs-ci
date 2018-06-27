import random
import string
import logging

log = logging.getLogger(__name__)


class Rbd:
    def __init__(self, cluster):
        self.ceph_nodes = cluster

        # Identifying Monitor And Client node
        for node in self.ceph_nodes:
            if node.role == 'mon':
                self.ceph_mon = node
                continue
            if node.role == 'client':
                self.ceph_client = node
                continue

    def exec_cmd(self, **kw):
        cmd = kw.get('cmd')
        node = kw.get('node') if kw.get('node') else self.ceph_client

        out, err = node.exec_command(
            sudo=True, cmd=cmd, long_running=kw.get('long_running', False),
            check_ec=kw.get('check_ec', True))

        if kw.get('output', False):
            return out.read() if out else err.read()

        if out:
            return 0

        return 1

    def random_string(self):
        temp_str = ''.join(
            [random.choice(string.ascii_letters) for _ in xrange(10)])
        return temp_str

    def check_cmd_ec(self, arg):
        for rc in arg:
            if rc != 0:
                return 1
        return 0
