import logging
import time
import os

from ceph.parallel import parallel

logger = logging.getLogger(__name__)
log = logger



def run(**kw):
    log.info("Running workunit test")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')

    clients = []
    role = 'client'
    if config.get('role'):
        role = config.get('role')
    with parallel() as p:
        for cnode in ceph_nodes:
            if cnode.role == role:
                if config.get('kernel-repo'):
                    repo = config.get('kernel-repo')
                    log.info("writing " + repo)
                    p.spawn(update_kernel_and_reboot, cnode, repo)
                elif os.environ['KERNEL-REPO-URL'] is not None:
                    log.info("writing from ENV " + repo)
                    repo = os.environ['KERNEL-REPO-URL']
                    p.spawn(update_kernel_and_reboot, cnode, repo)

    return 0

def update_kernel_and_reboot(client, repo_url):

    kernel_repo_file = """
[KernelUpdate]
name=KernelUpdate
baseurl= {base_url}
gpgcheck=0
enabled=1
""".format(base_url=repo_url)
    kernel_repo = client.write_file(sudo=True,
                                file_name='/etc/yum.repos.d/rh_kernel.repo',
                                file_mode='w')
    kernel_repo.write(kernel_repo_file)
    kernel_repo.flush()
    o, e = client.exec_command(cmd='uname -a')
    log.info(o.read())
    client.exec_command(cmd='sudo yum update metadata')
    o, e = client.exec_command(cmd='sudo yum update -y kernel')
    client.exec_command(cmd='sudo reboot', check_ec=False)
    time.sleep(300)
    client.reconnect()
    o, e = client.exec_command(cmd='uname -a')
    log.info(o.read())
