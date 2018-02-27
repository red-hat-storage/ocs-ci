import logging

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running exec test")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')

    clients = []
    role = 'installer'
    if config.get('role'):
        role = config.get('role')
    for cnode in ceph_nodes:
        if cnode.role == role:
            clients.append(cnode)

    idx = 0
    client = clients[idx]

    if config.get('idx'):
        idx = config['idx']
        client = clients[idx]

    cmd = ""
    if config.get('cmd'):
        cmd = config.get('cmd')

    if config.get('env'):
        env = config.get('env')
    else:
        env = ''

    if config.get('sudo'):
        sudo = 'sudo -E'
    else:
        sudo = ''

    cmd1 = '{env} {sudo} {cmd}'.format(env=env, sudo=sudo, cmd=cmd)
    output, ec = client.exec_command(cmd=cmd1,
                                     long_running=True)
    if ec == 0:
        log.info("Exec {cmd} completed successfully".format(cmd=cmd1))
    else:
        log.info("Error during Exec of {cmd}".format(cmd=cmd1))
    return ec
