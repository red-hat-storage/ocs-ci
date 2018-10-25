import logging

from ceph.ceph import NodeVolume

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Remove osd from cluster using shrink-osd.yml

    Args:
        ceph_cluster(ceph.ceph.Ceph):
        **kw:
            config sample:
                config:
                  osd-to-kill:
                    - 2
    Returns:
        int: non-zero on failure, zero on pass
    """
    log.info("Shrinking osd")
    config = kw.get('config')
    osd_to_kill_list = config.get('osd-to-kill')
    osd_to_kill = ','.join((str(osd) for osd in osd_to_kill_list))

    for osd_id in osd_to_kill_list:
        osd_host = ceph_cluster.get_osd_metadata(osd_id).get('hostname')
        for ceph_node in ceph_cluster:
            osd_volumes = ceph_node.get_allocated_volumes()
            if ceph_node.shortname == str(osd_host):
                osd_volumes.pop().status = NodeVolume.FREE
                if len(osd_volumes) < 1:
                    ceph_node.remove_ceph_object(ceph_node.get_ceph_objects('osd')[0])

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    ceph_installer.node.obtain_root_permissions('/var/log')
    ansible_dir = ceph_installer.ansible_dir

    ceph_installer.exec_command(sudo=True, cmd='cp -R {ansible_dir}/infrastructure-playbooks/shrink-osd.yml '
                                               '{ansible_dir}/shrink-osd.yml'.format(ansible_dir=ansible_dir))

    out, err = ceph_installer.exec_command(cmd='export ANSIBLE_DEPRECATION_WARNINGS=False ; cd {ansible_dir} ; '
                                               'ansible-playbook -e ireallymeanit=yes shrink-osd.yml '
                                               '-e osd_to_kill={osd_to_kill} -i hosts'.format(ansible_dir=ansible_dir,
                                                                                              osd_to_kill=osd_to_kill),
                                           long_running=True)

    if err != 0:
        log.error("Failed during ansible playbook run")
        return err

    return ceph_cluster.check_health()
