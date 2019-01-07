import logging

import re

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Remove monitor from cluster using shrink-mon.yml

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster

    Returns:
        int: non-zero on failure, zero on pass
    """
    log.info("Shrinking monitor")
    config = kw.get('config')
    mon_to_kill_list = config.get('mon-to-kill')
    mon_to_kill = None
    mon_short_name_list = [ceph_node.shortname for ceph_node in ceph_cluster.get_nodes('mon')]
    for _mon_to_kill in mon_to_kill_list:
        matcher = re.compile(_mon_to_kill)
        matched_short_names = list(filter(matcher.match, mon_short_name_list))
        if len(matched_short_names) > 0:
            shrinked_nodes = [ceph_node for ceph_node in ceph_cluster if ceph_node.shortname in matched_short_names]
            for ceph_node in shrinked_nodes:
                ceph_node.remove_ceph_object(ceph_node.get_ceph_objects('mon')[0])
        else:
            raise RuntimeError('No match for {node_name}'.format(node_name=_mon_to_kill))
        mon_to_kill = ','.join([mon_to_kill, ','.join(matched_short_names)]) if mon_to_kill else ','.join(
            matched_short_names)

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    ceph_installer.node.obtain_root_permissions('/var/log')
    ansible_dir = ceph_installer.ansible_dir

    ceph_installer.exec_command(sudo=True, cmd='cp -R {ansible_dir}/infrastructure-playbooks/shrink-mon.yml '
                                               '{ansible_dir}/shrink-mon.yml'.format(ansible_dir=ansible_dir))

    out, err = ceph_installer.exec_command(cmd='export ANSIBLE_DEPRECATION_WARNINGS=False ; cd {ansible_dir} ; '
                                               'ansible-playbook -e ireallymeanit=yes shrink-mon.yml '
                                               '-e mon_to_kill={mon_to_kill} -i hosts'.format(ansible_dir=ansible_dir,
                                                                                              mon_to_kill=mon_to_kill),
                                           long_running=True)

    if err != 0:
        log.error("Failed during ansible playbook run")
        return err

    return ceph_cluster.check_health()
