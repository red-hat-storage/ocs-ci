import os
from ocs.utils import create_oc_resource
from utility import templating
from utility.get_ceph_secret import get_ceph_secret
from utility.get_ip_addrs import get_ip_addrs_for_ceph_type


def make_rbd_pool(cluster_namespace, local_dir):
    """
    Make an rbd pool for the calling rbd storage class

    Args:
        cluster_namespace (str): namespace used by tester.
        local_dir (str): diretory where rbd_pool.yaml will be stored

    """
    env_data = dict()
    env_data['cluster_namespace'] = cluster_namespace
    templ_parm = templating.Templating()
    create_oc_resource('rbd_pool.yaml', local_dir, templ_parm,
                       env_data)


def make_rbd_storage_class(cluster_namespace):
    """
    Make rbd storage class.
    This code will fail if the private namespace corresponding to
    cluster_namespace has not already been created.

    Args:
        cluster_namespace (str): namespace used by tester.

    Returns:
        True if rbd_storage_class.yaml file is built and the corresponding
        storage class is created.  False if this class is not created.

    """
    port = '6789'
    env_data = dict()
    env_data['cluster_namespace'] = cluster_namespace
    local_dir = f"/tmp/{cluster_namespace}"
    if not os.path.isdir(local_dir):
        print(f"namespace directory {local_dir} is invalid")
        return False
    env_data['ceph_secret'] = get_ceph_secret(namespace='openshift-storage')
    env_data['pool_name'] = cluster_namespace
    mon_ips = [f"{x}:{port}" for x in get_ip_addrs_for_ceph_type('mon')]
    env_data['mon_ports'] = ','.join(mon_ips)
    make_rbd_pool(cluster_namespace, local_dir)
    templ_parm = templating.Templating()
    create_oc_resource('rbd_storage_class.yaml', local_dir, templ_parm,
                       env_data)
    return True


if __name__ == "__main__":
    make_rbd_storage_class('ocs-368')
