import random

from tests.iscsi.iscsi_utils import IscsiUtils


def run(**kw):
    ceph_nodes = kw.get('ceph_nodes')
    clients = kw.get('clients')
    win_client = clients[0]
    config = kw.get('config')
    test_data = kw.get('test_data')
    no_of_luns = config.get('no_of_luns', 10)
    image_name = 'test_image' + str(random.randint(10, 999))
    login = test_data['initiator_name'].split(":")[1]

    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_util.create_luns(
        no_of_luns,
        test_data['gwcli_node'],
        test_data['initiator_name'],
        image_name,
        iosize="2g",
        map_to_client=True)

    win_client.connect_to_target(test_data['gwcli_node'].private_ip, login, "redhat@123456")
    win_client.create_disk(no_of_luns)
    job_options = iscsi_util.get_fio_jobs(no_of_luns)
    win_client.create_fio_job_options(job_options)
    output = win_client.run_fio_test()
    return output
