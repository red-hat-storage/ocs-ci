
consumer2_name = None
provider_first = True

provider_name = 'ikave-52-pr'
consumer_name = 'ikave-52-c1'
provider_name = 'dosypenk-137-pr'
consumer_name = 'dosypenk-137-c1'
# consumer2_name = 'ikave-c2'
# provider_first = True

test_file = f'tests/manage/z_cluster/nodes/test_nodes_restart_ms.py::TestNodesRestartMS::test_osd_node_restart_and_check_osd_pods_status'
provider_config = 'conf/deployment/rosa/managed_3az_provider_qe_3m_3w_m52x.yaml'
consumer_config = 'conf/deployment/rosa/managed_3az_consumer_qe_3m_3w_m52x.yaml'
consumer2_config = 'conf/deployment/rosa/managed_3az_consumer_qe_3m_3w_m52x.yaml'

if provider_first:
    provider_name, consumer_name = consumer_name, provider_name
    provider_config, consumer_config = consumer_config, provider_config

if consumer2_name:
    test_cmd = (
        f"run-ci multicluster 2 {test_file} --cluster1 --cluster-path path_to_cluster_{consumer_name} \\\n"
        f"--cluster-name {consumer_name} --ocp-version 4.11 --ocs-version 4.10 \\\n "
        f"--ocsci-conf conf/deployment/rosa/pager_duty.yaml \\\n"
        f"--ocsci-conf {consumer_config} \\\n"
        f"--cluster2 --ocsci-conf {provider_config} \\\n"
        f"--cluster-path path_to_cluster_{provider_name} --cluster-name {provider_name} --ocp-version 4.10 \\\n"
        f"--ocs-version 4.10 --ocsci-conf conf/deployment/rosa/pager_duty.yaml \\\n"
        f"--cluster3 --cluster-path path_to_cluster_{consumer2_name} \\\n"
        f"--cluster-name {consumer2_name} --ocp-version 4.11 --ocs-version 4.10 \\\n "
        f"--ocsci-conf conf/deployment/rosa/pager_duty.yaml \\\n"
        f"--ocsci-conf {consumer2_config} \n\n"
        f"****************************************************************************** \n\n"
    )
else:
    test_cmd = (
        f"run-ci multicluster 2 {test_file} --cluster1 --cluster-path path_to_cluster_{consumer_name} \\\n"
        f"--cluster-name {consumer_name} --ocp-version 4.11 --ocs-version 4.10 \\\n "
        f"--ocsci-conf conf/deployment/rosa/pager_duty.yaml \\\n"
        f"--ocsci-conf {consumer_config} \\\n"
        f"--cluster2 --ocsci-conf {provider_config} \\\n"
        f"--cluster-path path_to_cluster_{provider_name} --cluster-name {provider_name} --ocp-version 4.11 \\\n"
        f"--ocs-version 4.10 --ocsci-conf conf/deployment/rosa/pager_duty.yaml \n\n"
        f"****************************************************************************** \n\n"
    )


f = open("/home/ikave/Documents/run_multicluster_test_commands.txt", "a")
f.write(test_cmd)
f.close()
