import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants


log = logging.getLogger(__name__)


class TestWorkLoadsManagedService(object):
    """
    Test WorkLoads Managed Service

    """

    def test_workloads_managed_service(
        self,
        # get_consumer_clusters,
        jenkins_factory_fixture,
        pgsql_factory_fixture,
        amq_factory_fixture,
        couchbase_factory_fixture,
    ):
        """
        test workloads managed service
        """
        config.index_consumer_clusters = [1, 2]
        self.jenkins_deployment_status = False
        self.pgsql_deployment_status = False
        self.amq_deployment_status = False
        self.couchbase_deployment_status = False
        multi_consumer_wl_dict = {
            1: [["jenkins", "pgsql", "amq", "couchbase"]],
            2: [["jenkins", "pgsql"], ["amq", "couchbase"]],
            3: [["jenkins"], ["pgsql"], ["amq", "couchbase"]],
            4: [["jenkins"], ["pgsql"], ["amq"], ["couchbase"]],
        }
        workloads_cluster_index = dict()

        workloads = multi_consumer_wl_dict.get(len(config.index_consumer_clusters), 4)
        for sub_workloads, index_consumer in zip(
            workloads, config.index_consumer_clusters
        ):
            sub_workloads.append(index_consumer)

        for sub_workloads in workloads:
            for workload in sub_workloads:
                if workload == "jenkins":
                    workloads_cluster_index["jenkins"] = sub_workloads[
                        len(sub_workloads) - 1
                    ]
                    try:
                        jenkins_obj = jenkins_factory_fixture(
                            num_projects=1,
                            num_of_builds=1,
                            wait_for_build_to_complete=False,
                            consumer_index=workloads_cluster_index["jenkins"],
                        )
                        self.jenkins_deployment_status = True
                    except Exception as e:
                        log.error(e)

                elif workload == "pgsql":
                    workloads_cluster_index["pgsql"] = sub_workloads[
                        len(sub_workloads) - 1
                    ]
                    try:
                        pgsql_obj = pgsql_factory_fixture(
                            transactions=10,
                            replicas=1,
                            consumer_index=workloads_cluster_index["pgsql"],
                            wait_for_pgbench_to_complete=False,
                        )
                        self.pgsql_deployment_status = True
                    except Exception as e:
                        log.error(e)
                elif workload == "amq":
                    workloads_cluster_index["amq"] = sub_workloads[
                        len(sub_workloads) - 1
                    ]
                    try:
                        amq_threads = amq_factory_fixture(
                            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
                            index_consumer=workloads_cluster_index["amq"],
                            run_in_bg=False,
                            validate_messages=False,
                        )
                        self.amq_deployment_status = True
                    except Exception as e:
                        log.error(e)
                elif workload == "couchbase":
                    workloads_cluster_index["couchbase"] = sub_workloads[
                        len(sub_workloads) - 1
                    ]
                    try:
                        couchbase_obj = couchbase_factory_fixture(
                            index_consumer=workloads_cluster_index["couchbase"],
                            wait_for_pillowfights_to_complete=False,
                        )
                        self.couchbase_deployment_status = True
                    except Exception as e:
                        log.error(e)

        if self.jenkins_deployment_status:
            # config.switch_ctx(workloads_cluster_index["jenkins"])
            log.info(f"consumer_index={workloads_cluster_index['jenkins']}")
            jenkins_obj.wait_for_build_to_complete()

        if self.pgsql_deployment_status:
            # config.switch_ctx(workloads_cluster_index["pgsql"])
            log.info(f"consumer_index={workloads_cluster_index['pgsql']}")
            pgsql_obj.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)
            pgbench_pods = pgsql_obj.get_pgbench_pods()
            pgsql_obj.validate_pgbench_run(pgbench_pods)

        if self.couchbase_deployment_status:
            log.info(f"consumer_index={workloads_cluster_index['couchbase']}")
            # config.switch_ctx(workloads_cluster_index["couchbase"])
            couchbase_obj.run_workload(replicas=3)

        if self.amq_deployment_status:
            log.info(f"consumer_index={workloads_cluster_index['amq']}")
            # config.switch_ctx(workloads_cluster_index["amq"])
            for thread in amq_threads:
                thread.result(timeout=1800)
