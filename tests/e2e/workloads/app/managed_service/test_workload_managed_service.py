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
        get_consumer_clusters,
        # jenkins_factory_fixture,
        pgsql_factory_fixture,
        # amq_factory_fixture,
    ):
        """
        test workloads managed service
        """
        self.jenkins_deployment_status = True
        self.pgsql_deployment_status = None
        self.amq_deployment_status = True

        multi_consumer_wl_dict = {
            1: [["pgsql"]],
            2: [["pgsql"]],
            3: [["pgsql"]],
        }
        workloads_cluster_index = dict()

        workloads = multi_consumer_wl_dict.get(len(config.index_consumer_clusters), 3)
        for sub_workloads, index_consumer in zip(
            workloads, config.index_consumer_clusters
        ):
            sub_workloads.append(index_consumer)

        for sub_workloads in workloads:
            for workload in sub_workloads:
                # if workload == "jenkins":
                #     workloads_cluster_index["jenkins"] = sub_workloads[
                #         len(sub_workloads) - 1
                #     ]
                #     try:
                #         jenkins_obj = jenkins_factory_fixture(
                #             num_projects=1,
                #             num_of_builds=1,
                #             wait_for_build_to_complete=False,
                #             consumer_index=workloads_cluster_index["jenkins"],
                #         )
                #         self.jenkins_deployment_status = True
                #     except Exception as e:
                #         self.jenkins_deployment_status = f"Jenkins workload errors {e},"
                #         log.error(e)

                if workload == "pgsql":
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
                        self.pgsql_deployment_status = f"Pgsql workload errors {e},"
                        log.error(e)

                # elif workload == "amq":
                #     workloads_cluster_index["amq"] = sub_workloads[
                #         len(sub_workloads) - 1
                #     ]
                #     try:
                #         amq = amq_factory_fixture(
                #             sc_name=constants.DEFAULT_STORAGECLASS_RBD,
                #             consumer_index=workloads_cluster_index["amq"],
                #             run_in_bg=False,
                #             validate_messages=False,
                #         )
                #         self.amq_deployment_status = True
                #     except Exception as e:
                #         self.amq_deployment_status = f"AMQ workload errors {e},"
                #         log.error(e)

        # if self.jenkins_deployment_status is True:
        #     try:
        #         config.switch_ctx(workloads_cluster_index["jenkins"])
        #         log.info(f"consumer_index={workloads_cluster_index['jenkins']}")
        #         jenkins_obj.wait_for_build_to_complete()
        #     except Exception as e:
        #         log.error(e)
        #         self.jenkins_deployment_status = f"Jenkins workload errors {e}"

        if self.pgsql_deployment_status is True:
            try:
                config.switch_ctx(workloads_cluster_index["pgsql"])
                log.info(f"consumer_index={workloads_cluster_index['pgsql']}")
                pgsql_obj.wait_for_pgbench_status(
                    status=constants.STATUS_COMPLETED, timeout=9000
                )
                pgbench_pods = pgsql_obj.get_pgbench_pods()
                pgsql_obj.validate_pgbench_run(pgbench_pods)
            except Exception as e:
                log.error(e)
                self.pgsql_deployment_status = f"PGSQL workload errors {e}"

        # if self.amq_deployment_status is True:
        #     try:
        #         log.info(f"consumer_index={workloads_cluster_index['amq']}")
        #         config.switch_ctx(workloads_cluster_index["amq"])
        #         amq.validate_messages_are_produced()
        #         amq.validate_messages_are_consumed()
        #     except Exception as e:
        #         log.error(e)
        #         self.amq_deployment_status = f"AMQ workload errors {e}"

        log.info(
            f"{self.jenkins_deployment_status}\n{self.pgsql_deployment_status}\n{self.amq_deployment_status}"
        )
        assert [
            self.jenkins_deployment_status,
            self.pgsql_deployment_status,
            self.amq_deployment_status,
        ] == [True, True, True], "Not all Workloads pass"
