import pytest
from datetime import datetime
import logging
import tempfile
import ocs_ci.ocs.bdi.config as bdi_config
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs import ocp
from ocs_ci.utility import templating
from ocs_ci.framework import config


from ocs_ci.helpers.bdi_helpers import (
    init_configure_workload_yaml,
    init_data_load_yaml,
    init_run_workload_yaml,
    install_db2u,
    clean_temp_files,
)

log = logging.getLogger(__name__)


@workloads
class TestBdiWorkloadBaseClass(E2ETest):
    temp_helm_options = tempfile.NamedTemporaryFile(
        mode="w", prefix="helm_options_temp_", delete=False
    )

    @pytest.fixture(autouse=True)
    def setup(
        self,
        clone_ibm_chart_project_factory,
        create_machineset_factory,
        install_helm_factory,
        create_db2u_project_factory,
        create_scc_factory,
        create_security_prereqs_factory,
        create_secretes_factory,
        create_ibm_container_registry_factory,
    ):
        clone_ibm_chart_project_factory(
            destination_dir=bdi_config.bdi_dir, git_url=bdi_config.chart_git_url
        )
        if config.ENV_DATA["deployment_type"].lower() == "ipi":
            create_machineset_factory(additional_nodes=bdi_config.machine_set_replica)
        install_helm_factory()
        create_db2u_project_factory(db2u_project_name=bdi_config.db2u_project)
        create_scc_factory(db2u_project_name=bdi_config.db2u_project)
        create_security_prereqs_factory(
            db2u_project_name=bdi_config.db2u_project,
            bdi_dir=bdi_config.bdi_dir,
            chart_dir=bdi_config.chart_dir,
        )
        create_secretes_factory(
            db2u_project_name=bdi_config.db2u_project,
            ldap_r_n=bdi_config.ldap_r_n,
            ldap_r_p=bdi_config.ldap_r_p,
            db2u_r_n=bdi_config.db2u_r_n,
            db2u_r_p=bdi_config.db2u_r_p,
        )
        bdi_config.ibm_cloud_key = config.AUTH["ibmcloud"]["api_key"]
        create_ibm_container_registry_factory(
            db2u_project_name=bdi_config.db2u_project,
            ibm_cloud_key=bdi_config.ibm_cloud_key,
        )

    def run(self):
        """
        Installing db2warehouse, running the workload and perform a cleanup for temporary files

        """
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Start setup at: {current_time} #######################"
        )
        install_db2u(self)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished setup at: {current_time} #######################"
        )
        self.run_workload()
        clean_temp_files(self)

    def run_workload(self):
        """Running the workload:
        This function instantiates and creates 3 yaml files for creating 3 jobs:
        - configure workload - Creates database and all required tables as well as generates the data.
        - data load - loads the data to the DB tables.
        - run workload - runs 3 iterations for reading/writing from/to the DB tables:
            - warmup
            - static values, multiuser (16 users)
            - static values, multiuser (32 users)

        """

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Start configure-workload at: {current_time} #######################"
        )
        ocp_jobs = ocp.OCP(kind="Job", namespace=bdi_config.db2u_project)

        # CONFIGURE WORKLOAD
        bdi_config.temp_configure_dict = init_configure_workload_yaml(
            namespace=bdi_config.db2u_project,
            image=bdi_config.db2u_image_url,
            sf=bdi_config.scale_factor,
            pvc_name=bdi_config.db2u_pvc_name,
        )

        bdi_config.temp_yaml_configure = tempfile.NamedTemporaryFile(
            mode="w", prefix="configure_yaml_", delete=False
        )
        templating.dump_data_to_temp_yaml(
            bdi_config.temp_configure_dict, bdi_config.temp_yaml_configure.name
        )

        log.info("Creating Job for for configure-workload")
        ocp_jobs.create(yaml_file=bdi_config.temp_yaml_configure.name)
        ocp_jobs.wait_for_resource(
            condition="1/1",
            resource_name="db2u-configure-workload-job",
            column="COMPLETIONS",
            timeout=bdi_config.configure_timeout,
            sleep=30,
        )
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished configure-workload at: {current_time} #######################"
        )
        log.info(
            f"##################### Start data-load at: {current_time} #######################"
        )
        # DATA WORKLOAD
        bdi_config.temp_data_load_dict = init_data_load_yaml(
            namespace=bdi_config.db2u_project,
            image=bdi_config.db2u_image_url,
            pvc_name=bdi_config.db2u_pvc_name,
            secret_name=bdi_config.db2u_secret_name,
        )
        bdi_config.temp_yaml_data = tempfile.NamedTemporaryFile(
            mode="w", prefix="data_yaml_", delete=False
        )
        templating.dump_data_to_temp_yaml(
            bdi_config.temp_data_load_dict, bdi_config.temp_yaml_data.name
        )

        log.info("Creating Job for for data-load")
        ocp_jobs.create(yaml_file=bdi_config.temp_yaml_data.name)
        ocp_jobs.wait_for_resource(
            condition="1/1",
            resource_name="db2u-data-load-job",
            column="COMPLETIONS",
            timeout=bdi_config.data_load_timeout,
            sleep=30,
        )

        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished data-load at: {current_time} #######################"
        )
        log.info(
            f"##################### Start run-workload at: {current_time} #######################"
        )
        # RUN WORKLOAD
        bdi_config.temp_run_dict = init_run_workload_yaml(
            namespace=bdi_config.db2u_project,
            image=bdi_config.db2u_image_url,
            pvc_name=bdi_config.db2u_pvc_name,
            secret_name=bdi_config.db2u_secret_name,
        )
        bdi_config.temp_yaml_run = tempfile.NamedTemporaryFile(
            mode="w", prefix="run_yaml_", delete=False
        )
        templating.dump_data_to_temp_yaml(
            bdi_config.temp_run_dict, bdi_config.temp_yaml_run.name
        )

        log.info("Creating Job for for run-workload")
        ocp_jobs.create(yaml_file=bdi_config.temp_yaml_run.name)
        ocp_jobs.wait_for_resource(
            condition="1/1",
            resource_name="db2u-run-workload-job",
            column="COMPLETIONS",
            timeout=bdi_config.run_workload_timeout,
            sleep=30,
        )
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished run-workload at: {current_time} #######################"
        )
