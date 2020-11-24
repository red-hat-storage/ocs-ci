import pytest
from datetime import datetime
import logging
import tempfile
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs import ocp
from ocs_ci.utility import templating

from ocs_ci.helpers.bdi_helpers import (
    init_configure_workload_yaml,
    init_data_load_yaml,
    init_run_workload_yaml,
    install_db2u,
    clean_temp_files,
)

from tests.e2e.workloads.bdi.conftest import (
    clone_ibm_chart_project,
    create_machineset,
    install_helm,
    create_db2u_project,
    create_security_context_constraints,
    security_prereqs,
    create_secretes,
    ibm_container_registry,
)

log = logging.getLogger(__name__)


@workloads
@pytest.mark.usefixtures(
    clone_ibm_chart_project.__name__,
    create_machineset.__name__,
    install_helm.__name__,
    create_db2u_project.__name__,
    create_security_context_constraints.__name__,
    security_prereqs.__name__,
    create_secretes.__name__,
    ibm_container_registry.__name__,
)
class TestBdiWorkloadBaseClass(E2ETest):
    chart_dir = "/stable/ibm-db2warehouse/ibm_cloud_pak/pak_extensions"
    chart_git_url = "https://github.com/IBM/charts"
    tiller_namespace = "tiller"
    helm_tar_file = "helm-v2.9.0-linux-amd64.tar.gz"
    helm_url = "https://storage.googleapis.com/kubernetes-helm/" + helm_tar_file
    helm_dir = None
    helm_version = "v2.9.0"
    tiller_template_url = "https://github.com/openshift/origin/raw/master/examples/helm/tiller-template.yaml"
    bdi_dir = "/tmp/bdi_temp_dir"
    db2u_project = "db2u-project"
    ldap_r_n = "test-bdi-db-release-name"
    ldap_r_p = "test-bdi-bluadmin-release-password"
    db2u_r_n = ldap_r_n
    db2u_r_p = "test-bdi-db-release-password"
    ibm_cloud_key = "bIniFfn9hDJ4MlJZE2u_s0CpglC1hKgWs0W7IaLcdg7p"
    ms_name = None
    machine_set_replica = 3
    db2u_pvc_name_suffix = "-db2u-meta-storage"
    db2wh_scc = "db2wh-scc"

    db2u_configure_workload_job_name = None
    db2u_image_url = "icr.io/obs/hdm/db2u/db2u.db2client.workload:11.5.4.0-1362-x86_64"

    # TODO: Move SF to be passed as a parameter
    temp_configure_dict = None
    temp_data_load_dict = None
    temp_run_dict = None
    pvc_size = "50Gi"
    scale_factor = 1
    configure_timeout = 1200
    data_load_timeout = 1800
    run_workload_timeout = 10800

    dbu2_pvc_size = None
    temp_tiller_yaml = None

    db2u_pvc_name = ldap_r_n + db2u_pvc_name_suffix

    db2u_data_load_job_name = None
    db2u_secret_name = db2u_r_n + "-db2u-instance"
    db2u_run_workload_job_name = None
    temp_yaml_configure = None
    temp_yaml_data = None
    temp_yaml_run = None
    temp_helm_options = tempfile.NamedTemporaryFile(
        mode="w", prefix="helm_options_temp_", delete=False
    )
    temp_scc_yaml = None

    def run(self):
        """Installing db2warehouse, running the workload and perform a cleanup for temporary files"""
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

        ocp_jobs = ocp.OCP(kind="Job", namespace=self.db2u_project)

        # CONFIGURE WORKLOAD
        self.temp_configure_dict = init_configure_workload_yaml(
            namespace=self.db2u_project,
            image=self.db2u_image_url,
            sf=self.scale_factor,
            pvc_name=self.db2u_pvc_name,
        )

        self.temp_yaml_configure = tempfile.NamedTemporaryFile(
            mode="w", prefix="configure_yaml_", delete=False
        )
        templating.dump_data_to_temp_yaml(
            self.temp_configure_dict, self.temp_yaml_configure.name
        )

        log.info("Creating Job for for configure-workload")
        ocp_jobs.create(yaml_file=self.temp_yaml_configure.name)
        ocp_jobs.wait_for_resource(
            condition="1/1",
            resource_name="db2u-configure-workload-job",
            column="COMPLETIONS",
            timeout=self.configure_timeout,
            sleep=30,
        )
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished configure-workload at: {current_time} #######################"
        )

        # DATA WORKLOAD
        self.temp_data_load_dict = init_data_load_yaml(
            namespace=self.db2u_project,
            image=self.db2u_image_url,
            pvc_name=self.db2u_pvc_name,
            secret_name=self.db2u_secret_name,
        )
        self.temp_yaml_data = tempfile.NamedTemporaryFile(
            mode="w", prefix="data_yaml_", delete=False
        )
        templating.dump_data_to_temp_yaml(
            self.temp_data_load_dict, self.temp_yaml_data.name
        )

        log.info("Creating Job for for data-load")
        ocp_jobs.create(yaml_file=self.temp_yaml_data.name)
        ocp_jobs.wait_for_resource(
            condition="1/1",
            resource_name="db2u-data-load-job",
            column="COMPLETIONS",
            timeout=self.data_load_timeout,
            sleep=30,
        )

        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished data-load at: {current_time} #######################"
        )

        # RUN WORKLOAD
        self.temp_run_dict = init_run_workload_yaml(
            namespace=self.db2u_project,
            image=self.db2u_image_url,
            pvc_name=self.db2u_pvc_name,
            secret_name=self.db2u_secret_name,
        )
        self.temp_yaml_run = tempfile.NamedTemporaryFile(
            mode="w", prefix="run_yaml_", delete=False
        )
        templating.dump_data_to_temp_yaml(self.temp_run_dict, self.temp_yaml_run.name)

        log.info("Creating Job for for run-workload")
        ocp_jobs.create(yaml_file=self.temp_yaml_run.name)
        ocp_jobs.wait_for_resource(
            condition="1/1",
            resource_name="db2u-run-workload-job",
            column="COMPLETIONS",
            timeout=self.run_workload_timeout,
            sleep=30,
        )
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")
        log.info(
            f"##################### Finished run-workload at: {current_time} #######################"
        )
