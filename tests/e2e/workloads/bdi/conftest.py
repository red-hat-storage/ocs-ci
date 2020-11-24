import pytest
import os
import logging
import tempfile
from ocs_ci.utility.utils import run_cmd, clone_repo, exec_cmd
from ocs_ci.ocs import machine, node, ocp, constants
from ocs_ci.framework import config
from ocs_ci.utility import templating

log = logging.getLogger(__name__)


@pytest.fixture()
def clone_ibm_chart_project(request):
    """
    Clone IBM Chart project from Github
    """
    class_instance = request.node.cls

    def finalizer():
        """
        Delete bdi temporary directory
        """
        log.info(f"Deleting directory {class_instance.bdi_dir}")
        if os.path.isdir(class_instance.bdi_dir):
            run_cmd(cmd="rm -rf " + class_instance.bdi_dir)

    request.addfinalizer(finalizer)

    log.info(f"Cloning chart from github into {class_instance.bdi_dir}")
    clone_repo(url=class_instance.chart_git_url, location=class_instance.bdi_dir)


@pytest.fixture()
def create_machineset(request):
    """
    Creates a machineset with replica 3.
    Additional worker nodes are needed in order to support the workload execution (3 default workers is not enough)
    Relevant only for IPI deployment.
    If we want to use UPI deployment, we need to create the additional workers during the post deployment precess
    """

    if config.ENV_DATA["deployment_type"].lower() == "ipi":
        class_instance = request.node.cls

        def finalizer():
            """
            Delete machineset
            """
            if class_instance.ms_name is not None and machine.check_machineset_exists(
                machine_set=class_instance.ms_name
            ):
                log.info(f"Deleting machineset {class_instance.ms_name}")
                machine.delete_custom_machineset(class_instance.ms_name)

        request.addfinalizer(finalizer)
        log.info("Creating machineset")
        class_instance.ms_name = machine.create_custom_machineset(
            instance_type="m5.4xlarge", zone="a"
        )
        machine.wait_for_new_node_to_be_ready(class_instance.ms_name)
        log.info(
            f"Adding {class_instance.machine_set_replica} more nodes to machineset {class_instance.ms_name}"
        )
        node.add_new_node_and_label_it(
            machineset_name=class_instance.ms_name,
            num_nodes=class_instance.machine_set_replica,
            mark_for_ocs_label=False,
        )
        machine.wait_for_new_node_to_be_ready(class_instance.ms_name)


@pytest.fixture()
def install_helm(request):
    """
    Create 'tiller' project and install Helm client
    """
    class_instance = request.node.cls
    class_instance.helm_dir = tempfile.mkdtemp(prefix="helm_dir_")
    class_instance.temp_tiller_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="tiller_yaml_", delete=False
    )

    def finalizer():
        ocp_project = ocp.OCP(kind="Project", namespace=class_instance.tiller_namespace)
        ocp.switch_to_project("openshift-storage")
        log.info(f"Deleting project {class_instance.tiller_namespace}")
        ocp_project.delete_project(project_name=class_instance.tiller_namespace)
        ocp_project.wait_for_delete(resource_name=class_instance.tiller_namespace)

        if os.path.isdir(class_instance.helm_dir):
            run_cmd(cmd="rm -rf " + class_instance.helm_dir)
        if os.path.isfile(class_instance.temp_tiller_yaml.name):
            run_cmd(cmd="rm -f " + class_instance.temp_tiller_yaml.name)

    request.addfinalizer(finalizer)
    os.environ["TILLER_NAMESPACE"] = ""
    ocp_tiller_proj = ocp.OCP(kind="Project", namespace=class_instance.tiller_namespace)
    log.info(f"Creating a new project '{class_instance.tiller_namespace}'")
    assert ocp_tiller_proj.new_project(
        class_instance.tiller_namespace
    ), f"Failed to create project {class_instance.tiller_namespace}"
    os.environ["TILLER_NAMESPACE"] = class_instance.tiller_namespace

    os.chdir(class_instance.helm_dir)
    log.info(f"Fetching helm chart from {class_instance.helm_url}")
    curl_helm_cmd = f"wget {class_instance.helm_url}"
    exec_cmd(cmd=curl_helm_cmd)

    curl_helm_extract_cmd = f"tar -xzvf {class_instance.helm_tar_file}"
    exec_cmd(cmd=curl_helm_extract_cmd)
    os.chdir("linux-amd64")

    log.info("Installing helm chart")
    helm_cmd = "./helm init --client-only"
    exec_cmd(cmd=helm_cmd)

    log.info(f"Creating project {class_instance.tiller_namespace}")
    ocp_proj = ocp.OCP()
    output = ocp_proj.exec_oc_cmd(
        command=f"process -f {class_instance.tiller_template_url} "
        f"-p TILLER_NAMESPACE={class_instance.tiller_namespace} "
        f"-p HELM_VERSION={class_instance.helm_version}",
        out_yaml_format=True,
    )
    templating.dump_data_to_temp_yaml(output, class_instance.temp_tiller_yaml.name)
    ocp_proj.exec_oc_cmd(command=f"create -f {class_instance.temp_tiller_yaml.name}")
    ocp_proj.exec_oc_cmd(
        command=f"rollout status deployment {class_instance.tiller_namespace}",
        out_yaml_format=False,
    )


@pytest.fixture()
def create_db2u_project(request):
    """
    Creates 'db2u-project' for the workload as well as a new service account for tiller
    """
    class_instance = request.node.cls

    def finalizer():
        ocp_project = ocp.OCP(kind=constants.NAMESPACE)
        if ocp_project.is_exist(resource_name=class_instance.db2u_project):
            ocp_project.delete_project(project_name=class_instance.db2u_project)
            ocp_project.wait_for_delete(resource_name=class_instance.db2u_project)

    request.addfinalizer(finalizer)

    ocp_proj = ocp.OCP(kind="Project", namespace=class_instance.db2u_project)
    log.info(f"Creating a new project '{class_instance.db2u_project}'")
    assert ocp_proj.new_project(
        class_instance.db2u_project
    ), f"Failed to create project {class_instance.db2u_project}"

    ocp_proj.exec_oc_cmd(
        command=f"policy add-role-to-user edit system:serviceaccount:{class_instance.tiller_namespace}:tiller"
    )


@pytest.fixture()
def create_security_context_constraints(request):
    """
    Creating New Security Context Constraints
    """
    class_instance = request.node.cls

    def finalizer():
        ocp_project = ocp.OCP()
        ocp_project.exec_oc_cmd(command=f"delete scc {class_instance.db2wh_scc}")

        if os.path.isdir(class_instance.helm_dir):
            run_cmd(cmd="rm -rf " + class_instance.helm_dir)
        if os.path.isfile(class_instance.temp_scc_yaml.name):
            run_cmd(cmd="rm -f " + class_instance.temp_scc_yaml.name)

    request.addfinalizer(finalizer)

    log.info("Creating Security Context Constraints")
    ocp_proj = ocp.OCP(namespace=class_instance.db2u_project)
    template_yaml_dict = templating.load_yaml(constants.IBM_BDI_SCC_WORKLOAD_YAML)
    class_instance.temp_scc_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="scc_yaml_", delete=False
    )
    templating.dump_data_to_temp_yaml(
        template_yaml_dict, class_instance.temp_scc_yaml.name
    )
    ocp_proj.exec_oc_cmd(command=f"create -f {class_instance.temp_scc_yaml.name}")


@pytest.fixture()
def security_prereqs(request):
    """
    Create security privileges for chart deployment
    """
    class_instance = request.node.cls

    log.info(f"Creating secrets on project {class_instance.db2u_project}")
    os.chdir(class_instance.bdi_dir + class_instance.chart_dir)
    cluster_prereqs_cmd = (
        f"./pre-install/clusterAdministration/createSecurityClusterPrereqs.sh"
    )
    exec_cmd(cmd=cluster_prereqs_cmd)
    namespace_prereqs_cmd = (
        f"./pre-install/namespaceAdministration/createSecurityNamespacePrereqs.sh "
        f"{class_instance.db2u_project}"
    )
    exec_cmd(cmd=namespace_prereqs_cmd)


@pytest.fixture()
def create_secretes(request):
    """
    Create DB2 & LDAP secrets
    """
    class_instance = request.node.cls
    ocp_proj = ocp.OCP(namespace=class_instance.db2u_project)

    def finalizer():
        ocp_proj.exec_oc_cmd(
            command=f"delete -n {class_instance.db2u_project} "
            f"secret/{class_instance.ldap_r_n}-db2u-ldap-bluadmin "
            f"secret/{class_instance.db2u_r_n}-db2u-instance"
        )

    request.addfinalizer(finalizer)

    log.info("Creating LDAP secrets")
    ocp_proj.exec_oc_cmd(
        command=f"create secret generic {class_instance.ldap_r_n}-db2u-ldap-bluadmin "
        f"--from-literal=password={class_instance.ldap_r_p}"
    )

    log.info("Creating DB2U secrets")
    ocp_proj.exec_oc_cmd(
        command=f"create secret generic {class_instance.db2u_r_n}-db2u-instance "
        f"--from-literal=password={class_instance.db2u_r_p}"
    )


@pytest.fixture()
def ibm_container_registry(request):
    """
    Register with IBM could in order to get the DB2 Warehouse container  images
    """
    class_instance = request.node.cls

    ocp_proj = ocp.OCP(namespace=class_instance.db2u_project)

    log.info("Registering with IBM could for container images")
    ocp_proj.exec_oc_cmd(
        command=f"create secret docker-registry ibm-registry --docker-server=icr.io "
        f"--docker-username=iamapikey --docker-password={class_instance.ibm_cloud_key}"
    )

    ocp_proj.exec_oc_cmd(command="secrets link db2u ibm-registry --for=pull")
