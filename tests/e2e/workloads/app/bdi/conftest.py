import pytest
import os
import logging
import tempfile
from ocs_ci.utility.utils import clone_repo, exec_cmd
from ocs_ci.ocs import machine, node, ocp, constants
from ocs_ci.utility import templating
from ocs_ci.framework import config

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def clone_ibm_chart_project_factory_class(request):
    return clone_ibm_chart_project_factory_fixture(request)


@pytest.fixture(scope="session")
def clone_ibm_chart_project_factory_session(request):
    return clone_ibm_chart_project_factory_fixture(request)


@pytest.fixture(scope="function")
def clone_ibm_chart_project_factory(request):
    return clone_ibm_chart_project_factory_fixture(request)


def clone_ibm_chart_project_factory_fixture(request):
    """
    Clone IBM Chart project from Github.
    Calling this fixture will clone IBM chart project in github
    """
    bdi_dir = []

    def factory(destination_dir, git_url):
        """
        Args:
            destination_dir (str): Destination directory for cloned project.
            git_url (str): Project's URL.
        """
        bdi_dir.append(destination_dir)
        log.info(f"Cloning chart from github into {destination_dir}")
        clone_repo(url=git_url, location=destination_dir)

    def finalizer():
        """
        Delete bdi temporary directory
        """
        log.info(f"Deleting directory {bdi_dir[0]}")
        # if os.path.isdir(class_instance.bdi_dir[0]):
        exec_cmd(cmd=f"rm -rf {bdi_dir[0]}")

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def create_machineset_factory_class(request):
    return create_machineset_factory_fixture(request)


@pytest.fixture(scope="session")
def create_machineset_factory_session(request):
    return create_machineset_factory_fixture(request)


@pytest.fixture(scope="function")
def create_machineset_factory(request):
    return create_machineset_factory_fixture(request)


def create_machineset_factory_fixture(request):
    """
    Creates a machineset.
    Additional worker nodes are needed in order to support the workload execution (3 default workers is not enough)
    Relevant only for IPI deployment.
    If we want to use UPI deployment, we need to create the additional workers during the post deployment precess
    """

    machineset_name = []

    def factory(additional_nodes=3):
        """
        Args:
            additional_nodes (int): Number of additional nodes to be added (default=3).
        """
        log.info("Creating machineset")
        machineset_name.append(
            machine.create_custom_machineset(instance_type="m5.4xlarge", zone="a")
        )
        machine.wait_for_new_node_to_be_ready(machineset_name[0])
        log.info(
            f"Adding {additional_nodes} more nodes to machineset {machineset_name[0]}"
        )
        node.add_new_node_and_label_it(
            machineset_name=machineset_name[0],
            num_nodes=additional_nodes,
            mark_for_ocs_label=False,
        )
        machine.wait_for_new_node_to_be_ready(machineset_name[0])

    def finalizer():
        """
        Delete machineset
        """
        if config.ENV_DATA["deployment_type"].lower() == "ipi":
            if machineset_name[0] is not None and machine.check_machineset_exists(
                machine_set=machineset_name[0]
            ):
                log.info(f"Deleting machineset {machineset_name[0]}")
                machine.delete_custom_machineset(machineset_name[0])

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def install_helm_factory_class(request):
    return install_helm_factory_fixture(request)


@pytest.fixture(scope="session")
def install_helm_factory_session(request):
    return install_helm_factory_fixture(request)


@pytest.fixture(scope="function")
def install_helm_factory(request):
    return install_helm_factory_fixture(request)


def install_helm_factory_fixture(request):
    """
    Create 'tiller' project and install Helm client
    """

    helm_dir = tempfile.mkdtemp(prefix="helm_dir_")
    helm_tar_file = "helm-v2.17.0-linux-amd64.tar.gz"
    helm_url = "https://get.helm.sh/" + helm_tar_file
    tiller_namespace = "tiller"

    def factory():

        os.environ["TILLER_NAMESPACE"] = ""
        ocp_tiller_proj = ocp.OCP(kind="Project", namespace=tiller_namespace)
        log.info(f"Creating a new project '{tiller_namespace}'")
        assert ocp_tiller_proj.new_project(
            tiller_namespace
        ), f"Failed to create project {tiller_namespace}"
        os.environ["TILLER_NAMESPACE"] = tiller_namespace

        create_sa_cmd = (
            f"kubectl create serviceaccount --namespace {tiller_namespace} tiller"
        )
        exec_cmd(cmd=create_sa_cmd)

        create_crb_cmd = (
            f"kubectl create clusterrolebinding tiller-cluster-rule "
            f"--clusterrole=cluster-admin --serviceaccount={tiller_namespace}:tiller"
        )
        exec_cmd(cmd=create_crb_cmd)

        os.chdir(helm_dir)
        log.info(f"Fetching helm chart from {helm_url}")
        curl_helm_cmd = f"wget {helm_url}"
        exec_cmd(cmd=curl_helm_cmd)

        curl_helm_extract_cmd = f"tar -xzvf {helm_tar_file}"
        exec_cmd(cmd=curl_helm_extract_cmd)
        os.chdir("linux-amd64")

        copy_helm_binary_cmd = "sudo cp helm /usr/local/bin/helm"

        exec_cmd(cmd=copy_helm_binary_cmd)

        log.info("Installing helm chart")
        helm_cmd = (
            f"./helm init --stable-repo-url https://charts.helm.sh/stable "
            f"--tiller-namespace {tiller_namespace} --service-account tiller"
        )
        exec_cmd(cmd=helm_cmd)

        ocp_proj = ocp.OCP()
        ocp_proj.exec_oc_cmd(
            command="rollout status deployment tiller-deploy",
            out_yaml_format=False,
        )

    def finalizer():
        """
        Delete 'tiller' project and temporary files
        """
        ocp_project = ocp.OCP(kind="Project", namespace=tiller_namespace)
        ocp.switch_to_project("openshift-storage")
        log.info(f"Deleting project {tiller_namespace}")
        ocp_project.delete_project(project_name=tiller_namespace)
        ocp_project.wait_for_delete(resource_name=tiller_namespace)

        if os.path.isdir(helm_dir):
            exec_cmd(cmd="rm -rf " + helm_dir)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def create_db2u_project_factory_class(request):
    return create_db2u_project_factory_fixture(request)


@pytest.fixture(scope="session")
def create_db2u_project_factory_session(request):
    return create_db2u_project_factory_fixture(request)


@pytest.fixture(scope="function")
def create_db2u_project_factory(request):
    return create_db2u_project_factory_fixture(request)


def create_db2u_project_factory_fixture(request):
    """
    Creates 'db2u-project' for the workload as well as a new service account for tiller
    """

    db2u_project = []

    def factory(db2u_project_name):
        """
        Args:
            db2u_project_name (str): Name of the db2u project to be created.
        """
        db2u_project.append(db2u_project_name)
        ocp_proj = ocp.OCP(kind="Project", namespace=db2u_project_name)
        log.info(f"Creating a new project '{db2u_project_name}'")
        assert ocp_proj.new_project(
            db2u_project_name
        ), f"Failed to create project {db2u_project_name}"

        ocp_proj.exec_oc_cmd(
            command="policy add-role-to-user edit system:serviceaccount:tiller:tiller"
        )

    def finalizer():
        """
        Delete project
        """
        ocp_project = ocp.OCP(kind=constants.NAMESPACE)
        if ocp_project.is_exist(db2u_project[0]):
            ocp_project.delete_project(project_name=db2u_project[0])
            ocp_project.wait_for_delete(resource_name=db2u_project[0], timeout=180)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def create_scc_factory_class(request):
    return create_scc_factory_fixture(request)


@pytest.fixture(scope="session")
def create_scc_factory_session(request):
    return create_scc_factory_fixture(request)


@pytest.fixture(scope="function")
def create_scc_factory(request):
    return create_scc_factory_fixture(request)


def create_scc_factory_fixture(request):
    """
    Creating ew Security Context Constraints (SCC)
    """

    temp_scc_yaml = []

    def factory(db2u_project_name):
        """
        Args:
            db2u_project_name (str): Name of the db2u project to be created.
        """
        log.info("Creating Security Context Constraints")
        ocp_proj = ocp.OCP(namespace=db2u_project_name)
        template_yaml_dict = templating.load_yaml(constants.IBM_BDI_SCC_WORKLOAD_YAML)
        temp_scc_yaml.append(
            tempfile.NamedTemporaryFile(mode="w+", prefix="scc_yaml_", delete=False)
        )
        templating.dump_data_to_temp_yaml(template_yaml_dict, temp_scc_yaml[0].name)
        ocp_proj.exec_oc_cmd(command=f"create -f {temp_scc_yaml[0].name}")

    def finalizer():
        """
        Delete Security Context Constraints
        """
        ocp_project = ocp.OCP()
        ocp_project.exec_oc_cmd(command="delete scc db2wh-scc")

        if os.path.isfile(temp_scc_yaml[0].name):
            exec_cmd(cmd="rm -f " + temp_scc_yaml[0].name)

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def create_security_prereqs_factory_class(request):
    return create_security_prereqs_factory_fixture(request)


@pytest.fixture(scope="session")
def create_security_prereqs_factory_session(request):
    return create_security_prereqs_factory_fixture(request)


@pytest.fixture(scope="function")
def create_security_prereqs_factory(request):
    return create_security_prereqs_factory_fixture(request)


def create_security_prereqs_factory_fixture(request):
    """
    Creating security prerequisites
    """

    def factory(db2u_project_name, bdi_dir, chart_dir):
        """
        Args:
            db2u_project_name (str): Name of the db2u project.
            bdi_dir (str): Bdi directory contains the DB2U project from git.
            chart_dir (str): Path to chart directory within the bdi directory.
        """
        log.info(f"Creating secrets on project {db2u_project_name}")
        os.chdir(bdi_dir + chart_dir)
        cluster_prereqs_cmd = (
            "./pre-install/clusterAdministration/createSecurityClusterPrereqs.sh"
        )
        exec_cmd(cmd=cluster_prereqs_cmd)
        namespace_prereqs_cmd = (
            f"./pre-install/namespaceAdministration/createSecurityNamespacePrereqs.sh "
            f"{db2u_project_name}"
        )
        exec_cmd(cmd=namespace_prereqs_cmd)

    return factory


@pytest.fixture(scope="class")
def create_secretes_factory_class(request):
    return create_secretes_factory_fixture(request)


@pytest.fixture(scope="session")
def create_secretes_factory_session(request):
    return create_secretes_factory_fixture(request)


@pytest.fixture(scope="function")
def create_secretes_factory(request):
    return create_secretes_factory_fixture(request)


def create_secretes_factory_fixture(request):
    """
    Create DB2U & LDAP secrets
    """
    temp_ldap_r_n = []
    temp_db2u_r_n = []
    ocp_proj = []
    db2u_project = []

    def factory(db2u_project_name, ldap_r_n, ldap_r_p, db2u_r_n, db2u_r_p):
        """
        Args:
            db2u_project_name (str): Name of the db2u project.
            ldap_r_n (str): LDAP release name.
            ldap_r_p (str): LDAP release password.
            db2u_r_n (str): DB2U release name
            db2u_r_p (str): DB2U release name
        """
        db2u_project.append(db2u_project_name)
        ocp_proj.append(ocp.OCP(namespace=db2u_project_name))
        temp_ldap_r_n.append(ldap_r_n)
        temp_db2u_r_n.append(db2u_r_n)

        log.info("Creating LDAP secrets")
        ocp_proj[0].exec_oc_cmd(
            command=f"create secret generic {ldap_r_n}-db2u-ldap-bluadmin "
            f"--from-literal=password={ldap_r_p}"
        )

        log.info("Creating DB2U secrets")
        ocp_proj[0].exec_oc_cmd(
            command=f"create secret generic {db2u_r_n}-db2u-instance "
            f"--from-literal=password={db2u_r_p}"
        )

    def finalizer():
        """
        Delete secrets
        """
        ocp_proj[0].exec_oc_cmd(
            command=f"delete -n {db2u_project[0]} "
            f"secret/{temp_ldap_r_n[0]}-db2u-ldap-bluadmin "
            f"secret/{temp_db2u_r_n[0]}-db2u-instance"
        )

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture(scope="class")
def create_ibm_container_registry_factory_class(request):
    return create_ibm_container_registry_factory_fixture(request)


@pytest.fixture(scope="session")
def create_ibm_container_registry_factory_session(request):
    return create_ibm_container_registry_factory_fixture(request)


@pytest.fixture(scope="function")
def create_ibm_container_registry_factory(request):
    return create_ibm_container_registry_factory_fixture(request)


def create_ibm_container_registry_factory_fixture(request):
    """
    Register with IBM could in order to get the DB2 Warehouse container images

    """

    def factory(db2u_project_name, ibm_cloud_key):
        """
        Args:
            db2u_project_name (str): Name of the db2u project.
            ibm_cloud_key (str): IBM cloud key for image pulling

        """

        ocp_proj = ocp.OCP(namespace=db2u_project_name)

        log.info("Registering with IBM could for container images")
        ocp_proj.exec_oc_cmd(
            command=f"create secret docker-registry ibm-registry --docker-server=icr.io "
            f"--docker-username=iamapikey --docker-password={ibm_cloud_key}",
            secrets=[ibm_cloud_key],
        )

        ocp_proj.exec_oc_cmd(command="secrets link db2u ibm-registry --for=pull")

    return factory
