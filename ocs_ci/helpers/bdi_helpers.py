"""
Helper functions specific for bdi
"""
import os
import logging
import ocs_ci.ocs.bdi.config as bdi_config
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


def init_configure_workload_yaml(namespace=None, image=None, sf=1, pvc_name=None):
    """
    Loads the “configure-workload.yaml” and sets all the relevant parameters according to the type of execution

    Args:
        namespace (str): The name of the namespace
        image (str): The name of the image to pull
        sf (int): scale factor. Can be 1, 10, 100...
        pvc_name (str): The name of the pvc to be used

    Returns:
        dict: The dictionary representing the yaml with all the relevant changes

    """
    temp_yaml = templating.load_yaml(constants.IBM_BDI_CONFIGURE_WORKLOAD_YAML)
    temp_yaml["metadata"]["namespace"] = namespace
    temp_yaml["spec"]["template"]["spec"]["initContainers"][0]["image"] = image
    temp_yaml["spec"]["template"]["spec"]["initContainers"][0]["env"][2]["value"] = str(
        sf
    )
    temp_yaml["spec"]["template"]["spec"]["containers"][0]["image"] = image
    temp_yaml["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
        "claimName"
    ] = pvc_name

    return temp_yaml


def init_data_load_yaml(namespace=None, image=None, pvc_name=None, secret_name=None):
    """
    Loads the “data-load-job” and sets all the relevant parameters according to the type of execution

    Args:
        namespace (str): The name of the namespace
        image (str): The name of the image to pull
        pvc_name (str): The name of the pvc to be used
        secret_name (str): The secret name to be used

    Returns:
        dict: The dictionary representing the yaml with all the relevant changes

    """
    temp_yaml = templating.load_yaml(constants.IBM_BDI_DATA_LOAD_WORKLOAD_YAML)

    temp_yaml["metadata"]["namespace"] = namespace
    temp_yaml["spec"]["template"]["spec"]["initContainers"][0]["image"] = image
    temp_yaml["spec"]["template"]["spec"]["containers"][0]["image"] = image
    temp_yaml["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
        "claimName"
    ] = pvc_name
    temp_yaml["spec"]["template"]["spec"]["volumes"][1]["secret"][
        "secretName"
    ] = secret_name

    return temp_yaml


def init_run_workload_yaml(namespace=None, image=None, pvc_name=None, secret_name=None):
    """
    Loads the “run-workload-job” and sets all the relevant parameters according to the type of execution

    Args:
        namespace (str): The name of the namespace
        image (str): The name of the image to pull
        pvc_name (str): The name of the pvc to be used
        secret_name (str): The secret name to be used

    Returns:
        dict: The dictionary representing the yaml with all the relevant changes

    """
    temp_yaml = templating.load_yaml(constants.IBM_BDI_RUN_WORKLOAD_YAML)
    temp_yaml["metadata"]["namespace"] = namespace
    temp_yaml["spec"]["template"]["spec"]["initContainers"][0]["image"] = image
    temp_yaml["spec"]["template"]["spec"]["containers"][0]["image"] = image
    temp_yaml["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
        "claimName"
    ] = pvc_name
    temp_yaml["spec"]["template"]["spec"]["volumes"][1]["secret"][
        "secretName"
    ] = secret_name

    return temp_yaml


def install_db2u(class_instance):
    """
    Chart installation

    """

    os.chdir(bdi_config.bdi_dir + bdi_config.chart_dir + "/common")

    with open("helm_options", "r") as file:
        data = file.read()
        data = data.replace("40Gi", bdi_config.pvc_size)
        with open(class_instance.temp_helm_options.name, "w") as f:
            f.write(data)

    log.info("Installing db2warehouse")
    install_cmd = (
        f"./db2u-install --db-type db2wh --namespace {bdi_config.db2u_project} "
        f"--release-name {bdi_config.db2u_r_n} --helm-opt-file "
        f"{class_instance.temp_helm_options.name} --accept-eula"
    )
    exec_cmd(cmd=install_cmd)

    log.info("Waiting for Pods to be in Running/Complete state")
    pod = ocp.OCP(kind=constants.POD, namespace=bdi_config.db2u_project)
    assert pod.wait_for_resource(
        condition="Running",
        selector="app=" + bdi_config.db2u_r_n,
        resource_count=6,
        timeout=600,
    )

    assert pod.wait_for_resource(
        condition="Completed",
        selector="app=" + bdi_config.db2u_r_n,
        resource_count=4,
        timeout=600,
    )


def clean_temp_files(class_instance):
    """
    Deletes temporary files created during the execution

    """
    log.info("Deleting temporary files")
    if os.path.isfile(bdi_config.temp_yaml_configure.name):
        exec_cmd(cmd=f"rm -f {bdi_config.temp_yaml_configure.name}")
    if os.path.isfile(bdi_config.temp_yaml_data.name):
        exec_cmd(cmd=f"rm -f {bdi_config.temp_yaml_data.name}")
    if os.path.isfile(bdi_config.temp_yaml_run.name):
        exec_cmd(cmd=f"rm -f {bdi_config.temp_yaml_run.name}")
        exec_cmd(cmd="rm -f tmp*")
    os.environ["TILLER_NAMESPACE"] = ""
