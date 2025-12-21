import logging
import base64
import os
import re
import time

from ocs_ci.ocs import constants, ocp, node
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.framework import config
from ocs_ci.ocs.utils import mirror_image, get_pod_name_by_pattern


logger = logging.getLogger(__name__)


def change_registry_backend_to_ocs():
    """
    Function to deploy registry with OCS backend. By default, the image registry may use ephemeral storage,
    so we overwrite this to utilize ODF(OCS) storage.

    Raises:
        AssertionError: When failure in change of registry backend to OCS

    """
    sc = helpers.default_storage_class(interface_type=constants.CEPHFILESYSTEM)
    pv_obj = helpers.create_pvc(
        sc_name=sc.name,
        pvc_name="registry-cephfs-rwx-pvc",
        namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
        size="100Gi",
        access_mode=constants.ACCESS_MODE_RWX,
    )
    helpers.wait_for_resource_state(pv_obj, "Bound", timeout=300)
    param_cmd = f'[{{"op": "add", "path": "/spec/storage", "value": {{"pvc": {{"claim": "{pv_obj.name}"}}}}}}]'

    run_cmd(
        f"oc patch {constants.IMAGE_REGISTRY_CONFIG} -p " f"'{param_cmd}' --type json"
    )

    # Validate registry pod status
    retry((CommandFailed, UnexpectedBehaviour), tries=3, delay=15)(
        validate_registry_pod_status
    )()

    # Validate pvc mount in the registry pod
    retry((CommandFailed, UnexpectedBehaviour, AssertionError), tries=3, delay=15)(
        validate_pvc_mount_on_registry_pod
    )()


def get_registry_pod_obj():
    """
    Function to get registry pod obj

    Returns:
        pod_obj (list): List of Registry pod objs

    Raises:
        UnexpectedBehaviour: When image-registry pod is not present.

    """
    # Sometimes when there is a update in config crd, there will be 2 registry pods
    # i.e. old pod will be terminated and new pod will be up based on new crd
    # so below loop waits till old pod terminates

    registry_deployment = ocp.OCP(
        kind="deployment",
        namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
        resource_name=constants.OPENSHIFT_IMAGE_REGISTRY_DEPLOYMENT,
    )
    replicas = registry_deployment.data["spec"].get("replicas", 1)
    registry_pods = ocp.OCP(
        kind="pod",
        namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
        selector=constants.OPENSHIFT_IMAGE_SELECTOR,
    )
    registry_pods.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        timeout=400,
        resource_count=replicas,
        dont_allow_other_resources=True,
    )
    pod_objs = [pod.Pod(**data) for data in registry_pods.data["items"]]
    pod_objs_len = len(pod_objs)
    if pod_objs_len == 0:
        raise UnexpectedBehaviour("No image-registry pod is present!")
    elif pod_objs_len != replicas:
        raise UnexpectedBehaviour(
            f"Expected {replicas} image-registry pod(s), but {pod_objs_len} " f"found!"
        )
    return pod_objs


def get_oc_podman_login_cmd(skip_tls_verify=True):
    """
    Function to get oc and podman login commands on node

    Args:
        skip_tls_verify (bool): If true, the server's certificate will not be checked for validity

    Returns:
        cmd_list (list): List of cmd for oc/podman login

    """
    user = config.RUN["username"]
    filename = os.path.join(
        config.ENV_DATA["cluster_path"], config.RUN["password_location"]
    )
    with open(filename) as f:
        password = f.read().strip()
    cluster_name = config.ENV_DATA["cluster_name"]
    base_domain = config.ENV_DATA["base_domain"]
    cmd_list = [
        "export KUBECONFIG=/home/core/auth/kubeconfig",
        f"oc login -u {user} -p {password} "
        f"https://api-int.{cluster_name}.{base_domain}:6443"
        f" --insecure-skip-tls-verify={skip_tls_verify}",
        f"podman login -u {user} -p $(oc whoami -t) image-registry.openshift-image-registry.svc:5000",
    ]
    master_list = node.get_master_nodes()
    helpers.rsync_kubeconf_to_node(node=master_list[0])
    return cmd_list


def validate_pvc_mount_on_registry_pod():
    """
    Function to validate pvc mounted on the registry pod

    Raises:
        AssertionError: When PVC mount not present in the registry pod

    """
    pod_objs = get_registry_pod_obj()
    for pod_obj in pod_objs:
        mount_point = pod_obj.exec_cmd_on_pod(
            command="mount",
            out_yaml_format=False,
        )
        assert "/registry" in mount_point, f"pvc is not mounted on pod {pod_obj.name}"
        logger.info(f"Verified pvc is mounted on {pod_obj.name} pod")


def validate_registry_pod_status():
    """
    Function to validate registry pod status
    """
    pod_objs = get_registry_pod_obj()
    for pod_obj in pod_objs:
        helpers.wait_for_resource_state(pod_obj, state=constants.STATUS_RUNNING)


def get_default_route_name():
    """
    Function to get default route name

    Returns:
        route_name (str): Returns default route name

    """
    ocp_obj = ocp.OCP()
    route_cmd = f"get route -n {constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE} -o yaml"
    route_dict = ocp_obj.exec_oc_cmd(command=route_cmd)
    return route_dict.get("items")[0].get("spec").get("host")


def add_role_to_user(role_type, user, cluster_role=False, namespace=None):
    """
    Function to add a cluster/regular role to user

    Args:
        role_type (str): Type of the role to be added
        user (str): User to be added for the role
        cluster_role (bool): Whether to add a cluster-role or a regular role
        namespace (str): Namespace to be used

    Raises:
        AssertionError: When failure in adding new role to user

    """
    ocp_obj = ocp.OCP()
    cluster = "cluster-" if cluster_role else ""
    namespace = f"-n {namespace}" if namespace else ""
    role_cmd = f"adm policy add-{cluster}role-to-user {role_type} {user} {namespace}"
    assert ocp_obj.exec_oc_cmd(command=role_cmd), "Adding role failed"
    logger.info(f"Role_type {role_type} added to the user {user}")


def remove_role_from_user(role_type, user, cluster_role=False, namespace=None):
    """
    Function to remove a cluster/regular role from a user

    Args:
        role_type (str): Type of the role to be removed
        user (str): User of the role
        cluster_role (bool): Whether to remove a cluster-role or a regular role
        namespace (str): Namespace to be used

    Raises:
        AssertionError: When failure in removing role from user

    """
    ocp_obj = ocp.OCP()
    cluster = "cluster-" if cluster_role else ""
    namespace = f"-n {namespace}" if namespace else ""
    role_cmd = (
        f"adm policy remove-{cluster}role-from-user {role_type} {user} {namespace}"
    )
    assert ocp_obj.exec_oc_cmd(command=role_cmd), "Removing role failed"
    logger.info(f"Role_type {role_type} removed from user {user}")


def enable_route_and_create_ca_for_registry_access():
    """
    Function to enable route and to create ca,
    copy to respective location for registry access

    Raises:
        AssertionError: When failure in enabling registry default route

    """
    ocp_obj = ocp.OCP(
        kind=constants.CONFIG, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )
    assert ocp_obj.patch(
        resource_name=constants.IMAGE_REGISTRY_RESOURCE_NAME,
        params='{"spec": {"defaultRoute": true}}',
        format_type="merge",
    ), "Registry pod defaultRoute enable is not success"
    logger.info("Enabled defaultRoute to true")
    ocp_obj = ocp.OCP()
    crt_cmd = (
        f"get secret {constants.DEFAULT_ROUTE_CRT} "
        f"-n {constants.OPENSHIFT_INGRESS_NAMESPACE} -o yaml"
    )
    crt_dict = ocp_obj.exec_oc_cmd(command=crt_cmd)
    crt = crt_dict.get("data").get("tls.crt")
    route = get_default_route_name()
    if not os.path.exists("/tmp/secret"):
        run_cmd(cmd="mkdir /tmp/secret")
    with open(f"/tmp/secret/{route}.crt", "wb") as temp:
        temp.write(base64.b64decode(crt))
    master_list = node.get_master_nodes()
    ocp.rsync(
        src="/tmp/secret/",
        dst="/etc/pki/ca-trust/source/anchors",
        node=master_list[0],
        dst_node=True,
    )
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=["update-ca-trust enable"])
    logger.info("Created base64 secret, copied to source location and enabled ca-trust")


def image_pull(image_url):
    """
    Function to pull images from repositories

    Args:
        image_url (str): Image url container image repo link

    """
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append(f"podman pull {image_url}")
    master_list = node.get_master_nodes()
    ocp_obj = ocp.OCP()
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)


def image_push(image_url, namespace):
    """
    Function to push images to destination

    Args:
        image_url (str): Image url container image repo link
        namespace (str): Image to be uploaded namespace

    Returns:
        registry_path (str): Uploaded image path

    """
    image = image_url.split("/")[-1]
    image_path = f"image-registry.openshift-image-registry.svc:5000/{namespace}/{image}"
    tag_cmd = f"podman tag {image_url} {image_path}"
    push_cmd = f"podman push {image_path}"
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append(tag_cmd)
    cmd_list.append(push_cmd)
    master_list = node.get_master_nodes()
    ocp_obj = ocp.OCP()
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)
    logger.info(f"Pushed {image_path} to registry")
    image_list_all()
    return image_path


def image_list_all():
    """
    Function to list the images in the podman registry

    Returns:
        image_list_output (str): Images present in cluster

    """
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append("podman image list --format json")
    master_list = node.get_master_nodes()
    ocp_obj = ocp.OCP()
    return ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)


def image_rm(registry_path, image_url):
    """
    Function to remove images from registry

    Args:
        registry_path (str): Image registry path
        image_url (str): Image url container image repo link

    """
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append(f"podman rmi {registry_path}")
    cmd_list.append(f"podman rmi {image_url}")
    master_list = node.get_master_nodes()
    ocp_obj = ocp.OCP()
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)
    logger.info(f"Image {registry_path} rm successful")


def check_image_exists_in_registry(image_url):
    """
    Function to check either image exists in registry or not

    Args:
        image_url (str): Image url to be verified

    Returns:
        bool: True if image exists, else False

    """
    output = image_list_all()
    output = output.split("\n")
    if not any(image_url in i for i in output):
        return_value = False
        logger.error("Image url not exists in Registry")
    else:
        return_value = True
        logger.info("Image exists in Registry")
    return return_value


def image_pull_and_push(
    project_name,
    template="redis-ephemeral",
    image="registry.redhat.io/rhel8/redis",
    pattern="",
    wait=True,
):
    """
    Pull and push images running oc new-app command
    Args:
        project_name (str): Name of project
        template (str): Name of the template of the image
        image (str): Name of the image with tag
        pattern (str): name of the build with given pattern
        wait (bool): If true waits till the image pull and push completes.
    """
    ocp_obj = ocp.OCP(kind="template", namespace="openshift")
    try:
        ocp_obj.get(resource_name=template)
    except CommandFailed as cfe:
        if f'"{template}" not found' in str(cfe):
            logger.warn(f"Template {template} not found")
            template = "redis-ephemeral"
            image = "registry.redhat.io/rhel8/redis"
        else:
            raise

    if config.DEPLOYMENT.get("disconnected"):
        mirror_image(image=image)
    else:
        cmd = f"new-app --template={template} -n {project_name} --name=build1"
        ocp_obj = ocp.OCP()
        ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)

        # Validate it completed
        if wait:
            if template == "redis-ephemeral":
                wait_time = 180
                logger.info(f"Wait for {wait_time} seconds for deployment to come up")
                time.sleep(180)
                ocp_obj = ocp.OCP(kind=constants.POD, namespace=project_name)
                deploy_pod_name = get_pod_name_by_pattern(
                    pattern="deploy", namespace=project_name
                )
                ocp_obj.wait_for_resource(
                    condition=constants.STATUS_COMPLETED,
                    resource_name=deploy_pod_name[0],
                )
            else:
                wait_time = 300
                logger.info(f"Wait for {wait_time} seconds for build to come up")
                time.sleep(300)
                build_list = get_build_name_by_pattern(
                    pattern=pattern, namespace=project_name
                )
                if build_list is None:
                    raise Exception("Build is not created")
                build_obj = ocp.OCP(kind="Build", namespace=project_name)
                for build in build_list:
                    build_obj.wait_for_resource(
                        condition="Complete", resource_name=build, timeout=900
                    )


def get_build_name_by_pattern(pattern="", namespace=None):
    """
    In a given namespace find names of the builds that match
    the given pattern

    Args:
        pattern (str): name of the build with given pattern
        namespace (str): Namespace value

    Returns:
        build_list (list): List of build names matching the pattern

    """
    namespace = namespace if namespace else config.ENV_DATA["cluster_namespace"]
    ocp_obj = ocp.OCP(kind="Build", namespace=namespace)
    build_names = ocp_obj.exec_oc_cmd("get builds -o name", out_yaml_format=False)
    build_names = build_names.split("\n")
    build_list = []
    for name in build_names:
        if re.search(pattern, name):
            _, name = name.split("/")
            logger.info(f"pod name match found appending {name}")
            build_list.append(name)
    return build_list


def validate_image_exists(app="redis"):
    """
    Validate image exists on registries path
    Args:
        app (str): Label or application name

    Returns:
        image_list (str): Dir/Files/Images are listed in string format

    Raises:
        Exceptions if dir/folders not found
    """
    if not config.DEPLOYMENT.get("disconnected"):
        pod_list = get_pod_name_by_pattern(
            pattern="image-registry",
            namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
        )
        for pod_name in pod_list:
            if "cluster" not in pod_name:
                pod_obj = pod.get_pod_obj(
                    name=pod_name,
                    namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
                )

                return pod_obj.exec_cmd_on_pod(
                    command=f"find /registry/docker/registry/v2/repositories/openshift/{app}"
                )


def modify_registry_pod_count(count):
    """
    Function to modify registry replica count(increase/decrease pod count)

    Args:
        count (int): registry replica count to be changed to

    Returns:
        bool: True in case if changes are applied. False otherwise

    Raises:
        TimeoutExpiredError: When number of image registry pods doesn't match the count

    """
    params = '{"spec":{"replicas":%d}}' % count
    ocp_obj = ocp.OCP(
        kind=constants.IMAGE_REGISTRY_CONFIG,
        namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
    )
    ocp_obj.patch(params=params, format_type="merge"), (
        "Failed to run patch command to increase number of image registry pod"
    )

    # Validate number of image registry pod should match the count
    for pod_list in TimeoutSampler(
        300,
        10,
        get_pod_name_by_pattern,
        "image-registry",
        constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
    ):
        try:
            if pod_list is not None and len(pod_list) == count + 1:
                return True
        except IndexError as ie:
            logger.error(
                f"Number of image registry pod doesn't match the count. Error: {ie}"
            )
            return False


def check_if_registry_stack_exists():
    """
    Check if registry is configured on the cluster with ODF backed PVCs

    Returns:
        bool: True if registry is configured on the cluster, false otherwise

    """
    logger.info("Checking if registry stack exists on the cluster")
    reg_obj = ocp.OCP(
        kind=constants.CONFIG, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )
    config_dict = reg_obj.exec_oc_cmd(f"get {constants.IMAGE_REGISTRY_CONFIG} -o yaml ")
    if "emptyDir" in config_dict.get("spec").get("storage"):
        logger.info("Registry stack is not configured on the cluster")
        return False
    else:
        logger.info("Registry stack already exists on the cluster")
        return True
