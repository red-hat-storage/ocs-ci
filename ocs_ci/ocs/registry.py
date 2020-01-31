import logging
import time
import base64
import os

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import run_cmd
from tests import helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


def change_registry_backend_to_ocs():
    """
    Function to deploy registry with OCS backend.

    Raises:
        AssertionError: When failure in change of registry backend to OCS

    """
    sc_name = f"{config.ENV_DATA['storage_cluster_name']}-{constants.DEFAULT_SC_CEPHFS}"
    pv_obj = helpers.create_pvc(
        sc_name=sc_name, pvc_name='registry-cephfs-rwx-pvc',
        namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE, size='100Gi',
        access_mode=constants.ACCESS_MODE_RWX
    )
    helpers.wait_for_resource_state(pv_obj, 'Bound')
    ocp_obj = ocp.OCP(
        kind=constants.CONFIG, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )
    param_cmd = f'[{{"op": "add", "path": "/spec/storage", "value": {{"pvc": {{"claim": "{pv_obj.name}"}}}}}}]'
    assert ocp_obj.patch(
        resource_name=constants.IMAGE_REGISTRY_RESOURCE_NAME, params=param_cmd, format_type='json'
    ), f"Registry pod storage backend to OCS is not success"

    # Validate registry pod status
    validate_registry_pod_status()

    # Validate pvc mount in the registry pod
    validate_pvc_mount_on_registry_pod()


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
    wait_time = 30
    for iteration in range(10):
        pod_data = pod.get_pods_having_label(
            label='docker-registry=default', namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
        )
        pod_obj = [pod.Pod(**data) for data in pod_data]
        if len(pod_obj) == 1:
            break
        elif len(pod_obj) == 0:
            raise UnexpectedBehaviour("Image-registry pod not present")
        elif iteration > 5:
            raise UnexpectedBehaviour("Waited for 3 mins Image-registry pod is not in Running state")
        else:
            logger.info(f"Waiting for 30 sec's for registry pod to be up iteration {iteration}")
            time.sleep(wait_time)
    return pod_obj


def get_oc_podman_login_cmd():
    """
    Function to get oc and podman login commands on node

    Returns:
        cmd_list (list): List of cmd for oc/podman login

    """
    user = config.RUN['username']
    helpers.refresh_oc_login_connection()
    ocp_obj = ocp.OCP()
    token = ocp_obj.get_user_token()
    route = get_default_route_name()
    cmd_list = [
        'export KUBECONFIG=/home/core/auth/kubeconfig',
        f"podman login {route} -u {user} -p {token}"
    ]
    master_list = helpers.get_master_nodes()
    helpers.rsync_kubeconf_to_node(node=master_list[0])
    return cmd_list


def validate_pvc_mount_on_registry_pod():
    """
    Function to validate pvc mounted on the registry pod

    Raises:
        AssertionError: When PVC mount not present in the registry pod

    """
    pod_obj = get_registry_pod_obj()
    mount_point = pod_obj[0].exec_cmd_on_pod(command="mount")
    assert "/registry" in mount_point, f"pvc is not mounted on pod {pod_obj.name}"
    logger.info("Verified pvc is mounted on image-registry pod")


def validate_registry_pod_status():
    """
    Function to validate registry pod status
    """
    pod_obj = get_registry_pod_obj()
    helpers.wait_for_resource_state(pod_obj[0], state=constants.STATUS_RUNNING)


def get_registry_pvc():
    """
    Function to get registry pvc

    Returns:
        pvc_name (str): Returns name of the OCS pvc backed for registry

    """
    pod_obj = get_registry_pod_obj()
    return pod.get_pvc_name(pod_obj)


def get_default_route_name():
    """
    Function to get default route name

    Returns:
        route_name (str): Returns default route name

    """
    ocp_obj = ocp.OCP()
    route_cmd = f"get route -n {constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE} -o yaml"
    route_dict = ocp_obj.exec_oc_cmd(command=route_cmd)
    return route_dict.get('items')[0].get('spec').get('host')


def add_role_to_user(role_type, user):
    """
    Function to add role to user

    Args:
        role_type (str): Type of the role to be added
        user (str): User to be added for the role

    Raises:
        AssertionError: When failure in adding new role to user

    """
    ocp_obj = ocp.OCP()
    role_cmd = f"policy add-role-to-user {role_type} {user} " \
               f"-n {constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE}"
    assert ocp_obj.exec_oc_cmd(command=role_cmd), 'Adding role failed'
    logger.info(f"Role_type {role_type} added to the user {user}")


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
        params='{"spec": {"defaultRoute": true}}', format_type='merge'
    ), f"Registry pod defaultRoute enable is not success"
    logger.info(f"Enabled defaultRoute to true")
    ocp_obj = ocp.OCP()
    crt_cmd = f"get secret {constants.DEFAULT_ROUTE_CRT} " \
              f"-n {constants.OPENSHIFT_INGRESS_NAMESPACE} -o yaml"
    crt_dict = ocp_obj.exec_oc_cmd(command=crt_cmd)
    crt = crt_dict.get('data').get('tls.crt')
    route = get_default_route_name()
    if not os.path.exists('/tmp/secret'):
        run_cmd(cmd='mkdir /tmp/secret')
    with open(f"/tmp/secret/{route}.crt", "wb") as temp:
        temp.write(base64.b64decode(crt))
    master_list = helpers.get_master_nodes()
    ocp.rsync(
        src=f"/tmp/secret/", dst='/etc/pki/ca-trust/source/anchors',
        node=master_list[0], dst_node=True
    )
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=['update-ca-trust enable'])
    logger.info(f"Created base64 secret, copied to source location and enabled ca-trust")


def image_pull(image_url):
    """
    Function to pull images from repositories

    Args:
        image_url (str): Image url container image repo link

    """
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append(f"podman pull {image_url}")
    master_list = helpers.get_master_nodes()
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
    cmd_list = get_oc_podman_login_cmd()
    route = get_default_route_name()
    split_image_url = image_url.split("/")
    tag_name = split_image_url[-1]
    img_path = f"{route}/{namespace}/{tag_name}"
    cmd_list.append(f"podman tag {image_url} {img_path}")
    cmd_list.append(f"podman push {img_path}")
    master_list = helpers.get_master_nodes()
    ocp_obj = ocp.OCP()
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)
    logger.info(f"Pushed {img_path} to registry")
    image_list_all()
    return img_path


def image_list_all():
    """
    Function to list the images in the podman registry

    Returns:
        image_list_output (str): Images present in cluster

    """
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append(f"podman image list --format json")
    master_list = helpers.get_master_nodes()
    ocp_obj = ocp.OCP()
    return ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)


def image_rm(registry_path):
    """
    Function to remove images from registry

    Args:
        registry_path (str): Image registry path

    """
    cmd_list = get_oc_podman_login_cmd()
    cmd_list.append(f"podman rm {registry_path}")
    master_list = helpers.get_master_nodes()
    ocp_obj = ocp.OCP()
    ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=cmd_list)
    logger.info(f"Image {registry_path} rm successful")


def check_image_in_registry(image_url):
    """
    Function to check either image present in registry or not

    Args:
        image_url (str): Image url to be verified

    Returns:
        True : Returns True if present

    """
    output = image_list_all()
    output = output.split("\n")
    if any(image_url in i for i in output):
        logger.info("Image URL present")
        return True
    else:
        raise UnexpectedBehaviour("Image url not Present in Registry")
