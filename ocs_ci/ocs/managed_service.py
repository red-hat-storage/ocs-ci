import logging

from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import delete_pods
from tests.conftest import delete_projects
from ocs_ci.ocs.resources.pvc import delete_pvcs


logger = logging.getLogger(__name__)


def pvc_to_pvc_clone(pvc_factory, pod_factory, teardown_factory, data_process_dict):
    logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
    for interface_type in [constants.CEPHBLOCKPOOL]:
        try:
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            pvc_obj = pvc_factory(
                interface=interface_type, size=1, status=constants.STATUS_BOUND
            )
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            pod_obj = pod_factory(
                interface=interface_type, pvc=pvc_obj, status=constants.STATUS_RUNNING
            )
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            logger.info(f"Running IO on pod {pod_obj.name}")
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            file_name = pod_obj.name
            logger.info(f"File created during IO {file_name}")
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            pod_obj.run_io(storage_type="fs", size="500M", fio_filename=file_name)
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            # Wait for fio to finish
            pod_obj.get_fio_results()
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            logger.info(f"Io completed on pod {pod_obj.name}.")
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            # Verify presence of the file
            file_path = pod.get_file_path(pod_obj, file_name)
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            logger.info(f"Actual file path on the pod {file_path}")
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            assert pod.check_file_existence(
                pod_obj, file_path
            ), f"File {file_name} does not exist"
            logger.info(f"File {file_name} exists in {pod_obj.name}")
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            # Calculate md5sum of the file.
            orig_md5_sum = pod.cal_md5sum(pod_obj, file_name)
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            # Create a clone of the existing pvc.
            sc_name = pvc_obj.backed_sc
            parent_pvc = pvc_obj.name
            clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
            namespace = pvc_obj.namespace
            if interface_type == constants.CEPHFILESYSTEM:
                clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name, parent_pvc, clone_yaml, namespace
            )
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            teardown_factory(cloned_pvc_obj)
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND)
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            cloned_pvc_obj.reload()

            # Create and attach pod to the pvc
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            clone_pod_obj = helpers.create_pod(
                interface_type=interface_type,
                pvc_name=cloned_pvc_obj.name,
                namespace=cloned_pvc_obj.namespace,
                pod_dict_path=constants.NGINX_POD_YAML,
            )
            # Confirm that the pod is running
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            helpers.wait_for_resource_state(
                resource=clone_pod_obj, state=constants.STATUS_RUNNING
            )
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            clone_pod_obj.reload()
            teardown_factory(clone_pod_obj)

            # Verify file's presence on the new pod
            logger.info(
                f"Checking the existence of {file_name} on cloned pod "
                f"{clone_pod_obj.name}"
            )
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            assert pod.check_file_existence(
                clone_pod_obj, file_path
            ), f"File {file_path} does not exist"
            logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

            # Verify Contents of a file in the cloned pvc
            # by validating if md5sum matches.
            logger.info(
                f"Verifying that md5sum of {file_name} "
                f"on pod {pod_obj.name} matches with md5sum "
                f"of the same file on restore pod {clone_pod_obj.name}"
            )
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            assert pod.verify_data_integrity(
                clone_pod_obj, file_name, orig_md5_sum
            ), "Data integrity check failed"
            logger.info("Data integrity check passed, md5sum are same")

            logger.info("Run IO on new pod")
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            clone_pod_obj.run_io(storage_type="fs", size="100M", runtime=10)

            # Wait for IO to finish on the new pod
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            clone_pod_obj.get_fio_results()
            logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
            logger.info(f"IO completed on pod {clone_pod_obj.name}")
            data_process_dict[
                f"{config.ENV_DATA.get('cluster_name')}_pvc_to_pvc_clone_{interface_type}"
            ] = True
        except Exception as e:
            logger.info(e)
            if "pod_obj" in locals():
                delete_pods(pod_objs=[pod_obj])
            if "pvc_obj" in locals():
                delete_pvcs(pvc_objs=[pvc_obj])
            if "clone_pod_obj" in locals():
                delete_pods(pod_objs=[clone_pod_obj])
            if "cloned_pvc_obj" in locals():
                delete_pvcs(pvc_objs=[cloned_pvc_obj])
            if "pvc_obj" in locals():
                ocp_obj = ocp.OCP(namespace=pvc_obj.namespace)
                delete_projects([ocp_obj])
        finally:
            if "pod_obj" in locals():
                delete_pods(pod_objs=[pod_obj])
            if "pvc_obj" in locals():
                delete_pvcs(pvc_objs=[pvc_obj])
            if "clone_pod_obj" in locals():
                delete_pods(pod_objs=[clone_pod_obj])
            if "cloned_pvc_obj" in locals():
                delete_pvcs(pvc_objs=[cloned_pvc_obj])
            if "pvc_obj" in locals():
                ocp_obj = ocp.OCP(namespace=pvc_obj.namespace)
                delete_projects([ocp_obj])


def flow(pvc_factory, pod_factory, teardown_factory, index, data_process_dict):
    config.switch_ctx(index)
    logger.info(f"********{config.ENV_DATA.get('cluster_name')}************")
    pvc_to_pvc_clone(
        pvc_factory=pvc_factory,
        pod_factory=pod_factory,
        teardown_factory=teardown_factory,
        data_process_dict=data_process_dict,
    )
