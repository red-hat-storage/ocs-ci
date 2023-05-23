import csv
import logging
import os
import re
from tempfile import NamedTemporaryFile
from xml.etree import ElementTree
from datetime import datetime

from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.ocp import (
    OCP,
    switch_to_project,
    switch_to_default_rook_cluster_project,
)
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_pod_logs, get_pod_obj
from ocs_ci.ocs.exceptions import TimeoutExpiredError, UnexpectedBehaviour

logger = logging.getLogger(__name__)


class Cosbench(object):
    """
    Cosbench S3 benchmark tool

    """

    def __init__(self):
        """
        Initializer function

        """
        self.ns_obj = OCP(kind="namespace")
        self.namespace = constants.COSBENCH_PROJECT
        self.configmap_obj = OCP(namespace=self.namespace, kind=constants.CONFIGMAP)
        self.ocp_obj = OCP(namespace=self.namespace)
        self.cosbench_config = None
        self.cosbench_pod = None
        self.cosbench_dir = "/home"
        self.xml_file = ""
        self.workload_id = ""
        self.init_container = 1
        self.range_selector = "r"
        self.init_object = 1
        mcg_obj = MCG()
        self.access_key_id = mcg_obj.access_key_id
        self.access_key = mcg_obj.access_key
        self.endpoint = (
            "http://" + mcg_obj.s3_internal_endpoint.split("/")[2].split(":")[0]
        )

    def setup_cosbench(self):
        """
        Setups Cosbench namespace, configmap and pod

        """
        # Create cosbench project
        self.ns_obj.new_project(project_name=self.namespace)

        # Create configmap
        config_data = templating.load_yaml(file=constants.COSBENCH_CONFIGMAP)
        cosbench_configmap_name = create_unique_resource_name(
            constants.COSBENCH, "configmap"
        )
        config_data["metadata"]["name"] = cosbench_configmap_name
        config_data["metadata"]["namespace"] = self.namespace
        self.cosbench_config = OCS(**config_data)
        logger.info(f"Creating Cosbench configmap: {self.cosbench_config.name}")
        self.cosbench_config.create()
        self.configmap_obj.wait_for_resource(
            resource_name=self.cosbench_config.name, column="DATA", condition="4"
        )

        # Create Cosbench pod
        cosbench_pod_data = templating.load_yaml(file=constants.COSBENCH_POD)
        cosbench_pod_data["spec"]["containers"][0]["envFrom"][0]["configMapRef"][
            "name"
        ] = self.cosbench_config.name
        cosbench_pod_name = create_unique_resource_name(constants.COSBENCH, "pod")
        cosbench_pod_data["metadata"]["name"] = cosbench_pod_name
        cosbench_pod_data["metadata"]["namespace"] = self.namespace
        self.cosbench_pod = OCS(**cosbench_pod_data)
        logger.info(f"Creating Cosbench pod: {self.cosbench_pod.name}")
        self.cosbench_pod.create()
        helpers.wait_for_resource_state(
            resource=self.cosbench_pod, state=constants.STATUS_RUNNING, timeout=300
        )

    def _apply_mcg_auth(self, xml_root):
        """
        Applies MCG credentials

        Args:
            xml_root (Element): Root element of workload xml

        """
        xml_root[0].set(
            "config",
            f"accesskey={self.access_key_id};secretkey={self.access_key};"
            f"endpoint={self.endpoint};path_style_access=true",
        )

    def run_init_workload(
        self,
        prefix,
        containers,
        objects,
        start_container=None,
        start_object=None,
        size=64,
        size_unit="KB",
        sleep=15,
        timeout=300,
        validate=True,
    ):
        """
        Creates specific containers and objects in bulk

        Args:
            prefix (str): Prefix of bucket name.
            containers (int): Number of containers/buckets to be created.
            objects (int): Number of objects to be created on each bucket.
            start_container (int): Start of containers. Default: 1.
            start_object (int): Start of objects. Default: 1.
            size (int): Size of each objects.
            size_unit (str): Object size unit (B/KB/MB/GB)
            sleep (int): Sleep in seconds.
            timeout (int): Timeout in seconds.
            validate (bool): Validates whether init and prepare is completed.

        Returns:
            Tuple[str, str]: Workload xml and its name

        """
        init_template = """
        <workload name="Fill" description="Init and prepare operation">
        <storage type="s3" config="" />
          <workflow>
            <workstage name="init-containers">
              <work type="init" workers="1" config="" />
            </workstage>
            <workstage name="prepare-objects">
              <work type="prepare" workers="16" config="" />
            </workstage>
          </workflow>
        </workload>
        """
        xml_root, xml_tree = self._create_element_tree(template=init_template)
        workload_name = xml_root.get("name")
        self._apply_mcg_auth(xml_root)
        self.init_container = (
            start_container if start_container else self.init_container
        )
        self.init_object = start_object if start_object else self.init_object
        init_container_config = self.generate_container_stage_config(
            self.range_selector,
            self.init_container,
            containers,
        )
        init_config = self.generate_stage_config(
            self.range_selector,
            self.init_container,
            containers,
            self.init_object,
            objects,
        )
        for stage in xml_root.iter("work"):
            if stage.get("type") == "init":
                stage.set("config", f"cprefix={prefix};{init_container_config}")
            elif stage.get("type") == "prepare":
                stage.set(
                    "config",
                    f"cprefix={prefix};{init_config};sizes=c({str(size)}){size_unit}",
                )
        self._create_tmp_xml(xml_tree=xml_tree, xml_file_prefix=workload_name)
        self.submit_workload(workload_path=self.xml_file)
        self.wait_for_workload(
            workload_id=self.workload_id, sleep=sleep, timeout=timeout
        )
        if validate:
            self.validate_workload(
                workload_id=self.workload_id, workload_name=workload_name
            )
        else:
            return self.workload_id, workload_name

    def run_cleanup_workload(
        self,
        prefix,
        containers,
        objects,
        start_container=None,
        start_object=None,
        sleep=15,
        timeout=300,
        validate=True,
    ):
        """
        Deletes specific objects and containers in bulk.

        Args:
            prefix (str): Prefix of bucket name.
            containers (int): Number of containers/buckets to be created.
            objects (int): Number of objects to be created on each bucket.
            start_container (int): Start of containers. Default: 1.
            start_object (int): Start of objects. Default: 1.
            sleep (int): Sleep in seconds.
            timeout (int): Timeout in seconds.
            validate (bool): Validates whether cleanup and dispose is completed.

        Returns:
            Tuple[str, str]: Workload xml and its name

        """
        cleanup_template = """
        <workload name="Cleanup" description="Cleanup and Dispose">
          <storage type="s3" config="" />
          <workflow>
            <workstage name="cleanup-objects">
              <work type="cleanup" workers="4" config="" />
            </workstage>
            <workstage name="dispose-containers">
              <work type="dispose" workers="1" config="" />
            </workstage>
          </workflow>
        </workload>
        """
        xml_root, xml_tree = self._create_element_tree(template=cleanup_template)
        workload_name = xml_root.get("name")
        self._apply_mcg_auth(xml_root)
        self.init_container = (
            start_container if start_container else self.init_container
        )
        self.init_object = start_object if start_object else self.init_object
        cleanuo_config = self.generate_stage_config(
            self.range_selector,
            self.init_container,
            containers,
            self.init_object,
            objects,
        )
        for stage in xml_root.iter("work"):
            if stage.get("type") == "cleanup":
                stage.set(
                    "config",
                    f"cprefix={prefix};{cleanuo_config}",
                )
            elif stage.get("type") == "dispose":
                stage.set("config", f"cprefix={prefix};{cleanuo_config}")

        self._create_tmp_xml(xml_tree=xml_tree, xml_file_prefix=workload_name)
        self.submit_workload(workload_path=self.xml_file)
        self.wait_for_workload(
            workload_id=self.workload_id, sleep=sleep, timeout=timeout
        )
        if validate:
            self.validate_workload(
                workload_id=self.workload_id, workload_name=workload_name
            )
        else:
            return self.workload_id, workload_name

    def run_main_workload(
        self,
        operation_type,
        prefix,
        containers,
        objects,
        workers=4,
        selector="s",
        start_container=None,
        start_object=None,
        size=64,
        size_unit="KB",
        sleep=15,
        timeout=300,
        extend_objects=None,
        validate=True,
        result=True,
    ):
        """
        Creates and runs main Cosbench workload.

        Args:
            operation_type (dict): Cosbench operation and its ratio.
                                   Operation (str): Supported ops are read, write, list and delete.
                                   Ratio (int): Percentage of each operation. Should add up to 100.
            workers (int): Number of users to perform operations.
            containers (int): Number of containers/buckets to be created.
            objects (int): Number of objects to be created on each bucket.
            selector (str): The way object is accessed/selected. u=uniform, r=range, s=sequential.
            prefix (str): Prefix of bucket name.
            start_container (int): Start of containers. Default: 1.
            start_object (int): Start of objects. Default: 1.
            size (int): Size of each objects.
            size_unit (str): Object size unit (B/KB/MB/GB)
            sleep (int): Sleep in seconds
            timeout (int): Timeout in seconds
            validate (bool): Validates whether each stage is completed
            extend_objects (int): Extends the total number of objects to prevent overlap.
                                  Use only for Write and Delete operations.
            result (bool): Get performance results when running workload is completed.

        Returns:
            Tuple[str, str]: Workload xml and its name

        """
        main_template = """
        <workload name="workload_name" description="Main workload">
          <storage type="s3" config="" />
          <workflow>
            <workstage name="Main">
              <work name="work_name" workers="4" division="object" runtime="60">
              </work>
            </workstage>
          </workflow>
        </workload>
        """
        xml_root, xml_tree = self._create_element_tree(template=main_template)
        workload_name = xml_root.get("name")
        self._apply_mcg_auth(xml_root)
        start_container = start_container if start_container else self.init_container
        start_object = start_object if start_object else self.init_object
        for stage in xml_root.iter("work"):
            stage.set("workers", f"{workers}")
            for operation, ratio in operation_type.items():
                if operation == "write" or "delete":
                    if extend_objects:
                        start_object = objects + 1
                        stage_config = self.generate_stage_config(
                            selector,
                            start_container,
                            containers,
                            start_object,
                            extend_objects,
                        )
                        attributes = {
                            "type": f"{operation}",
                            "ratio": f"{ratio}",
                            "config": f"cprefix={prefix};{stage_config};sizes=c({str(size)}){size_unit}",
                        }
                        ElementTree.SubElement(stage, "operation", attributes)
                    else:
                        stage_config = self.generate_stage_config(
                            selector,
                            start_container,
                            containers,
                            start_object,
                            objects,
                        )

                        attributes = {
                            "type": f"{operation}",
                            "ratio": f"{ratio}",
                            "config": f"cprefix={prefix};{stage_config};sizes=c({str(size)}){size_unit}",
                        }
                        ElementTree.SubElement(stage, "operation", attributes)
                else:
                    stage_config = self.generate_stage_config(
                        selector,
                        start_container,
                        containers,
                        start_object,
                        objects,
                    )
                    attributes = {
                        "type": f"{operation}",
                        "ratio": f"{ratio}",
                        "config": f"cprefix={prefix};{stage_config}",
                    }
                    ElementTree.SubElement(stage, "operation", attributes)

        self._create_tmp_xml(xml_tree=xml_tree, xml_file_prefix=workload_name)
        self.submit_workload(workload_path=self.xml_file)
        self.wait_for_workload(
            workload_id=self.workload_id, sleep=sleep, timeout=timeout
        )
        if validate:
            self.validate_workload(
                workload_id=self.workload_id, workload_name=workload_name
            )
        else:
            return self.workload_id, workload_name

        if result:
            throughput, bandwidth = self.get_performance_result(
                workload_id=self.workload_id,
                workload_name=workload_name,
                size=size,
            )
            return throughput, bandwidth
        else:
            return self.workload_id, workload_name

    @staticmethod
    def generate_stage_config(
        selector, start_container, end_container, start_objects, end_object
    ):
        """
        Generates config which is used in stage creation

        Args:
            selector (str): The way object is accessed/selected. u=uniform, r=range, s=sequential.
            start_container (int): Start of containers
            end_container (int): End of containers
            start_objects (int): Start of objects
            end_object (int): End of objects

        Returns:
            (str): Container and object configuration

        """
        xml_config = (
            f"containers={selector}({str(start_container)},{str(end_container)});"
            f"objects={selector}({str(start_objects)},{str(end_object)})"
        )
        return xml_config

    @staticmethod
    def generate_container_stage_config(selector, start_container, end_container):
        """
        Generates container config which creates buckets in bulk

        Args:
            selector (str): The way object is accessed/selected. u=uniform, r=range, s=sequential.
            start_container (int): Start of containers
            end_container (int): End of containers

        Returns:
            (str): Container and object configuration

        """
        container_config = (
            f"containers={selector}({str(start_container)},{str(end_container)});"
        )
        return container_config

    def _create_tmp_xml(self, xml_tree, xml_file_prefix):
        """
        Creates a xml file and writes the workload

        Args:
            xml_file_prefix (str): Prefix of xml file
            xml_tree (Element): Element tree

        """
        self.xml_file = NamedTemporaryFile(
            dir=self.cosbench_dir,
            prefix=f"{xml_file_prefix}",
            suffix=".xml",
            delete=False,
        ).name
        logger.info(self.xml_file)
        xml_tree.write(self.xml_file)

    @staticmethod
    def _create_element_tree(template):
        """
        Creates element tree and root element of xml

        Args:
            template (str): Template of Cosbench workload

        Returns:
            Tuple[Element, ElementTree]: Root element and element tree of xml

        """
        xml_root = ElementTree.fromstring(text=template)
        xml_tree = ElementTree.ElementTree(element=xml_root)
        return xml_root, xml_tree

    def _copy_workload(self, workload_path):
        """
        Copies workload xml to Cosbench pod

        Args:
            workload_path (str): Absolute path of xml to copy

        """
        self.ocp_obj.exec_oc_cmd(
            command=f"cp {workload_path} {self.cosbench_pod.name}:/cos",
            out_yaml_format=False,
            timeout=180,
        )

    def submit_workload(self, workload_path):
        """
        Submits Cosbench xml to initiate workload

        Args:
            workload_path (str): Absolute path of xml to submit

        """
        self._copy_workload(workload_path=workload_path)
        workload = os.path.split(workload_path)[1]
        self._cosbench_cli(workload)

    @retry(AttributeError, tries=15, delay=5, backoff=1)
    def _cosbench_cli(self, workload):
        """
        Runs Cosbench cli to initiate workload

        Args:
            workload (str): Workload file

        """
        submit_key = "Accepted with ID"
        cobench_pod_obj = get_pod_obj(
            name=self.cosbench_pod.name, namespace=self.namespace
        )
        submit = cobench_pod_obj.exec_cmd_on_pod(
            command=f"/cos/cli.sh submit /cos/{workload}",
            out_yaml_format=True,
            timeout=180,
        )
        if submit_key in submit.keys():
            self.workload_id = submit[submit_key]
        else:
            assert f"Failed to submit the workload, ID not found. stdout: {submit}"

    def wait_for_workload(self, workload_id, sleep=1, timeout=60):
        """
        Waits for the cosbench workload to complete

        Args:
            workload_id (str): ID of cosbench workload
            sleep: sleep in seconds
            timeout: timeout in seconds to check if mirroring

        Returns:
            bool: Whether cosbench workload processed successfully

        """
        logger.info(f"Waiting for workload {workload_id} to be processed")
        pattern = f"sucessfully processed workload {workload_id}"
        try:
            for ret in TimeoutSampler(
                timeout=timeout,
                sleep=sleep,
                func=get_pod_logs,
                pod_name=self.cosbench_pod.name,
                namespace=self.namespace,
            ):
                if re.search(pattern=pattern, string=ret):
                    break
            logger.info(f"Verified: Workload {workload_id} processed successfully")
            return True
        except TimeoutExpiredError:
            logger.error(
                f"Workload {workload_id} did not complete. Dumping cosbench pod log"
            )
            # Log cosbench pod for debugging purpose
            cosbench_log = get_pod_logs(
                pod_name=self.cosbench_pod.name, namespace=self.namespace
            )
            logger.debug(cosbench_log)
            return False

    def validate_workload(self, workload_id, workload_name):
        """
        Validates each stage of cosbench workload

        Args:
            workload_id (str): ID of cosbench workload
            workload_name (str): Name of the workload

        Raises:
            UnexpectedBehaviour: When workload csv is incorrect/malformed.

        """
        workload_csv = self.get_result_csv(
            workload_id=workload_id, workload_name=workload_name
        )
        logger.info(f"workload_csv ******************{workload_csv}")
        logger.info(type(workload_csv))
        with open(workload_csv, "r") as file:
            reader = csv.reader(file)
            header = next(reader)
            if header is not None:
                # Iterate over each row after the header
                logger.info(
                    f"Verifying whether each stage of workload {workload_id} completed"
                )
                for row in reader:
                    if row[16] == "completed":
                        logger.info(f"Stage {row[0]} completed successfully")
                    else:
                        assert (
                            f"Failed: Stage {row[0]} did not complete. Status {row[16]}"
                        )
            else:
                raise UnexpectedBehaviour(
                    f"Workload csv is incorrect/malformed. Dumping csv {reader}"
                )

    def get_result_csv(self, workload_id, workload_name):
        """
        Gets cosbench workload result csv

        Args:
            workload_id (str): ID of cosbench workload
            workload_name (str): Name of the workload

        Returns:
            str: Absolute path of the result csv

        """
        archive_file = f"{workload_id}-{workload_name}"
        cmd = (
            f"cp {self.cosbench_pod.name}:/cos/archive/{archive_file}/{archive_file}.csv "
            f"{self.cosbench_dir}/{archive_file}.csv "
        )
        self.ocp_obj.exec_oc_cmd(
            command=cmd,
            out_yaml_format=False,
            timeout=300,
        )
        return f"{self.cosbench_dir}/{archive_file}.csv"

    def cleanup(self):
        """
        Cosbench cleanup

        """
        switch_to_project(constants.COSBENCH_PROJECT)
        logger.info("Deleting Cosbench pod, configmap and namespace")
        if (
            self.cosbench_pod.ocp.get(
                resource_name=self.cosbench_pod.name, dont_raise=True
            )
            is not None
        ):
            self.cosbench_pod.delete()
        self.cosbench_config.delete()
        switch_to_default_rook_cluster_project()
        self.ns_obj.delete_project(self.namespace)
        self.ns_obj.wait_for_delete(resource_name=self.namespace, timeout=90)

    def get_performance_result(self, workload_name, workload_id, size):
        workload_file = self.get_result_csv(
            workload_id=workload_id, workload_name=workload_name
        )
        throughput_data = {}
        bandwidth_data = {}
        with open(workload_file, "r") as file:
            reader = csv.reader(file)
            header = next(reader)
            if header is not None:
                for row in reader:
                    throughput_data[row[1]] = row[13]
                    bandwidth_data[row[1]] = row[14]
            else:
                raise UnexpectedBehaviour(
                    f"Workload csv is incorrect/malformed. Dumping csv {reader}"
                )
        # Store throughput data on csv file
        log_path = f"{self.cosbench_dir}"
        with open(f"{log_path}/{workload_name}-{size}-throughput.csv", "a") as fd:
            csv_obj = csv.writer(fd)
            for k, v in throughput_data.items():
                csv_obj.writerow([k, v])
        logger.info(
            f"Throughput data present in {log_path}/{workload_name}-{size}-throughput.csv"
        )

        # Store bandwidth data on csv file
        with open(f"{log_path}/{workload_name}-{size}-bandwidth.csv", "a") as fd:
            csv_obj = csv.writer(fd)
            for k, v in bandwidth_data.items():
                csv_obj.writerow([k, v])
        logger.info(
            f"Bandwidth data present in {log_path}/{workload_name}-{size}-bandwidth.csv"
        )
        return throughput_data, bandwidth_data

    def cosbench_full(self):
        """
        Run full Cosbench workload
        """
        bucket_prefix = "bucket-"
        buckets = 10
        objects = 1000

        # Operations to perform and its ratio(%)
        operations = {"read": 50, "write": 50}

        # Deployment of cosbench
        self.setup_cosbench()

        # Create initial containers and objects
        self.run_init_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )
        # Start measuring time
        start_time = datetime.now()

        # Run main workload
        self.run_main_workload(
            operation_type=operations,
            prefix=bucket_prefix,
            containers=buckets,
            objects=objects,
            validate=True,
            timeout=10800,
        )

        # Calculate the total run time of Cosbench workload
        end_time = datetime.now()
        diff_time = end_time - start_time
        logger.info(f"Cosbench workload completed after {diff_time}")

        # Dispose containers and objects
        self.run_cleanup_workload(
            prefix=bucket_prefix, containers=buckets, objects=objects, validate=True
        )
