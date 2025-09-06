import os
from jinja2 import Environment, FileSystemLoader
from ocs_ci.ocs.constants import KRKN_SCENARIO_TEMPLATE


class TemplateWriter:
    """Generates YAML from Jinja2 templates."""

    def __init__(self, template_path):
        """Initializes the template writer with a Jinja2 template.

        Args:
            template_path (str): Path to the Jinja2 template file.
        """
        template_dir = os.path.dirname(template_path) or "."
        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.template = self.env.get_template(os.path.basename(template_path))
        self.config = {}

    def render_yaml(self):
        """Renders the YAML string from the Jinja2 template.

        Returns:
            str: Rendered YAML string.
        """
        return self.template.render(self.config)

    def write_to_file(self, output_path):
        """Writes the rendered YAML to a file.

        Args:
            output_path (str): Path to save the generated YAML file.

        Returns:
            str: Path to the written YAML file.
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            f.write(self.render_yaml())
        return output_path


def _get_selector_config(node_name=None, node_selector=None):
    """Generates selector configuration for node targeting.

    Args:
        node_name (str, optional): Specific node name to target.
        node_selector (dict, optional): Node selector dictionary.

    Returns:
        dict: Configuration with either node_name or node_selector.
    """
    if node_name:
        return {"node_name": node_name}
    return {"node_selector": node_selector or {}}


def _get_pod_selector_config(pod_name=None, label_selector=None):
    """Generates selector configuration for pod targeting.

    Args:
        pod_name (str, optional): Specific pod name to target.
        label_selector (dict, optional): Pod label selector dictionary.

    Returns:
        dict: Configuration with either pod_name or label_selector.

    Raises:
        ValueError: If neither pod_name nor label_selector is provided.
    """
    if not pod_name and not label_selector:
        raise ValueError("Either pod_name or label_selector must be provided")
    return {"pod_name": pod_name} if pod_name else {"label_selector": label_selector}


class HogScenarios:
    """Generates configuration for Krkn hog scenarios."""

    @staticmethod
    def _create_hog(scenario_dir, template_name, hog_data, output_name):
        """Creates hog scenario YAML from template.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            template_name (str): Name of the Jinja2 template file.
            hog_data (dict): Configuration data for the template.
            output_name (str): Name of the output YAML file.

        Returns:
            str: Path to the written YAML file.
        """
        template_path = os.path.join(KRKN_SCENARIO_TEMPLATE, "kube", template_name)
        writer = TemplateWriter(template_path)
        writer.config = hog_data
        return writer.write_to_file(os.path.join(scenario_dir, output_name))

    @staticmethod
    def cpu_hog(
        scenario_dir,
        duration=60,
        workers="''",
        image="quay.io/krkn-chaos/krkn-hog",
        namespace="default",
        cpu_load_percentage=90,
        cpu_method="all",
        node_name=None,
        node_selector=None,
        number_of_nodes=1,
        taints=None,
    ):
        """Generates CPU hog YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            duration (int): Duration in seconds for the CPU hog scenario (default: 60).
            workers (str): Worker configuration (default: "''").
            image (str): Container image for the hog (default: "quay.io/krkn-chaos/krkn-hog").
            namespace (str): Target namespace (default: "default").
            cpu_load_percentage (int): CPU load percentage (default: 90).
            cpu_method (str): CPU load method (default: "all").
            node_name (str, optional): Specific node name to target.
            node_selector (dict, optional): Node selector dictionary.
            number_of_nodes (int): Number of nodes to target (default: 1).
            taints (list, optional): List of taints to apply.

        Returns:
            str: Path to the generated YAML file.
        """
        hog_data = {
            "duration": duration,
            "workers": workers,
            "hog_type": "cpu",
            "image": image,
            "namespace": namespace,
            "cpu_load_percentage": cpu_load_percentage,
            "cpu_method": cpu_method,
            "number_of_nodes": number_of_nodes,
            "taints": taints or [],
            **_get_selector_config(node_name, node_selector),
        }
        return HogScenarios._create_hog(
            scenario_dir, "cpu-hog.yml.j2", hog_data, "cpu_hog.yaml"
        )

    @staticmethod
    def io_hog(
        scenario_dir,
        duration=30,
        workers="''",
        image="quay.io/krkn-chaos/krkn-hog",
        namespace="default",
        io_block_size="1m",
        io_write_bytes="1g",
        io_target_pod_folder="/hog-data",
        io_target_pod_volume=None,
        node_name=None,
        node_selector=None,
        number_of_nodes=3,
        taints=None,
    ):
        """Generates IO hog YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            duration (int): Duration in seconds for the IO hog scenario (default: 30).
            workers (str): Worker configuration (default: "''").
            image (str): Container image for the hog (default: "quay.io/krkn-chaos/krkn-hog").
            namespace (str): Target namespace (default: "default").
            io_block_size (str): IO block size (default: "1m").
            io_write_bytes (str): IO write bytes (default: "1g").
            io_target_pod_folder (str): Target folder in pod (default: "/hog-data").
            io_target_pod_volume (dict, optional): Volume configuration for pod.
            node_name (str, optional): Specific node name to target.
            node_selector (dict, optional): Node selector dictionary.
            number_of_nodes (int): Number of nodes to target (default: 3).
            taints (list, optional): List of taints to apply.

        Returns:
            str: Path to the generated YAML file.
        """
        hog_data = {
            "duration": duration,
            "workers": workers,
            "hog_type": "io",
            "image": image,
            "namespace": namespace,
            "io_block_size": io_block_size,
            "io_write_bytes": io_write_bytes,
            "io_target_pod_folder": io_target_pod_folder,
            "io_target_pod_volume": io_target_pod_volume
            or {"name": "node-volume", "hostPath": {"path": "/tmp"}},
            "number_of_nodes": number_of_nodes,
            "taints": taints or [],
            **_get_selector_config(node_name, node_selector),
        }
        return HogScenarios._create_hog(
            scenario_dir, "io-hog.yml.j2", hog_data, "io_hog.yaml"
        )

    @staticmethod
    def memory_hog(
        scenario_dir,
        duration=60,
        workers="''",
        image="quay.io/krkn-chaos/krkn-hog",
        namespace="default",
        memory_vm_bytes="90%",
        node_name=None,
        node_selector=None,
        number_of_nodes=3,
        taints=None,
    ):
        """Generates Memory hog YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            duration (int): Duration in seconds for the memory hog scenario (default: 60).
            workers (str): Worker configuration (default: "''").
            image (str): Container image for the hog (default: "quay.io/krkn-chaos/krkn-hog").
            namespace (str): Target namespace (default: "default").
            memory_vm_bytes (str): Memory usage in bytes or percentage (default: "90%").
            node_name (str, optional): Specific node name to target.
            node_selector (dict, optional): Node selector dictionary.
            number_of_nodes (int): Number of nodes to target (default: 3).
            taints (list, optional): List of taints to apply.

        Returns:
            str: Path to the generated YAML file.
        """
        hog_data = {
            "duration": duration,
            "workers": workers,
            "hog_type": "memory",
            "image": image,
            "namespace": namespace,
            "memory_vm_bytes": memory_vm_bytes,
            "number_of_nodes": number_of_nodes,
            "taints": taints or [],
            **_get_selector_config(node_name, node_selector),
        }
        return HogScenarios._create_hog(
            scenario_dir, "memory-hog.yml.j2", hog_data, "memory_hog.yaml"
        )


class ApplicationOutageScenarios:
    """Generates configuration for application outage scenarios."""

    @staticmethod
    def application_outage(
        scenario_dir,
        duration=600,
        namespace="default",
        pod_selector=None,
        block=None,
    ):
        """Generates application outage YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            duration (int): Seconds after which routes become accessible (default: 600).
            namespace (str): Target namespace (default: "default").
            pod_selector (dict, optional): Label selector dictionary for target pods.
            block (list, optional): Directions to block (e.g., ["Ingress", "Egress"]).

        Returns:
            str: Path to the generated YAML file.
        """
        template_path = os.path.join(
            KRKN_SCENARIO_TEMPLATE, "openshift", "app_outage.yml.j2"
        )
        config = {
            "duration": duration,
            "namespace": namespace,
            "pod_selector": pod_selector or {},
            "block": block or ["Ingress", "Egress"],
        }
        writer = TemplateWriter(template_path)
        writer.config = config
        return writer.write_to_file(
            os.path.join(scenario_dir, "application_outage.yaml")
        )


class NetworkOutageScenarios:
    """Generates configuration for network outage scenarios."""

    @staticmethod
    def _create_network_scenario(scenario_dir, template_name, config, output_name):
        """Creates network scenario YAML from template.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            template_name (str): Name of the Jinja2 template file.
            config (dict): Configuration data for the template.
            output_name (str): Name of the output YAML file.

        Returns:
            str: Path to the written YAML file.
        """
        template_path = os.path.join(KRKN_SCENARIO_TEMPLATE, "openshift", template_name)
        writer = TemplateWriter(template_path)
        writer.config = config
        return writer.write_to_file(os.path.join(scenario_dir, output_name))

    @staticmethod
    def pod_egress_shaping(
        scenario_dir,
        namespace,
        label_selector=None,
        pod_name=None,
        network_params=None,
        execution_type="parallel",
        instance_count=1,
        wait_duration=300,
        test_duration=120,
    ):
        """Generates pod egress shaping YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            namespace (str): Target namespace (required).
            label_selector (dict, optional): Label selector for target pods.
            pod_name (str, optional): Specific pod name to target.
            network_params (dict, optional): Network parameters
            (e.g., {"latency": "50ms", "loss": "'0.02%'", "bandwidth": "100mbit"}).
            execution_type (str): Execution mode, 'serial' or 'parallel' (default: "parallel").
            instance_count (int): Number of matching pods to act on (default: 1).
            wait_duration (int): Wait duration in seconds (default: 300).
            test_duration (int): Test duration in seconds (default: 120).

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither pod_name nor label_selector is provided.
        """
        config = {
            "namespace": namespace,
            "network_params": network_params
            or {"latency": "50ms", "loss": "'0.02%'", "bandwidth": "100mbit"},
            "execution_type": execution_type,
            "instance_count": instance_count,
            "wait_duration": wait_duration,
            "test_duration": test_duration,
            **_get_pod_selector_config(pod_name, label_selector),
        }
        return NetworkOutageScenarios._create_network_scenario(
            scenario_dir, "pod_egress_shaping.yml.j2", config, "pod_egress_shaping.yaml"
        )

    @staticmethod
    def pod_network_outage(
        scenario_dir,
        namespace,
        direction=None,
        ingress_ports=None,
        egress_ports=None,
        pod_name=None,
        label_selector=None,
        instance_count=1,
        wait_duration=300,
        test_duration=120,
    ):
        """Generates pod network outage YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            namespace (str): Target namespace (required).
            direction (list, optional): Directions to apply filters (e.g., ["egress", "ingress"]).
            ingress_ports (list, optional): Ingress ports to block (default: []).
            egress_ports (list, optional): Egress ports to block (default: []).
            pod_name (str, optional): Specific pod name to target.
            label_selector (dict, optional): Label selector for target pods.
            instance_count (int): Number of matching instances to act on (default: 1).
            wait_duration (int): Wait duration in seconds (default: 300).
            test_duration (int): Test duration in seconds (default: 120).

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither pod_name nor label_selector provided.
        """
        # Generate unique filename based on scenario configuration
        direction_str = "_".join(sorted(direction or ["egress", "ingress"]))
        ports_str = ""
        if ingress_ports:
            ports_str += f"_ingress_{'-'.join(map(str, sorted(ingress_ports)))}"
        if egress_ports:
            ports_str += f"_egress_{'-'.join(map(str, sorted(egress_ports)))}"

        filename = f"pod_network_outage_{direction_str}{ports_str}.yaml"
        config = {
            "namespace": namespace,
            "direction": direction or ["egress", "ingress"],
            "ingress_ports": ingress_ports or [],
            "egress_ports": egress_ports or [],
            "instance_count": instance_count,
            "wait_duration": wait_duration,
            "test_duration": test_duration,
            **_get_pod_selector_config(pod_name, label_selector),
        }
        return NetworkOutageScenarios._create_network_scenario(
            scenario_dir, "pod_network_outage.yml.j2", config, filename
        )

    @staticmethod
    def pod_network_chaos(
        scenario_dir,
        duration=300,
        node_name=None,
        label_selector=None,
        instance_count=1,
        interfaces=None,
        execution="serial",
        egress=None,
    ):
        """Generates pod network chaos YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            duration (int): Duration in seconds for network chaos (default: 300).
            node_name (str, optional): Specific node name to target.
            label_selector (str, optional): Node label selector string.
            instance_count (int): Number of matching nodes to act on (default: 1).
            interfaces (list, optional): List of host network interface names (e.g., ["eth0"]).
            execution (str): Execution mode, 'serial' or 'parallel' (default: "serial").
            egress (dict, optional): Egress impairment parameters (e.g., {"latency": "50ms", "loss": 0.02}).

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither node_name nor label_selector is provided.
        """
        if not node_name and not label_selector:
            raise ValueError("Either node_name or label_selector must be provided")
        config = {
            "duration": duration,
            "instance_count": instance_count,
            "interfaces": interfaces or [],
            "execution": execution,
            "egress": egress or {"latency": "50ms", "loss": 0.02},
            **_get_selector_config(node_name, label_selector),
        }
        return NetworkOutageScenarios._create_network_scenario(
            scenario_dir, "network_chaos.yml.j2", config, "network_chaos.yaml"
        )

    @staticmethod
    def network_chaos_ingress(
        scenario_dir,
        node_interface_name=None,
        label_selector=None,
        instance_count=1,
        kubeconfig_path=None,
        execution_type="parallel",
        network_params=None,
        wait_duration=300,
        test_duration=120,
        kraken_config=None,
    ):
        """Generates network chaos ingress YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            node_interface_name (dict, optional): Dict mapping node names to interfaces (e.g., {"worker-0": ["eth0"]}).
            label_selector (str, optional): Node label selector string.
            instance_count (int): Number of nodes to select (default: 1).
            kubeconfig_path (str, optional): Path to kubeconfig (default: ~/.kube/config).
            execution_type (str): Execution mode, 'serial' or 'parallel' (default: "parallel").
            network_params (dict, optional): Network parameters (e.g., {"latency": "50ms", "loss": "0.02"}).
            wait_duration (int): Wait duration in seconds (default: 300).
            test_duration (int): Test duration in seconds (default: 120).
            kraken_config (str, optional): Path to Cerberus config.

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither node_interface_name nor label_selector is provided.
        """
        if not node_interface_name and not label_selector:
            raise ValueError(
                "Either node_interface_name or label_selector must be provided"
            )
        config = {
            "node_interface_name": node_interface_name,
            "label_selector": label_selector,
            "instance_count": instance_count,
            "kubeconfig_path": kubeconfig_path,
            "execution_type": execution_type,
            "network_params": network_params or {"latency": "50ms", "loss": "0.02"},
            "wait_duration": wait_duration,
            "test_duration": test_duration,
            "kraken_config": kraken_config,
        }
        return NetworkOutageScenarios._create_network_scenario(
            scenario_dir,
            "network_chaos_ingress.yml.j2",
            config,
            "network_chaos_ingress.yaml",
        )

    @staticmethod
    def pod_ingress_shaping(
        scenario_dir,
        namespace,
        label_selector=None,
        pod_name=None,
        network_params=None,
        execution_type="parallel",
        instance_count=1,
        wait_duration=300,
        test_duration=120,
    ):
        """Generates pod ingress shaping YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            namespace (str): Target namespace (required).
            label_selector (dict, optional): Label selector for target pods.
            pod_name (str, optional): Specific pod name to target.
            network_params (dict, optional): Network parameters
            (e.g., {"latency": "50ms", "loss": "'0.02%'", "bandwidth": "100mbit"}).
            execution_type (str): Execution mode, 'serial' or 'parallel' (default: "parallel").
            instance_count (int): Number of matching pods to act on (default: 1).
            wait_duration (int): Wait duration in seconds (default: 300).
            test_duration (int): Test duration in seconds (default: 120).

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither pod_name nor label_selector is provided.
        """
        config = {
            "namespace": namespace,
            "network_params": network_params
            or {"latency": "50ms", "loss": "'0.02%'", "bandwidth": "100mbit"},
            "execution_type": execution_type,
            "instance_count": instance_count,
            "wait_duration": wait_duration,
            "test_duration": test_duration,
            **_get_pod_selector_config(pod_name, label_selector),
        }
        return NetworkOutageScenarios._create_network_scenario(
            scenario_dir,
            "pod_ingress_shaping.yml.j2",
            config,
            "pod_ingress_shaping.yaml",
        )


class ContainerScenarios:
    """Generates configuration for container chaos scenarios."""

    @staticmethod
    def _create_container_scenario(scenario_dir, template_name, config, filename):
        """Internal method to create container scenario YAML files."""
        template_path = os.path.join(KRKN_SCENARIO_TEMPLATE, template_name)
        writer = TemplateWriter(template_path)
        writer.config = config
        return writer.write_to_file(os.path.join(scenario_dir, filename))

    @staticmethod
    def container_kill(
        scenario_dir,
        namespace,
        label_selector=None,
        pod_name=None,
        container_name="",
        kill_signal="SIGKILL",
        instance_count=1,
        wait_duration=300,
    ):
        """Generates container kill scenario YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            namespace (str): Target namespace (required).
            label_selector (str, optional): Label selector for target pods.
            pod_name (str, optional): Specific pod name to target.
            container_name (str, optional): Specific container name to kill (default: random).
            kill_signal (str): Signal to send to the container (default: "SIGKILL").
            instance_count (int): Number of matching pods to act on (default: 1).
            wait_duration (int): Wait duration in seconds (default: 300).

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither pod_name nor label_selector is provided.
        """
        if not pod_name and not label_selector:
            raise ValueError("Either pod_name or label_selector must be provided")

        config = {
            "namespace": namespace,
            "container_name": container_name,
            "kill_signal": kill_signal,
            "instance_count": instance_count,
            "wait_duration": wait_duration,
            **_get_pod_selector_config(pod_name, label_selector),
        }
        return ContainerScenarios._create_container_scenario(
            scenario_dir,
            "openshift/container_kill.yml.j2",
            config,
            "container_kill.yaml",
        )

    @staticmethod
    def container_pause(
        scenario_dir,
        namespace,
        label_selector=None,
        pod_name=None,
        container_name="",
        pause_seconds=60,
        instance_count=1,
        wait_duration=300,
    ):
        """Generates container pause scenario YAML.

        Args:
            scenario_dir (str): Directory to write the YAML file.
            namespace (str): Target namespace (required).
            label_selector (str, optional): Label selector for target pods.
            pod_name (str, optional): Specific pod name to target.
            container_name (str, optional): Specific container name to pause (default: random).
            pause_seconds (int): Duration to pause the container in seconds (default: 60).
            instance_count (int): Number of matching pods to act on (default: 1).
            wait_duration (int): Wait duration in seconds (default: 300).

        Returns:
            str: Path to the generated YAML file.

        Raises:
            ValueError: If neither pod_name nor label_selector is provided.
        """
        if not pod_name and not label_selector:
            raise ValueError("Either pod_name or label_selector must be provided")

        config = {
            "namespace": namespace,
            "container_name": container_name,
            "pause_seconds": pause_seconds,
            "instance_count": instance_count,
            "wait_duration": wait_duration,
            **_get_pod_selector_config(pod_name, label_selector),
        }
        return ContainerScenarios._create_container_scenario(
            scenario_dir,
            "openshift/container_pause.yml.j2",
            config,
            "container_pause.yaml",
        )
