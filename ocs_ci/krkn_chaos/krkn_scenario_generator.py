import os
from jinja2 import Environment, FileSystemLoader
from ocs_ci.ocs.constants import (
    KRKN_SCENARIO_TEMPLATE,
)


class TemplateWriter:
    """
    Base class for hog scenario YAML generators. Provides template loading and rendering logic.
    """

    def __init__(self, template_path):
        """
        Initialize the generator with a Jinja2 template.

        Args:
            template_path: Path to the Jinja2 template file.
        """
        template_dir = os.path.dirname(template_path) or "."
        template_file = os.path.basename(template_path)

        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.template = self.env.get_template(template_file)
        self.config = {}

    def render_yaml(self):
        """
        Render the YAML string from the Jinja2 template.

        Returns:
            Rendered YAML string.
        """
        return self.template.render(self.config)

    def write_to_file(self, output_path):
        """
        Write the rendered YAML content to a file.

        Args:
            output_path: Path to save the generated YAML file.
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            f.write(self.render_yaml())


class HogScenarios:
    """
    A class to generate configuration data for Krkn hog scenarios.
    """

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
        """
        Static method to generate dictionary for CPU hog Jinja template.

        - Only one of `node_name` or `node_selector` is included in the config.
        - If neither is provided, `node_selector` defaults to an empty dict ({}).
        """
        cpu_hog_template = os.path.join(
            KRKN_SCENARIO_TEMPLATE, "kube", "cpu-hog.yml.j2"
        )

        # Prepare selector fields based on priority
        selector_config = {}
        if node_name:
            selector_config["node_name"] = node_name
        elif node_selector:
            selector_config["node_selector"] = node_selector
        else:
            selector_config["node_selector"] = {}

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
            **selector_config,  # Injects either node_name or node_selector
        }

        # Create a TemplateWriter instance and write the YAML
        writer = TemplateWriter(cpu_hog_template)
        writer.config = hog_data
        output_path = os.path.join(scenario_dir, "cpu_hog.yaml")
        writer.write_to_file(output_path)
        return output_path

    @staticmethod
    def io_hog(
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
        number_of_nodes="",
        taints=None,
    ):
        """
        Static method to generate dictionary for IO hog Jinja template.
        """
        return {
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
            "node_name": node_name,
            "node_selector": node_selector,
            "number_of_nodes": number_of_nodes,
            "taints": taints or [],
        }

    @staticmethod
    def memory_hog(
        duration=60,
        workers="''",
        image="quay.io/krkn-chaos/krkn-hog",
        namespace="default",
        memory_vm_bytes="90%",
        node_name=None,
        node_selector=None,
        number_of_nodes="''",
        taints=None,
    ):
        """
        Static method to generate dictionary for Memory hog Jinja template.
        """
        return {
            "duration": duration,
            "workers": workers,
            "hog_type": "memory",
            "image": image,
            "namespace": namespace,
            "memory_vm_bytes": memory_vm_bytes,
            "node_name": node_name,
            "node_selector": node_selector,
            "number_of_nodes": number_of_nodes,
            "taints": taints or [],
        }
