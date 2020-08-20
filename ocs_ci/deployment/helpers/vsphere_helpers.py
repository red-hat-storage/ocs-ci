"""
This module contains helpers functions needed for deployment
of clusters on vsphere platform.
"""
import json
import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.templating import (
    Templating,
    load_yaml,
    dump_data_to_temp_yaml,
)
from ocs_ci.utility.utils import (
    convert_yaml2tfvars, create_directory_path,
    remove_keys_from_tf_variable_file, replace_content_in_file, run_cmd,
    upload_file, get_ocp_version, get_infra_id, get_cluster_id,
)
from semantic_version import Version

logger = logging.getLogger(__name__)


class VSPHEREHELPERS(object):
    """
    Helper class for vSphere
    """
    def __init__(self):
        """
        Initialize required variables
        """
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.kubeconfig = os.path.join(
            self.cluster_path,
            config.RUN.get('kubeconfig_location')
        )
        self.folder_structure = config.ENV_DATA.get('folder_structure')
        self.ocp_version = get_ocp_version(seperator="_")
        self._templating = Templating()

    def generate_terraform_vars_for_scaleup(self, rhcos_ips):
        """
        Generates the terraform variables file for scaling nodes
        """
        self.scale_up_terraform_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR
        )
        scale_up_terraform_var_yaml = os.path.join(
            self.scale_up_terraform_dir,
            "scale_up_terraform.tfvars.yaml"
        )
        config.ENV_DATA['cluster_info_path'] = self.scale_up_terraform_dir
        config.ENV_DATA['credentials_path'] = self.scale_up_terraform_dir

        if self.folder_structure:
            logger.info(
                "Generating terraform variables for "
                "scaling nodes with folder structure"
            )
            scale_up_terraform_var_template_with_folder_structure = (
                "scale_up_terraform_with_folder_structure.tfvars.j2"
            )
            scale_up_terraform_var_template_path_with_folder_structure = (
                os.path.join(
                    "ocp-deployment",
                    scale_up_terraform_var_template_with_folder_structure
                )
            )

            scale_up_terraform_config_str_with_folder_structure = (
                self._templating.render_template(
                    scale_up_terraform_var_template_path_with_folder_structure,
                    config.ENV_DATA
                )
            )

            with open(scale_up_terraform_var_yaml, "w") as f:
                f.write(scale_up_terraform_config_str_with_folder_structure)

            scale_up_terraform_var = convert_yaml2tfvars(
                scale_up_terraform_var_yaml
            )
            replace_content_in_file(scale_up_terraform_var, "None", "")

        else:
            logger.info(
                "Generating terraform variables for scaling"
                " nodes without folder structure"
            )
            scale_up_terraform_var_template = "scale_up_terraform.tfvars.j2"
            scale_up_terraform_var_template_path = os.path.join(
                "ocp-deployment", scale_up_terraform_var_template
            )
            scale_up_terraform_config_str = self._templating.render_template(
                scale_up_terraform_var_template_path, config.ENV_DATA
            )

            with open(scale_up_terraform_var_yaml, "w") as f:
                f.write(scale_up_terraform_config_str)

            scale_up_terraform_var = convert_yaml2tfvars(
                scale_up_terraform_var_yaml
            )

            # append RHCOS ip list to terraform variable file
            with open(scale_up_terraform_var, "a+") as fd:
                fd.write(f"rhcos_list = {json.dumps(rhcos_ips)}")

        logger.info(
            f"scale-up terraform variable file: {scale_up_terraform_var}"
        )

        return scale_up_terraform_var

    def modify_scaleup_repo(self):
        """
        Modify the scale-up repo. Considering the user experience, removing
        the access and secret keys and variable from appropriate location
        in the scale-up repo
        """
        # importing here to avoid circular dependancy
        from ocs_ci.deployment.vmware import change_vm_root_disk_size
        if self.folder_structure:
            logger.info("Modifying scaleup repo for folder structure")
            # modify default_map.yaml
            default_map_path = os.path.join(
                constants.CLUSTER_LAUNCHER_VSPHERE_DIR,
                f"aos-{self.ocp_version}",
                "default_map.yaml"
            )
            dict_data = load_yaml(default_map_path)
            dict_data['cluster_domain'] = config.ENV_DATA['base_domain']
            dict_data['vsphere']['vcsa-qe']['datacenter'] = (
                config.ENV_DATA['vsphere_datacenter']
            )
            dict_data['vsphere']['vcsa-qe']['datastore'] = (
                config.ENV_DATA['vsphere_datastore']
            )
            dict_data['vsphere']['vcsa-qe']['network'] = (
                config.ENV_DATA['vm_network']
            )
            dict_data['vsphere']['vcsa-qe']['cpus'] = (
                config.ENV_DATA['rhel_num_cpus']
            )
            dict_data['vsphere']['vcsa-qe']['memory'] = (
                config.ENV_DATA['rhel_memory']
            )
            dict_data['vsphere']['vcsa-qe']['root_volume_size'] = (
                config.ENV_DATA.get('root_disk_size', '120'))

            dict_data['vsphere']['vcsa-qe']['image'] = (
                config.ENV_DATA['rhel_template']
            )

            dump_data_to_temp_yaml(dict_data, default_map_path)
        else:
            # remove access and secret key from constants.SCALEUP_VSPHERE_MAIN
            access_key = 'access_key       = "${var.aws_access_key}"'
            secret_key = 'secret_key       = "${var.aws_secret_key}"'
            replace_content_in_file(
                constants.SCALEUP_VSPHERE_MAIN,
                f"{access_key}",
                " "
            )
            replace_content_in_file(
                constants.SCALEUP_VSPHERE_MAIN,
                f"{secret_key}",
                " "
            )

            # remove access and secret key from constants.SCALEUP_VSPHERE_ROUTE53
            route53_access_key = 'access_key = "${var.access_key}"'
            route53_secret_key = 'secret_key = "${var.secret_key}"'
            replace_content_in_file(
                constants.SCALEUP_VSPHERE_ROUTE53,
                f"{route53_access_key}",
                " "
            )
            replace_content_in_file(
                constants.SCALEUP_VSPHERE_ROUTE53,
                f"{route53_secret_key}",
                " "
            )

            replace_content_in_file(
                constants.SCALEUP_VSPHERE_ROUTE53,
                "us-east-1",
                f"{config.ENV_DATA.get('region')}"
            )

            # remove access and secret variables from scale-up repo
            remove_keys_from_tf_variable_file(
                constants.SCALEUP_VSPHERE_VARIABLES,
                ['aws_access_key', 'aws_secret_key'])
            remove_keys_from_tf_variable_file(
                constants.SCALEUP_VSPHERE_ROUTE53_VARIABLES,
                ['access_key', 'secret_key']
            )

            # change root disk size
            change_vm_root_disk_size(constants.SCALEUP_VSPHERE_MACHINE_CONF)

    def generate_cluster_info(self):
        """
        Generates the cluster information file
        """
        logger.info("Generating cluster information file")

        # get kubeconfig and upload to httpd server
        kubeconfig = os.path.join(
            self.cluster_path,
            config.RUN.get('kubeconfig_location')
        )
        remote_path = os.path.join(
            config.ENV_DATA.get('path_to_upload'),
            f"{config.RUN.get('run_id')}_kubeconfig"
        )
        upload_file(
            config.ENV_DATA.get('httpd_server'),
            kubeconfig,
            remote_path,
            config.ENV_DATA.get('httpd_server_user'),
            config.ENV_DATA.get('httpd_server_password')
        )

        #  Form the kubeconfig url path
        kubeconfig_url_path = os.path.join(
            'http://',
            config.ENV_DATA.get('httpd_server'),
            remote_path.lstrip('/var/www/html/')
        )
        config.ENV_DATA['kubeconfig_url'] = kubeconfig_url_path

        # get the infra_id
        infra_id = get_infra_id(self.cluster_path)
        config.ENV_DATA['infra_id'] = infra_id

        # get the cluster id
        cluster_id = get_cluster_id(self.cluster_path)
        config.ENV_DATA['cluster_id'] = cluster_id

        # fetch the installer version
        installer_version_str = run_cmd(
            f"{config.RUN['bin_dir']}/openshift-install version"
        )
        installer_version = installer_version_str.split()[1]
        config.ENV_DATA['installer_version'] = installer_version

        # get the major and minor version of OCP
        version_obj = Version(installer_version)
        ocp_version_x = version_obj.major
        ocp_version_y = version_obj.minor
        config.ENV_DATA['ocp_version_x'] = ocp_version_x
        config.ENV_DATA['ocp_version_y'] = ocp_version_y

        # generate the cluster info yaml file
        terraform_var_template = "cluster_info.yaml.j2"
        terraform_var_template_path = os.path.join(
            "ocp-deployment", terraform_var_template
        )
        terraform_config_str = self._templating.render_template(
            terraform_var_template_path, config.ENV_DATA
        )
        terraform_var_yaml = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR,
            "cluster_info.yaml"
        )

        with open(terraform_var_yaml, "w") as f:
            f.write(terraform_config_str)

        # config.ENV_DATA['dns_server'] = config.ENV_DATA['dns']
        template_vars = (
            f"\"dns_server: {config.ENV_DATA['dns']}"
            f"\\nremove_rhcos_worker: 'yes'\\n\""
        )

        replace_content_in_file(
            terraform_var_yaml,
            "PLACEHOLDER",
            template_vars
        )
        logger.info(f"cluster yaml file: {terraform_var_yaml}")

    def generate_config_yaml(self):
        """
        Generate config yaml file
        """
        # create config directory in scale_up_terraform_data directory
        sclaeup_data_config_dir = os.path.join(
            self.cluster_path,
            constants.TERRAFORM_DATA_DIR,
            constants.SCALEUP_TERRAFORM_DATA_DIR,
            "config"
        )
        create_directory_path(sclaeup_data_config_dir)

        # generate config yaml file
        scale_up_config_var_template = "scale_up_config.yaml.j2"
        scale_up_config_var_template_path = os.path.join(
            "ocp-deployment", scale_up_config_var_template
        )
        config.ENV_DATA['ssh_key_private'] = (
            config.DEPLOYMENT['ssh_key_private']
        )
        scale_up_config_str = self._templating.render_template(
            scale_up_config_var_template_path, config.ENV_DATA
        )
        scale_config_var_yaml = os.path.join(
            sclaeup_data_config_dir,
            "config.yaml"
        )

        with open(scale_config_var_yaml, "w") as f:
            f.write(scale_up_config_str)

        logger.debug(f"scaleup config yaml file : {scale_config_var_yaml}")
