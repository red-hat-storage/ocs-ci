"""
All the flexy related classes and functionality lives here
"""
import logging
import os
import yaml

import io
import configparser
import subprocess
from subprocess import list2cmdline, CalledProcessError
import shlex

from ocs_ci.framework import config, merge_dict
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    get_ocp_version, clone_repo
)
from ocs_ci.ocs import exceptions

logger = logging.getLogger(__name__)


class FlexyBase(object):
    """
    A base class for all types of flexy installs
    """
    def __init__(self):
        self.cluster_name = config.ENV_DATA['cluster_name']
        # Host dir path which will be mounted inside flexy container
        # This will be root for all flexy housekeeping
        self.flexy_host_dir = os.path.expanduser(
            config.ENV_DATA['flexy_host_dir'], constants.FLEXY_HOST_DIR
        )

        # Path inside container where flexy_host_dir will be mounted
        self.flexy_mnt_container_dir = config.ENV_DATA.get(
            'flexy_mnt_container_dir', constants.FLEXY_MNT_CONTAINER_DIR
        )
        self.flexy_img_url = config.ENV_DATA.get('flexy_img_url')

        # If user has provided an absolute path for env file
        # then we don't clone private-conf repo else
        # we will look for 'flexy_private_conf_url' , if not specified
        # we will go ahead with default 'flexy-ocs-private' repo
        if not config.ENV_DATA.get('flexy-env-file'):
            self.flexy_private_conf_url = config.ENV_DATA.get(
                'flexy_private_conf_url',
                constants.FLEXY_DEFAULT_PRIVATE_CONF_URL
            )
            # Host dir path for private_conf dir where private-conf will be
            # cloned
            self.flexy_host_private_conf_dir_path = os.path.join(
                self.flexy_host_dir, 'flexy-ocs-private'
            )
            self.flexy_env_file = os.path.join(
                self.flexy_host_private_conf_dir_path, constants.FLEXY_DEFAULT_ENV_FILE
            )
        else:
            self.flexy_env_file = config.ENV_DATA['flexy-env-file']

    def deploy_prereq(self):
        """
          Common flexy prerequisites like cloning the private-conf
          repo locally and updating the contents with user supplied
          values

          """
        if not os.path.exists(self.flexy_host_dir):
            os.mkdir(self.flexy_host_dir)
            os.chmod(self.flexy_host_dir, mode=0o777)
        # If user has provided absolute path for env file (through
        # '--flexy-env-file <path>' CLI option)
        # then no need of clone else continue with default
        # private-conf repo and branch

        if not config.ENV_DATA.get('flexy-env-file'):
            self.clone_and_unlock_ocs_private_conf()
            config.FLEXY['VARIABLES_LOCATION'] = os.path.join(
                constants.OPENSHIFT_MISC_BASE, self.template_file
            )
            config.FLEXY['INSTANCE_NAME_PREFIX'] = self.cluster_name
            self.merge_flexy_env()

    def run_container(self, cmd_string):
        """
        Actual container run happens here, a thread will be
        spawned to asynchronously print flexy container logs

        Args:
            cmd_string (str): Podman command line along with options

        """
        logger.info(
            f"Starting Flexy container with options {cmd_string}"
        )
        with subprocess.Popen(
            cmd_string,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True
        ) as p:
            for line in p.stdout:
                logger.info(line)
            if p.returncode:
                logger.error("Flexy command failed")
                raise CalledProcessError(p.returncode, p.args)
            logger.info("Flexy run finished successfully")

    def build_install_cmd(self):
        """
        Build flexy command line for 'deploy' operation

        """
        cmd = shlex.split("sudo podman run --rm=true")
        flexy_container_args = self.build_container_args()
        cmd_string = list2cmdline(cmd + flexy_container_args)
        return cmd_string

    def build_destroy_cmd(self):
        """
        Build flexy command line for 'destroy' operation

        """
        cmd = shlex.split("sudo podman run --rm=true")
        flexy_container_args = self.build_container_args('destroy')
        cmd_string = list2cmdline(cmd + flexy_container_args + ['destroy'])
        return cmd_string

    def build_container_args(self, purpose=''):
        """
        Builds most commonly used arguments for flexy container

        Args:
            purpose (str): purpose for which we are building these args
                eg: destroy, debug. By default it will be empty string
                which turns into 'deploy' mode for flexy

        Returns:
            list: of flexy container args

        """
        args = list()
        args.append(f"--env-file={self.flexy_env_file}")
        args.append(f"-w={self.flexy_mnt_container_dir}")
        # For destroy on NFS mount, relabel=shared will not work
        # with podman hence we will keep 'relable=shared' only for
        # deploy which happens on Jenkins slave's local fs
        if purpose == 'destroy':
            args.append(
                f"--mount=type=bind,source={self.flexy_host_dir},"
                f"destination={self.flexy_mnt_container_dir}"
            )
        else:
            args.append(
                f"--mount=type=bind,source={self.flexy_host_dir},"
                f"destination={self.flexy_mnt_container_dir},relabel=shared"
            )
        args.append(f"{self.flexy_img_url}")
        return args

    def clone_and_unlock_ocs_private_conf(self):
        """
        Clone ocs_private_conf (flexy env and config) repo into
        flexy_host_dir

        """
        clone_repo(self.flexy_private_conf_url, self.flexy_host_private_conf_dir_path)
        # git-crypt unlock /path/to/keyfile
        cp = subprocess.run(
            f'git-crypt unlock {constants.FLEXY_GIT_CRYPT_KEYFILE}',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.flexy_host_private_conf_dir_path
        )
        if cp.returncode:
            raise exceptions.CommandFailed(cp.stderr)
        logger.info("Unlocked the git repo")

    def merge_flexy_env(self):
        """
        Update the ocs-osp.env file with the user supplied values.
        This function assumes that flexy-ocs-private repo has been
        already cloned

        """
        config_parser = configparser.ConfigParser()
        # without this,config parser would convert all
        # uppercase keys to lowercase
        config_parser.optionxform = str

        with open(self.flexy_env_file, "r") as fp:
            # we need to add section to make
            # config parser happy, this will be removed
            # while writing back to file
            file_content = "[root]\n" + fp.read()

        config_parser.read_string(file_content)
        # Iterate over config_parser keys, if same key is present
        # in user supplied dict update config_parser
        for ele in config_parser.items('root'):
            if ele[0] in config.FLEXY:
                # For LAUNCHER_VARS we need to merge the
                # user provided dict with default obtained
                # from env file
                if ele[0] == 'LAUNCHER_VARS':
                    config_parser.set(
                        'root',
                        ele[0],
                        str(
                            merge_dict(
                                yaml.safe_load(config_parser['root'][ele[0]]),
                                config.FLEXY[ele[0]]
                            )
                        )
                    )
                else:
                    config_parser.set('root', ele[0], config.FLEXY[ele[0]])
                logger.info(f"env updated {ele[0]}:{config.FLEXY[ele[0]]}")

        # write the updated config_parser content back to the env file
        tmp_file = os.path.join(
            self.flexy_host_private_conf_dir_path, f"{constants.FLEXY_DEFAULT_ENV_FILE}.tmp"
        )
        with open(tmp_file, "w") as fp:
            f = io.StringIO()
            config_parser.write(f)
            f.seek(0)
            # eliminate first line which has section [root]
            # last line ll have a '\n' so that needs to be
            # removed
            fp.write("".join(f.readlines()[1:-1]))

        # Move this tempfile to original file
        os.rename(
            tmp_file, self.flexy_env_file
        )


class FlexyBaremetalPSI(FlexyBase):
    """
    A specific implementation of Baremetal with PSI using flexy
    """
    def __init__(self):
        super().__init__()
        if not config.ENV_DATA.get('flexy-env-file'):
            if not config.ENV_DATA.get('template_file_path'):
                self.template_file = os.path.join(
                    constants.OPENSHIFT_MISC_BASE,
                    f"aos-{get_ocp_version('_')}",
                    constants.FLEXY_BAREMETAL_UPI_TEMPLATE
                )
            else:
                self.template_file = config.ENV_DATA.get('template_file_path')

    def deploy_prereq(self):
        super().deploy_prereq()

    def deploy(self, log_level='debug'):
        """
        build and invoke flexy deployer here

        Args:
            log_level (str): log level for flexy container

        """
        cmd = self.build_install_cmd()
        run_cmd = " ".join([cmd, log_level])
        self.run_container(run_cmd)

    def destroy(self):
        """
        Invokes flexy container with 'destroy' argument

        """
        cmd = self.build_destroy_cmd()
        self.run_container(cmd)
