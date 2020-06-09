"""
All the flexy related classes and functionality lives here
"""
import logging
import os
import sys

import io
import configparser
import subprocess
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework import config, merge_dict
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    get_ocp_version, TimeoutSampler, clone_repo
)
from ocs_ci.ocs import exceptions

logger = logging.getLogger(__name__)


class FlexyBase(object):
    """
    A base class for all types of flexy installs
    """
    def __init__(self):
        # Host dir path which will be mounted inside flexy container
        # This will be root for all flexy housekeeping
        self.cluster_name = config.ENV_DATA['cluster_name']
        self.flexy_mnt_host_dir = os.path.expanduser(
            config.ENV_DATA['flexy_mnt_host_dir']
        )
        # Host dir path for private_conf dir
        self.flexy_host_private_conf_dir_path = os.path.join(
            self.flexy_mnt_host_dir, config.ENV_DATA['flexy_host_private_conf_dir']
        )
        # Path inside container where flexy_mnt_host_dir will be mounted
        self.flexy_mnt_container_dir = config.ENV_DATA.get(
            'flexy_mnt_container_dir', constants.FLEXY_MNT_CONTAINER_DIR
        )
        # Path inside container where flexy_private_conf_dir will be mounted
        # same value needs to be set for BUSHSLICER_PRIVATE_DIR in env file
        # set BUSHSLICER_PRIVATE_DIR in ocs-osp.env itself
        self.flexy_mnt_private_conf_dir = config.ENV_DATA.get(
            'flexy_mnt_private_conf_dir', constants.FLEXY_MNT_PRIVATE_CONF_DIR
        )
        self.flexy_openshift_misc_url = config.ENV_DATA.get(
            'flexy_openshift_misc_url', constants.OCP_QE_MISC_REPO
        )
        self.flexy_private_conf_url = config.ENV_DATA.get(
            'flexy_private_conf_url', constants.FLEXY_PRIVATE_CONF_URL
        )
        self.flexy_img_url = config.ENV_DATA.get('flexy_img_url')
        self.flex_env_file = os.path.join(
            self.flexy_host_private_conf_dir_path, constants.FLEXY_DEFAULT_ENV_FILE
        )
        self.flexy_log_file = os.path.expanduser(
            config.ENV_DATA.get('flexy_log_file')
        )
        self.exitThread = False

    def run_container(self, cmd_string):
        """
        Actualy container run happens here, a thread will be
        spawned to asynchronously print flexy container logs

        """
        logger.info(
            f"Starting Flexy container with options {cmd_string}"
        )
        # Launch a seperate thread for getting the log file contents
        # simultaneously while flexy container running else logging will be
        # blocked till container terminates
        with ThreadPoolExecutor(max_workers=1) as tp:
            future = tp.submit(self.get_log)
            cp = subprocess.run(
                cmd_string, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            if cp.returncode:
                if cp.stderr:
                    logger.error(cp.stderr)
                raise exceptions.CommandFailed("Flexy run failed")
            logger.info("Flexy run finished successfully")
            self.exitThread = True
        for res in TimeoutSampler(60, 1, future.done):
            if not res:
                logger.info("Waiting for logger thread to join")

    def build_install_cmd(self):
        """
        Build flexy command line for 'deploy' operation

        """
        cmd = "sudo podman run --rm=true"
        flexy_container_args = self.build_container_args()
        cmd_string = " ".join([cmd, " ".join(flexy_container_args)])
        return cmd_string

    def build_destroy_cmd(self):
        """
        Build flexy command line for 'destroy' operation

        """
        cmd = "sudo podman run --rm=true"
        flexy_container_args = self.build_container_args()
        cmd_string = " ".join([cmd, " ".join(flexy_container_args), 'destroy'])
        return cmd_string

    def get_log(self):
        """
        This function runs in a seperate thread and continuously reads
        logs from flexy podman

        """
        for res in TimeoutSampler(20, 1, os.path.exists, self.flexy_log_file):
            if res:
                break
        log_fd = open(self.flexy_log_file, "r")

        while True:
            line = log_fd.readline()
            if line:
                logger.info(line.strip())
            if self.exitThread:
                break
        logger.info("Logger thread done printing flexy logs")
        # exit only this thread
        sys.exit()

    def build_container_args(self):
        """
        Builds most commonly used arguments for flexy container

        """
        args = list()
        args.append(f"--log-opt={self.flexy_log_file}")
        args.append(f"--env-file={self.flex_env_file}")
        args.append(f"-w={self.flexy_mnt_container_dir}")
        args.append(
            f"--mount=type=bind,source={self.flexy_mnt_host_dir},"
            f"destination=f{self.flexy_mnt_container_dir},relabel=shared"
        )
        args.append(
            f"--mount=type=bind,source={self.flexy_host_private_conf_dir_path},"
            f"destination={self.flexy_mnt_private_conf_dir},relabel=shared"
        )
        args.append(f"{self.flexy_img_url}")
        return args

    def clone_and_unlock_ocs_private_conf(self):
        """
        Clone ocs_private_conf (flexy env and config) repo into
        flexy_mnt_host_dir

        """
        clone_repo(self.flexy_private_conf_url, self.flexy_host_private_conf_dir_path)
        # git-crypt unlock /path/to/keyfile
        old_cwd = os.getcwd()
        os.chdir(self.flexy_host_private_conf_dir_path)
        cp = subprocess.run(
            f'git-crypt unlock {constants.FLEXY_GIT_CRYPT_KEYFILE}',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        if cp.returncode:
            raise exceptions.CommandFailed(cp.stderr)
        logger.info("Unlocked the git repo")
        os.chdir(old_cwd)

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
        env_file = os.path.join(
            self.flexy_host_private_conf_dir_path, constants.FLEXY_DEFAULT_ENV_FILE
        )

        with open(env_file, "r") as fp:
            # we need to add section to make
            # config parser happy, this will be removed
            # while writing back to file
            file_content = "[root]\n" + fp.read()

        config_parser.read_string(file_content)
        # Iterate over config_parser keys, if same key is present
        # in user supplied dict update config_parser
        for ele in config_parser.items('root'):
            if ele[0] in config.ENV_DATA:
                # For LAUNCHER_VARS we need to merge the
                # user provided dict with default obtained
                # from env file
                if ele[0] == 'LAUNCHER_VARS':
                    merge_dict(config.ENV_DATA[ele[0]], ele[1])
                config_parser['root'][ele[0]] = config.ENV_DATA[ele[0]]
                logger.info(f"env updated {ele[0]}:{config.ENV_DATA[ele[0]]}")

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
            tmp_file, env_file
        )


class FlexyBaremetalPSI(FlexyBase):
    """
    A specific implementation of Baremetal with PSI using flexy
    """
    def __init__(self):
        super().__init__()
        if not config.ENV_DATA.get('template_file_path'):
            self.template_file = os.path.join(
                constants.OPENSHIFT_MISC_BASE,
                f"aos-{get_ocp_version('_')}",
                constants.FLEXY_BAREMETAL_UPI_TEMPLATE
            )
        else:
            self.template_file = config.ENV_DATA.get('template_file_path')

    def deploy_prereq(self):
        """
        Common flexy prerequisites like cloning the private-conf
        repo locally and updating the contents with user supplied
        values

        """
        if not os.path.exists(self.flexy_mnt_host_dir):
            os.mkdir(self.flexy_mnt_host_dir)
            os.chmod(self.flexy_mnt_host_dir, mode=0o777)
        self.clone_and_unlock_ocs_private_conf()
        config.ENV_DATA['VARIABLES_LOCATION'] = os.path.join(
            constants.OPENSHIFT_MISC_BASE, self.template_file
        )
        config.ENV_DATA['INSTANCE_NAME_PREFIX'] = self.cluster_name
        self.merge_flexy_env()

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
