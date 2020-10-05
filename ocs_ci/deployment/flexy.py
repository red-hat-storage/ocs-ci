"""
All the flexy related classes and functionality lives here
"""
import logging
import os
import yaml

import io
import configparser
import subprocess
from subprocess import CalledProcessError
import shlex
import shutil

from ocs_ci.framework import config, merge_dict
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    get_ocp_version, clone_repo, run_cmd, expose_ocp_version
)
from ocs_ci.ocs import exceptions

logger = logging.getLogger(__name__)


class FlexyBase(object):
    """
    A base class for all types of flexy installs
    """
    def __init__(self):
        self.cluster_name = config.ENV_DATA['cluster_name']
        self.cluster_path = config.ENV_DATA['cluster_path']
        # Host dir path which will be mounted inside flexy container
        # This will be root for all flexy housekeeping
        self.flexy_host_dir = os.path.expanduser(
            constants.FLEXY_HOST_DIR_PATH
        )

        # Path inside container where flexy_host_dir will be mounted
        self.flexy_mnt_container_dir = config.ENV_DATA.get(
            'flexy_mnt_container_dir', constants.FLEXY_MNT_CONTAINER_DIR
        )
        self.flexy_img_url = config.ENV_DATA.get(
            'flexy_img_url',
            constants.FLEXY_IMAGE_URL
        )
        self.template_file = None

        # If user has provided an absolute path for env file
        # then we don't clone private-conf repo else
        # we will look for 'flexy_private_conf_url' , if not specified
        # we will go ahead with default 'flexy-ocs-private' repo
        if not config.ENV_DATA.get('flexy_env_file'):
            self.flexy_private_conf_url = config.ENV_DATA.get(
                'flexy_private_conf_repo',
                constants.FLEXY_DEFAULT_PRIVATE_CONF_REPO
            )
            self.flexy_private_conf_branch = config.ENV_DATA.get(
                'flexy_private_conf_branch',
                constants.FLEXY_DEFAULT_PRIVATE_CONF_BRANCH
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
            self.flexy_env_file = config.ENV_DATA['flexy_env_file']

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

        if not config.ENV_DATA.get('flexy_env_file'):
            self.clone_and_unlock_ocs_private_conf()
            config.FLEXY['VARIABLES_LOCATION'] = self.template_file
        config.FLEXY['INSTANCE_NAME_PREFIX'] = self.cluster_name
        config.FLEXY['LAUNCHER_VARS'].update(
            self.get_installer_payload()
        )
        self.merge_flexy_env()

    def get_installer_payload(self, version=None):
        """
        A proper installer payload url required for flexy
        based on DEPLOYMENT['installer_version'].
        If 'nigtly' is present then we will use registry.svc to get latest
        nightly else if '-ga' is present then we will look for
        ENV_DATA['installer_payload_image']

        """
        payload_img = {"installer_payload_image": None}
        vers = version or config.DEPLOYMENT['installer_version']
        installer_version = expose_ocp_version(vers)
        payload_img['installer_payload_image'] = (
            ":".join([constants.REGISTRY_SVC, installer_version])
        )
        return payload_img

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
                logger.info(line.strip())
            p.communicate()
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
        return cmd + flexy_container_args

    def build_destroy_cmd(self):
        """
        Build flexy command line for 'destroy' operation

        """
        cmd = shlex.split("sudo podman run --rm=true")
        flexy_container_args = self.build_container_args('destroy')
        return cmd + flexy_container_args + ['destroy']

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
        args.append(f"--env-file={constants.FLEXY_ENV_FILE_UPDATED}")
        args.append(f"-w={self.flexy_mnt_container_dir}")
        # For destroy on NFS mount, relabel=shared will not work
        # with podman hence we will keep 'relabel=shared' only for
        # deploy which happens on Jenkins slave's local fs.
        # Also during destroy we assume that copying of flexy
        # would be done during deployment and we can rely on
        if purpose == 'destroy':
            if self.is_jenkins_mount():
                flexy_dir = os.path.join(
                    constants.JENKINS_NFS_CURRENT_CLUSTER_DIR,
                    constants.FLEXY_HOST_DIR
                )
                if not os.path.exists(flexy_dir):
                    raise exceptions.FlexyDataNotFound(
                        "Failed to find flexy data"
                    )
            else:
                flexy_dir = self.flexy_host_dir
            args.append(
                f"--mount=type=bind,source={flexy_dir},"
                f"destination={self.flexy_mnt_container_dir}"
                f'{[",relabel=shared", ""][self.is_jenkins_mount()]}'
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
        clone_repo(
            self.flexy_private_conf_url,
            self.flexy_host_private_conf_dir_path,
            self.flexy_private_conf_branch
        )
        # git-crypt unlock /path/to/keyfile
        cmd = (
            f'git-crypt unlock '
            f'{os.path.expanduser(constants.FLEXY_GIT_CRYPT_KEYFILE)}'
        )
        run_cmd(cmd, cwd=self.flexy_host_private_conf_dir_path)
        logger.info("Unlocked the git repo")

    def merge_flexy_env(self):
        """
        Update the Flexy env file with the user supplied values.
        This function assumes that the flexy_env_file is available
        (e.g. flexy-ocs-private repo has been already cloned).

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
        # add or update all values from config.FLEXY section into Flexy env
        # configuration file
        for key in config.FLEXY:
            logger.info(f"Flexy env file - updating: {key}={config.FLEXY[key]}")
            # For LAUNCHER_VARS we need to merge the
            # user provided dict with default obtained
            # from env file
            if key == 'LAUNCHER_VARS':
                config_parser.set(
                    'root',
                    key,
                    str(
                        merge_dict(
                            yaml.safe_load(config_parser['root'][key]),
                            config.FLEXY[key]
                        )
                    )
                )
            else:
                config_parser.set('root', key, f"{config.FLEXY[key]}")

        # write the updated config_parser content to updated env file
        with open(constants.FLEXY_ENV_FILE_UPDATED, "w") as fp:
            src = io.StringIO()
            dst = io.StringIO()
            config_parser.write(src)
            src.seek(0)
            # config_parser introduces spaces around '='
            # sign, we need to remove that else flexy podman fails
            for line in src.readlines():
                dst.write(line.replace(' = ', '=', 1))
            dst.seek(0)
            # eliminate first line which has section [root]
            # last line ll have a '\n' so that needs to be
            # removed
            fp.write("".join(dst.readlines()[1:-1]))

    def flexy_post_processing(self):
        """
        Perform copying of flexy-dir to nfs mount
        and do this only if its jenkins run

        """
        auth_file = 'flexy/workdir/install-dir/auth/kubeconfig'
        secret_cmd = (
            f"oc set data secret/pull-secret "
            f"--kubeconfig {self.flexy_host_dir}/{auth_file} "
            f"-n {constants.OPENSHIFT_CONFIG_NAMESPACE} "
            f"--from-file=.dockerconfigjson={constants.DATA_DIR}/pull-secret"
        )
        abs_cluster_path = os.path.abspath(self.cluster_path)
        flexy_cluster_path = os.path.join(
            self.flexy_host_dir, 'flexy/workdir/install-dir'
        )
        if os.path.exists(abs_cluster_path):
            os.rmdir(abs_cluster_path)
        # Check whether its a jenkins run
        if self.is_jenkins_mount():
            flexy_nfs_path = os.path.join(
                constants.JENKINS_NFS_CURRENT_CLUSTER_DIR,
                constants.FLEXY_HOST_DIR
            )
            if not os.path.exists(flexy_nfs_path):
                shutil.copytree(
                    self.flexy_host_dir,
                    flexy_nfs_path,
                    symlinks=True,
                    ignore_dangling_symlinks=True
                )
                chmod = (
                    f"chmod -R 777 {flexy_nfs_path}"
                )
                run_cmd(chmod)
                logger.info(
                    f"Symlinking {abs_cluster_path} to {flexy_nfs_path}"
                )
                os.symlink(
                    os.path.join(
                        flexy_nfs_path,
                        constants.FLEXY_RELATIVE_CLUSTER_DIR
                    ),
                    abs_cluster_path
                )
                # Apply pull secrets on ocp cluster
                run_cmd(secret_cmd)
        else:
            # recursively change permissions
            # for all the subdirs
            chmod = f"sudo chmod -R 777 {constants.FLEXY_HOST_DIR_PATH}"
            run_cmd(chmod)
            logger.info(
                f"Symlinking {flexy_cluster_path, abs_cluster_path}"
            )
            os.symlink(flexy_cluster_path, abs_cluster_path)
            run_cmd(secret_cmd)
        if not config.ENV_DATA.get('skip_ntp_configuration', False):
            ntp_cmd = (
                f"oc --kubeconfig {self.flexy_host_dir}/{auth_file} "
                f"create -f {constants.NTP_CHRONY_CONF}"
            )
            logger.info("Creating NTP chrony")
            run_cmd(ntp_cmd)

    def is_jenkins_mount(self):
        """
        Find if this is jenkins run based on current-cluster-dir and
        NFS mount

        Returns:
            bool: True if this is jenkins run else False

        """
        return (
            os.path.exists(constants.JENKINS_NFS_CURRENT_CLUSTER_DIR)
            and os.path.ismount(constants.JENKINS_NFS_CURRENT_CLUSTER_DIR)
        )


class FlexyBaremetalPSI(FlexyBase):
    """
    A specific implementation of Baremetal with PSI using flexy
    """
    def __init__(self):
        super().__init__()
        if not config.ENV_DATA.get('flexy_env_file'):
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

    def deploy(self, log_level=''):
        """
        build and invoke flexy deployer here

        Args:
            log_level (str): log level for flexy container

        """
        # Ignoring log_level for now as flexy
        # only works with 'debug' option
        if log_level:
            pass
        cmd = self.build_install_cmd()
        self.run_container(cmd)
        super().flexy_post_processing()

    def destroy(self):
        """
        Invokes flexy container with 'destroy' argument

        """
        cmd = self.build_destroy_cmd()
        self.run_container(cmd)
