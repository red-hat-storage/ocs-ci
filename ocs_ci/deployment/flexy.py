"""
All the flexy related classes and functionality lives here
"""
import base64
import binascii
import logging
import os
import yaml
import time

import io
import configparser
import subprocess
from subprocess import CalledProcessError
import shlex
import shutil

from ocs_ci.framework import config, merge_dict
from ocs_ci.ocs import constants
from ocs_ci.utility.proxy import update_kubeconfig_with_proxy_url_for_client
from ocs_ci.utility.utils import (
    clone_repo,
    exec_cmd,
    expose_ocp_version,
    get_ocp_version,
    login_to_mirror_registry,
    wait_for_machineconfigpool_status,
)
from ocs_ci.utility.flexy import (
    configure_allowed_domains_in_proxy,
    load_cluster_info,
)

logger = logging.getLogger(__name__)


class FlexyBase(object):
    """
    A base class for all types of flexy installs
    """

    def __init__(self):
        self.cluster_name = config.ENV_DATA["cluster_name"]
        self.cluster_path = config.ENV_DATA["cluster_path"]

        # Host dir path which will be mounted inside flexy container
        # This will be root for all flexy housekeeping
        self.flexy_host_dir = os.path.expanduser(constants.FLEXY_HOST_DIR_PATH)
        self.flexy_prepare_work_dir()

        # Path inside container where flexy_host_dir will be mounted
        self.flexy_mnt_container_dir = config.ENV_DATA.get(
            "flexy_mnt_container_dir", constants.FLEXY_MNT_CONTAINER_DIR
        )
        self.flexy_img_url = config.ENV_DATA.get(
            "flexy_img_url", constants.FLEXY_IMAGE_URL
        )
        self.template_file = None

        # If user has provided an absolute path for env file
        # then we don't clone private-conf repo else
        # we will look for 'flexy_private_conf_url' , if not specified
        # we will go ahead with default 'flexy-ocs-private' repo
        if not config.ENV_DATA.get("flexy_env_file"):
            self.flexy_private_conf_url = config.ENV_DATA.get(
                "flexy_private_conf_repo", constants.FLEXY_DEFAULT_PRIVATE_CONF_REPO
            )
            self.flexy_private_conf_branch = config.ENV_DATA.get(
                "flexy_private_conf_branch", constants.FLEXY_DEFAULT_PRIVATE_CONF_BRANCH
            )
            # Host dir path for private_conf dir where private-conf will be
            # cloned
            self.flexy_host_private_conf_dir_path = os.path.join(
                self.flexy_host_dir, "flexy-ocs-private"
            )
            self.flexy_env_file = os.path.join(
                self.flexy_host_private_conf_dir_path, constants.FLEXY_DEFAULT_ENV_FILE
            )
        else:
            self.flexy_env_file = config.ENV_DATA["flexy_env_file"]

        if not config.ENV_DATA.get("flexy_env_file"):
            self.template_file = config.FLEXY.get(
                "VARIABLES_LOCATION",
                os.path.join(
                    constants.OPENSHIFT_MISC_BASE,
                    f"aos-{get_ocp_version('_')}",
                    config.ENV_DATA.get("flexy_template", self.default_flexy_template),
                ),
            )

    def deploy_prereq(self):
        """
        Common flexy prerequisites like cloning the private-conf
        repo locally and updating the contents with user supplied
        values

        """
        # If user has provided absolute path for env file (through
        # '--flexy-env-file <path>' CLI option)
        # then no need of clone else continue with default
        # private-conf repo and branch

        if not config.ENV_DATA.get("flexy_env_file"):
            self.clone_and_unlock_ocs_private_conf()
            config.FLEXY["VARIABLES_LOCATION"] = self.template_file
        config.FLEXY["INSTANCE_NAME_PREFIX"] = self.cluster_name
        config.FLEXY["LAUNCHER_VARS"].update(self.get_installer_payload())
        config.FLEXY["LAUNCHER_VARS"].update(
            {
                "vm_type_masters": config.ENV_DATA["master_instance_type"],
                "vm_type_workers": config.ENV_DATA["worker_instance_type"],
                "num_nodes": str(config.ENV_DATA["master_replicas"]),
                "num_workers": str(config.ENV_DATA["worker_replicas"]),
                "ssh_key_name": "openshift-dev",
            }
        )
        config.FLEXY["AVAILABILITY_ZONE_COUNT"] = config.ENV_DATA.get(
            "availability_zone_count", "1"
        )
        config.FLEXY["OPENSHIFT_SSHKEY_PATH"] = config.DEPLOYMENT["ssh_key_private"]
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
        vers = version or config.DEPLOYMENT["installer_version"]
        installer_version = expose_ocp_version(vers)
        payload_img["installer_payload_image"] = ":".join(
            [constants.REGISTRY_SVC, installer_version]
        )
        return payload_img

    def run_container(self, cmd_string):
        """
        Actual container run happens here, a thread will be
        spawned to asynchronously print flexy container logs

        Args:
            cmd_string (str): Podman command line along with options

        """
        logger.info(f"Starting Flexy container with options {cmd_string}")
        with subprocess.Popen(
            cmd_string,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
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
        cmd = shlex.split("podman run --rm=true")
        flexy_container_args = self.build_container_args()
        return cmd + flexy_container_args

    def build_destroy_cmd(self):
        """
        Build flexy command line for 'destroy' operation

        """

        cmd = shlex.split("podman run --rm=true")
        flexy_container_args = self.build_container_args("destroy")
        return cmd + flexy_container_args + ["destroy"]

    def build_container_args(self, purpose=""):
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
        args.append(f"--env-file={constants.FLEXY_ENV_FILE_UPDATED_PATH}")
        args.append(f"-w={self.flexy_mnt_container_dir}")
        # For destroy on NFS mount, relabel=shared will not work
        # with podman hence we will keep 'relabel=shared' only for
        # deploy which happens on Jenkins slave's local fs.
        # Also during destroy we assume that copying of flexy
        # would be done during deployment and we can rely on
        if purpose == "destroy":
            args.append(
                f"--mount=type=bind,source={self.flexy_host_dir},"
                f"destination={self.flexy_mnt_container_dir},relabel=shared"
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
            self.flexy_private_conf_branch,
        )
        # prepare flexy private repo keyfile (if it is base64 encoded)
        keyfile = os.path.expanduser(constants.FLEXY_GIT_CRYPT_KEYFILE)
        try:
            with open(keyfile, "rb") as fd:
                keyfile_content = base64.b64decode(fd.read())
            logger.info(
                "Private flexy repository git crypt keyfile is base64 encoded. "
                f"Decoding it and saving to the same place ({keyfile})"
            )
            with open(keyfile, "wb") as fd:
                fd.write(keyfile_content)
        except binascii.Error:
            logger.info(
                f"Private flexy repository git crypt keyfile is already prepared ({keyfile})."
            )
        # git-crypt unlock /path/to/keyfile
        cmd = f"git-crypt unlock {keyfile}"
        exec_cmd(cmd, cwd=self.flexy_host_private_conf_dir_path)
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
            if key == "LAUNCHER_VARS":
                config_parser.set(
                    "root",
                    key,
                    str(
                        merge_dict(
                            yaml.safe_load(config_parser["root"][key]),
                            config.FLEXY[key],
                        )
                    ),
                )
            else:
                config_parser.set("root", key, f"{config.FLEXY[key]}")

        # write the updated config_parser content to updated env file
        with open(constants.FLEXY_ENV_FILE_UPDATED_PATH, "w") as fp:
            src = io.StringIO()
            dst = io.StringIO()
            config_parser.write(src)
            src.seek(0)
            # config_parser introduces spaces around '='
            # sign, we need to remove that else flexy podman fails
            for line in src.readlines():
                dst.write(line.replace(" = ", "=", 1))
            dst.seek(0)
            # eliminate first line which has section [root]
            # last line ll have a '\n' so that needs to be
            # removed
            fp.write("".join(dst.readlines()[1:-1]))

    def flexy_prepare_work_dir(self):
        """
        Prepare Flexy working directory (flexy-dir):
            - copy flexy-dir from cluster_path to data dir (if available)
            - set proper ownership
        """
        logger.info(f"Prepare flexy working directory {self.flexy_host_dir}.")
        if not os.path.exists(self.flexy_host_dir):
            # if ocs-ci/data were cleaned up (e.g. on Jenkins) and flexy-dir
            # exists in cluster dir, copy it to the data directory, othervise
            # just create empty flexy-dir
            cluster_path_flexy_dir = os.path.join(
                self.cluster_path, constants.FLEXY_HOST_DIR
            )
            if os.path.exists(cluster_path_flexy_dir):
                shutil.copytree(
                    cluster_path_flexy_dir,
                    self.flexy_host_dir,
                    symlinks=True,
                    ignore_dangling_symlinks=True,
                )
            else:
                os.mkdir(self.flexy_host_dir)
        # change the ownership to the uid of user in flexy container
        chown_cmd = (
            f"sudo chown -R {constants.FLEXY_USER_LOCAL_UID} {self.flexy_host_dir}"
        )
        exec_cmd(chown_cmd)

    def flexy_backup_work_dir(self):
        """
        Perform copying of flexy-dir to cluster_path.
        """
        # change ownership of flexy-dir back to current user
        chown_cmd = f"sudo chown -R {os.getuid()}:{os.getgid()} {self.flexy_host_dir}"
        exec_cmd(chown_cmd)
        chmod_cmd = f"sudo chmod -R a+rX {self.flexy_host_dir}"
        exec_cmd(chmod_cmd)
        # mirror flexy work dir to cluster path
        rsync_cmd = f"rsync -av {self.flexy_host_dir} {self.cluster_path}/"
        exec_cmd(rsync_cmd, timeout=1200)

        # mirror install-dir to cluster path (auth directory, metadata.json
        # file and other files)
        install_dir = os.path.join(self.flexy_host_dir, "flexy/workdir/install-dir/")
        rsync_cmd = f"rsync -av {install_dir} {self.cluster_path}/"
        exec_cmd(rsync_cmd)

    def flexy_post_processing(self):
        """
        Perform a few actions required after flexy execution:
        - update global pull-secret
        - login to mirror registry (disconected cluster)
        - configure proxy server (disconnected cluster)
        - configure ntp (if required)
        """
        kubeconfig = os.path.join(
            self.cluster_path, config.RUN.get("kubeconfig_location")
        )

        # Update kubeconfig with proxy-url (if client_http_proxy
        # configured) to redirect client access through proxy server.
        # Since flexy-dir is already copied to cluster-dir, we will update
        # kubeconfig on both places.
        flexy_kubeconfig = os.path.join(
            self.flexy_host_dir,
            constants.FLEXY_RELATIVE_CLUSTER_DIR,
            "auth/kubeconfig",
        )
        update_kubeconfig_with_proxy_url_for_client(kubeconfig)
        update_kubeconfig_with_proxy_url_for_client(flexy_kubeconfig)

        # load cluster info
        load_cluster_info()

        # if on disconnected cluster, perform required tasks
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        if config.DEPLOYMENT.get("disconnected"):
            # login to mirror registry
            login_to_mirror_registry(pull_secret_path)
            # configure additional allowed domains in proxy
            configure_allowed_domains_in_proxy()

        # update pull-secret
        secret_cmd = (
            f"oc set data secret/pull-secret "
            f"--kubeconfig {kubeconfig} "
            f"-n {constants.OPENSHIFT_CONFIG_NAMESPACE} "
            f"--from-file=.dockerconfigjson={pull_secret_path}"
        )
        exec_cmd(secret_cmd)

        if not config.ENV_DATA.get("skip_ntp_configuration", False):
            ntp_cmd = (
                f"oc --kubeconfig {kubeconfig} "
                f"create -f {constants.NTP_CHRONY_CONF}"
            )
            logger.info("Creating NTP chrony")
            exec_cmd(ntp_cmd)
        # sleep here to start update machineconfigpool status
        time.sleep(60)
        wait_for_machineconfigpool_status("all")

    def deploy(self, log_level=""):
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
        # Ensure that flexy workdir will be copied to cluster dir even when
        # Flexy itself fails.
        try:
            self.run_container(cmd)
        except Exception as err:
            logger.error(err)
            raise
        finally:
            self.flexy_backup_work_dir()

        self.flexy_post_processing()

    def destroy(self):
        """
        Invokes flexy container with 'destroy' argument

        """
        cmd = self.build_destroy_cmd()
        try:
            self.run_container(cmd)
        except Exception as err:
            logger.error(err)
            raise
        finally:
            self.flexy_backup_work_dir()


class FlexyBaremetalPSI(FlexyBase):
    """
    A specific implementation of Baremetal with PSI using flexy
    """

    def __init__(self):
        self.default_flexy_template = constants.FLEXY_BAREMETAL_UPI_TEMPLATE
        super().__init__()


class FlexyAWSUPI(FlexyBase):
    """
    A specific implementation of AWS UPI installation using flexy
    """

    def __init__(self):
        self.default_flexy_template = constants.FLEXY_AWS_UPI_TEMPLATE
        super().__init__()
