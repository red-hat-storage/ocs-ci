import tempfile
import logging
import yaml

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd
from subprocess import run, CalledProcessError
log = logging.getLogger(__name__)


class MixWorkload(object):
    """
    Run mixed workload as defined in yaml
    """
    def __init__(self, branch='master', workload_name=None):
        """
        setup the mix workload using ocs-workloads

        repo(str): repo location of the workload
        branch(str): branch to use from repo
        workload_name(str): name of the workload dir

        """
        self.repo = constants.OCS_WORKLOADS
        self.branch = branch
        self.workload_name = workload_name
        self.workload_is_setup = False
        self._setup()

    def run(self):
        """
        Run the workload as defined in yaml

        """
        for cmd in self.workload['cmd']:
            log.info(f"running cmd {cmd}")
            run(cmd, shell=True, cwd=self.workload_dir, check=self.workload['check-for-errors'])

        if self.workload.get('ocp_upgrade'):
            log.info("Invoking OCP upgrade with mixed workload")
            upgrade_cmd = f"oc adm upgrade --to-image={self.workload['ocp_version']} --force"
            run_cmd(upgrade_cmd)

    def _setup(self):
        """
        setup the workload
        """
        self.dir = tempfile.mkdtemp(prefix='mix_')
        try:
            log.info(f'cloning ocs-workload in {self.dir}')
            git_clone_cmd = f'git clone -b {self.branch} {self.repo} '
            run(
                git_clone_cmd,
                shell=True,
                cwd=self.dir,
                check=True
            )

        except (CommandFailed, CalledProcessError)as cf:
            log.error(f'Error during cloning of {self.repo}')
            raise cf
        self.workload = yaml.safe_load(open(self.workload_name, 'r'))
        self.workload_dir = f"{self.dir}/ocs-workloads/{self.workload['folder']}"
