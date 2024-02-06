from ocs_ci.deployment.helpers.hypershift_base import HyperShiftBase
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment


class HypershiftHostedOCP(HyperShiftBase):
    def __init__(self):
        super(HypershiftHostedOCP, self).__init__()

    class OCPDeployment(BaseOCPDeployment, MetalLBInstaller):
        def __init__(self):
            super(BaseOCPDeployment, self).__init__()
            super(MetalLBInstaller, self).__init__()

        def deploy_prereq(self):
            # metallb rutines
            pass

        def create_config(self):
            pass

        def deploy(self, log_cli_level="DEBUG"):
            pass

        def test_cluster(self):
            pass

        def destroy(self, log_cli_level="DEBUG"):
            pass
