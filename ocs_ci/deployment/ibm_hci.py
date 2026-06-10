from ocs_ci.deployment.on_prem import OnPremDeploymentBase, IPIOCPDeployment


class IBM_HCI_IPI(OnPremDeploymentBase):
    """
    A class to handle IBM HCI IPI specific deployment
    """

    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(IBM_HCI_IPI, self).__init__()
