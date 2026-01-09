from ocs_ci.deployment.on_prem import OnPremDeploymentBase, IPIOCPDeployment


class IMB_HCI_IPI(OnPremDeploymentBase):
    """
    A class to handle RHV IPI specific deployment
    """

    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(IMB_HCI_IPI, self).__init__()
