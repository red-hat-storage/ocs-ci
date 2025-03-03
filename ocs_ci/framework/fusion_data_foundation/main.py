import sys

from ocs_ci.deployment.fusion import FusionDeployment
from ocs_ci.deployment.fusion_data_foundation import FusionDataFoundationDeployment
from ocs_ci.utility.framework.fusion_fdf_init import Initializer


def main(argv=None):
    # Retrieve provided args from CLI
    args = argv or sys.argv[1:]

    # Framework initialization
    init = Initializer("fdf")
    parsed_args = init.init_cli(args)
    init.init_config(parsed_args)
    init.init_logging()
    init.set_cluster_connection()

    # Verify fusion is deployed
    fusion = FusionDeployment()
    fusion.verify()

    # FDF deployment
    fdf = FusionDataFoundationDeployment()
    fdf.deploy()
