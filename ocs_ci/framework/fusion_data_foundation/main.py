import sys

from ocs_ci.deployment.fusion import FusionDeployment
from ocs_ci.deployment.fusion_data_foundation import FusionDataFoundationDeployment
from ocs_ci.utility.framework.fusion_fdf_init import Initializer, create_junit_report


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
    fdf_deployment()


@create_junit_report(
    "FusionDataFoundationDeployment", "fusion_data_foundation_deployment"
)
def fdf_deployment():
    fdf = FusionDataFoundationDeployment()
    fdf.deploy()
