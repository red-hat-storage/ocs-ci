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
    fusion.verify(sleep=0)

    # JUnit report custom properties
    suite_props = init.get_test_suite_props()
    case_props = init.get_test_case_props()

    @create_junit_report(
        "FusionDataFoundationDeployment",
        "fusion_data_foundation_deployment",
        suite_props,
        case_props,
    )
    def fdf_deployment():
        fdf = FusionDataFoundationDeployment()
        fdf.deploy()

    # FDF deployment
    exit_code = fdf_deployment()
    sys.exit(exit_code)
