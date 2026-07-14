import sys

from ocs_ci.deployment.fusion import FusionDeployment
from ocs_ci.utility.framework.fusion_fdf_init import Initializer, create_junit_report


def main(argv=None):
    # Retrieve provided args from CLI
    args = argv or sys.argv[1:]

    # Framework initialization
    init = Initializer("fusion")
    parsed_args = init.init_cli(args)
    init.init_config(parsed_args)
    init.init_logging()
    init.set_cluster_connection()

    # JUnit report custom properties
    suite_props = init.get_test_suite_props()
    case_props = init.get_test_case_props()

    @create_junit_report(
        "FusionDeployment", "fusion_deployment", suite_props, case_props
    )
    def fusion_deployment():
        fusion = FusionDeployment()
        fusion.deploy()

    # Fusion deployment
    exit_code = fusion_deployment()
    sys.exit(exit_code)
