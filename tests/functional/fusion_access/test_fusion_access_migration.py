"""
Fusion Access migration-tool test.

Automates the migration Job flow:

  1. Export cluster pull-secret from openshift-config
  2. ``oc image extract`` resources.yaml from DEPLOYMENT.migration_tool_image
  3. ``oc image extract`` migration-job.yaml from the same image
  4. ``oc apply -f resources.yaml``
  5. ``oc apply -f migration-job.yaml``
  6. Wait for Job(s) in migration-job.yaml to complete

The container image is intentionally not hardcoded — custom builds use different
tags. Provide ``DEPLOYMENT.migration_tool_image`` via conf (see
``conf/ocsci/fusion_access_migration.yaml``); the build team supplies the
pullspec when ready.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fusion_access_required,
    ignore_leftovers,
    yellow_squad,
)
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.helpers.fusion_access_migration_helpers import (
    get_migration_tool_image,
    run_fusion_access_migration_tool,
)

logger = logging.getLogger(__name__)


def _skip_without_migration_tool_image():
    """Skip when the build-provided migration-tool image is not configured."""
    try:
        image = get_migration_tool_image()
    except ValueError as e:
        pytest.skip(str(e))
    return image


@yellow_squad
@tier1
@fusion_access_required
@ignore_leftovers
class TestFusionAccessMigration(ManageTest):
    """
    Run the Fusion Access migration-tool Job against a premigration FA cluster.
    """

    def test_fusion_access_migration_tool_job(self):
        """
        Extract and apply migration-tool manifests, then wait for Job success.

        Prerequisites:
          - Fusion Access premigration setup (operator + storage cluster; FS/VM
            from section B when available)
          - ``DEPLOYMENT.migration_tool_image`` set to the build-team image

        Steps:
          1. Export pull-secret from openshift-config
          2. Extract /migration-tool/resources.yaml from the configured image
          3. Extract /migration-tool/migration-job.yaml from the same image
          4. Apply resources.yaml
          5. Apply migration-job.yaml
          6. Wait for Job(s) defined in migration-job.yaml to complete
        """
        image = _skip_without_migration_tool_image()
        logger.test_step(f"Run Fusion Access migration tool using image '{image}'")
        result = run_fusion_access_migration_tool(
            image=image,
            wait_for_jobs=True,
            job_timeout=config.DEPLOYMENT.get("migration_tool_job_timeout", 1800),
            insecure=bool(config.DEPLOYMENT.get("migration_tool_insecure", False)),
        )

        logger.assertion(
            f"Migration manifests applied from image '{result['image']}': "
            f"resources={result['resources_yaml']}, "
            f"job_yaml={result['migration_job_yaml']}, jobs={result['jobs']}"
        )
        assert result["resources_yaml"], "resources.yaml path missing after extract"
        assert result[
            "migration_job_yaml"
        ], "migration-job.yaml path missing after extract"
        if result["jobs"]:
            logger.info(f"Migration Job(s) completed successfully: {result['jobs']}")
        else:
            logger.warning(
                "No Job objects were found in migration-job.yaml; "
                "apply succeeded but completion was not asserted"
            )
