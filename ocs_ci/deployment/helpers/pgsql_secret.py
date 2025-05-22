import logging
import tempfile
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

from ocs_ci.ocs import constants
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


def create_external_pgsql_secret():
    """
    Creates secret for external PgSQL to be used by Noobaa
    """
    secret_data = templating.load_yaml(constants.EXTERNAL_PGSQL_NOOBAA_SECRET_YAML)
    secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    pgsql_data = config.AUTH["pgsql"]
    user = pgsql_data["username"]
    password = pgsql_data["password"]
    host = pgsql_data["host"]
    port = pgsql_data["port"]
    cluster_name = config.ENV_DATA["cluster_name"].replace("-", "_")
    secret_data["stringData"][
        "db_url"
    ] = f"postgres://{user}:{password}@{host}:{port}/nbcore_{cluster_name}"

    secret_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="external_pgsql_noobaa_secret", delete=False
    )
    templating.dump_data_to_temp_yaml(secret_data, secret_data_yaml.name)
    logger.info("Creating external PgSQL Noobaa secret")
    run_cmd(f"oc create -f {secret_data_yaml.name}")
