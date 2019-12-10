import tempfile
import argparse
import logging
import threading
import os

from ocs_ci.framework import config
from ocs_ci.ocs.constants import CLEANUP_YAML, TEMPLATE_CLEANUP_DIR
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility import templating


logger = logging.getLogger(__name__)


def cleanup(cluster_name, cluster_id):
    """
    Cleanup existing cluster in AWS

    Args:
        cluster_name (str): Name of the cluster
        cluster_id (str): Cluster id to cleanup

    """
    data = {'cluster_name': cluster_name, 'cluster_id': cluster_id}
    template = templating.Templating(base_path=TEMPLATE_CLEANUP_DIR)
    cleanup_template = template.render_template(CLEANUP_YAML, data)
    cleanup_path = tempfile.mkdtemp(prefix='cleanup_')
    cleanup_file = os.path.join(cleanup_path, 'metadata.json')
    with open(cleanup_file, "w") as temp:
        temp.write(cleanup_template)
    bin_dir = os.path.expanduser(config.RUN['bin_dir'])
    oc_bin = os.path.join(bin_dir, "openshift-install")
    logger.info(f"cleaning up {cluster_id}")
    run_cmd(f"{oc_bin} destroy cluster --dir {cleanup_path} --log-level=debug")


def main():
    parser = argparse.ArgumentParser(description='Cleanup AWS Resource')
    parser.add_argument(
        '--cluster',
        nargs=1,
        action='append',
        required=True,
        help="Cluster name tag"
    )
    logging.basicConfig(level=logging.DEBUG)
    args = parser.parse_args()
    procs = []
    for id in args.cluster:
        cluster_name = id[0].rsplit('-', 1)[0]
        logger.info(f"cleaning up {id[0]}")
        proc = threading.Thread(target=cleanup, args=(cluster_name, id[0]))
        proc.start()
        procs.append(proc)
    for p in procs:
        p.join()
