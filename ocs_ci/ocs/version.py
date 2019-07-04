# -*- coding: utf8 -*-

"""
Version reporting module for OCS QE purposes:

 * logging version of OCS/OCP stack for every test run
 * generating version report directly usable in bug reports

It asks openshift for:

 * ClusterVersion resource
 * image identifiers of all containers running in openshift storage related
   namespaces
 * rpm package versions of few selected components (such as rook or ceph, if
   given pod is running)
"""


import argparse
import logging
import os.path
import pprint
import sys

from ocs_ci import framework
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import utils


logger = logging.getLogger(__name__)


def get_ocs_version():
    """
    Query OCP to get all information about OCS version.

    Returns:
     * dict with ClusterVersion k8s object
     * image_dict with information about images IDs
    """
    logger.info("collecting ocs version")

    # We use ClusterVersion resource (which is acted upon by openshift
    # cluster-version operator, aka CVO) to get the version of our OCP
    # instance. See also:
    # https://github.com/openshift/cluster-version-operator/blob/master/docs/dev/clusterversion.md
    ocp = OCP(kind="clusterversion")
    cluster_version = ocp.get("version")

    # TODO: When OLM (operator-lifecycle-manager) maintains OCS, it will be
    # possible to add a check via CSV (cluster service version) to get OCS
    # version in a similar way as it's done for OCP itself above.
    # Reference: Jose A. Rivera on ocs-qe list.

    # Now get the OCS version by asking for version of all container images of
    # all pods in openshift-storage namespace.
    ocs = OCP(kind="pod", namespace="openshift-storage")
    ocs_pods = ocs.get()
    image_dict = {}
    for pod in ocs_pods['items']:
        for container in pod['spec']['containers']:
            logger.debug(
                'container %s (in pod %s) uses image %s',
                container['name'],
                pod['metadata']['name'],
                container['image'])
        for cs in pod['status']['containerStatuses']:
            image_dict.setdefault(cs['image'], set()).add(cs['imageID'])

    logger.debug("ocs version collected")

    return cluster_version, image_dict


def report_ocs_version(cluster_version, image_dict, file_obj):
    """
    Report OCS version via:

     * python logging
     * printing human readable version into file_obj (stdout by default)
    """
    # log the version
    logger.info("ClusterVersion .spec.channel: %s", cluster_version["spec"]["channel"])
    logger.info("ClusterVersion .status.desired.version: %s", cluster_version["status"]["desired"]["version"])
    logger.info("ClusterVersion .status.desired.image: %s", cluster_version["status"]["desired"]["image"])

    # write human readable version of the above
    print(f'cluster channel: {cluster_version["spec"]["channel"]}', file=file_obj)
    print(f'cluster version: {cluster_version["status"]["desired"]["version"]}', file=file_obj)
    print(f'cluster image: {cluster_version["status"]["desired"]["image"]}', file=file_obj)
    print('', file=file_obj)

    for image, image_ids in image_dict.items():
        logger.info("image %s %s", image, image_ids)
        print(f"image {image}", file=file_obj)
        # TODO: should len(image_ids) == 1?
        if len(image_ids) > 1:
            logging.warning("there are at least 2 different imageIDs for image %s", image)
        for image_id in image_ids:
            print(f" * {image_id}", file=file_obj)


def main():
    """
    Main fuction of version reporting command line tool.

    Based on suggestion from
    https://github.com/red-hat-storage/ocs-ci/issues/356
    """
    ap = argparse.ArgumentParser(
        description="report OCS version for QE purposes")
    ap.add_argument(
        "--cluster-path",
        required=True,
        help="Path to cluster directory"
        )
    ap.add_argument(
        "-l",
        "--loglevel",
        choices=["INFO", "DEBUG"],
        default=[],
        nargs=1,
        help="show log messages using given log level")
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="show raw version data via pprint insteaf of plaintext")
    args = ap.parse_args()

    if "INFO" in args.loglevel:
        logging.basicConfig(level=logging.INFO)
    elif "DEBUG" in args.loglevel:
        logging.basicConfig(level=logging.DEBUG)

    # make sure that bin dir is in PATH (for oc cli tool)
    utils.add_path_to_env_path(os.path.expanduser(
        framework.config.RUN['bin_dir']))

    # set cluster path (for KUBECONFIG required by oc cli tool)
    from ocs_ci.ocs.openshift_ops import OCP
    OCP.set_kubeconfig(
        os.path.join(args.cluster_path, config.RUN['kubeconfig_location']))

    cluster_version, image_dict = get_ocs_version()

    if args.verbose:
        pprint.pprint(cluster_version)
        pprint.pprint(image_dict)
        return

    report_ocs_version(cluster_version, image_dict, file_obj=sys.stdout)
