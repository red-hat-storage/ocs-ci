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
import re
import sys

from ocs_ci import framework
from ocs_ci.framework import config
from ocs_ci.ocs import constants, node, ocp
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import utils


logger = logging.getLogger(__name__)


def get_environment_info():
    """
    Getting the environment information, Information that will be collected

    Versions:
        OCP - version / build / channel
        OCS - version / build
        Ceph - version
        Rook - version

    Platform:
        BM / VmWare / Cloud provider etc.
        Instance type / architecture
        Cluster name
        User name that run the test

    Return:
      dict: dictionary that contain the environment information

    """
    results = {}
    # getting the name and email  of the user that running the test.
    try:
        user = utils.run_cmd("git config --get user.name").strip()
        email = utils.run_cmd("git config --get user.email").strip()
        results["user"] = f"{user} <{email}>"
    except CommandFailed:
        # if no git user define, the default user is none
        results["user"] = ""

    results["clustername"] = ocp.get_clustername()
    results["platform"] = node.get_provider()
    if results["platform"].lower() not in constants.ON_PREM_PLATFORMS:
        results["platform"] = results["platform"].upper()

    results["ocp_build"] = ocp.get_build()
    results["ocp_channel"] = ocp.get_ocp_channel()
    results["ocp_version"] = utils.get_ocp_version()

    results["ceph_version"] = utils.get_ceph_version()
    results["rook_version"] = utils.get_rook_version()

    results["ocs_build"] = utils.get_ocs_build_number()

    # Getting the instance type for cloud or Arch type for None cloud
    worker_lbl = node.get_nodes(num_of_nodes=1)[0].data["metadata"]["labels"]
    if "beta.kubernetes.io/instance-type" in worker_lbl:
        results["worker_type"] = worker_lbl["beta.kubernetes.io/instance-type"]
    else:
        results["worker_type"] = worker_lbl["kubernetes.io/arch"]

    return results


def get_ocp_version_dict():
    """
    Query OCP to get all information about OCP version.

    Returns:
      dict: ClusterVersion k8s object
    """
    logger.info("collecting ocp version")
    # We use ClusterVersion resource (which is acted upon by openshift
    # cluster-version operator, aka CVO) to get the version of our OCP
    # instance. See also:
    # https://github.com/openshift/cluster-version-operator/blob/master/docs/dev/clusterversion.md
    ocp = OCP(kind="clusterversion")
    version_dict = ocp.get("version")
    return version_dict


def get_ocp_version(version_dict=None):
    """
    Query OCP to get sheer OCP version string. If optional ``version_dict`` is
    specified, the version is extracted from the dict and OCP query is not
    performed.

    See an example of OCP version string::

        '4.10.0-0.nightly-2022-02-09-111355'

    Args:
      version_dict(dict): k8s ClusterVersion dict dump (optional)

    Returns:
      str: full version string of OCP cluster
    """
    version_dict = get_ocp_version_dict()
    version_str = version_dict["status"]["desired"]["version"]
    return version_str


def get_ocs_version():
    """
    Query OCP to get all information about OCS version.

    Returns:
      dict: image_dict with information about images IDs
    """
    logger.info("collecting ocs version")

    # TODO: When OLM (operator-lifecycle-manager) maintains OCS, it will be
    # possible to add a check via CSV (cluster service version) to get OCS
    # version in a similar way as it's done for OCP itself above.
    # Reference: Jose A. Rivera on ocs-qe list.

    # Get all openshift storage related namespaces. Eg. at the time of writing
    # this code (July 2019), there were these storage namespaces:
    #  * openshift-cluster-storage-operator
    #  * openshift-storage
    # TODO: how to do this in upstream where namespace is rook-ceph?
    # TODO: should we use config.ENV_DATA["cluster_namespace"] somehow here?
    storage_namespaces = []
    all_namespaces = OCP(kind="namespace").get()["items"]
    ns_re = re.compile("^openshift.*storage")
    for ns in all_namespaces:
        if ns_re.match(ns["metadata"]["name"]):
            storage_namespaces.append(ns["metadata"]["name"])

    logger.info("found storage namespaces %s", storage_namespaces)

    # Now get the OCS version by asking for version of all container images of
    # all pods in openshift-storage namespace.
    image_dict = {}
    for ns in storage_namespaces:
        ocs = OCP(kind="pod", namespace=ns)
        ocs_pods = ocs.get()
        ns_dict = {}
        for pod in ocs_pods["items"]:
            for container in pod["spec"]["containers"]:
                logger.debug(
                    "container %s (in pod %s) uses image %s",
                    container["name"],
                    pod["metadata"]["name"],
                    container["image"],
                )
            cs_items = pod["status"].get("containerStatuses")
            if cs_items is None:
                pod_name = pod["metadata"]["name"]
                logger.warning(f"pod {pod_name} has no containerStatuses")
                continue
            for cs in cs_items:
                ns_dict.setdefault(cs["image"], set()).add(cs["imageID"])
        image_dict.setdefault(ns, ns_dict)

    logger.debug("ocs version collected")

    return image_dict


def report_ocs_version(cluster_version, image_dict, file_obj):
    """
    Report OCS version via:
     * python logging
     * printing human readable version into file_obj (stdout by default)

    Args:
        cluster_version (dict): cluster version dict
        image_dict (dict): dict of image objects
        file_obj (object):  file object to log information

    """
    # For some reason in IBM cloud the channel is not defined.
    channel = cluster_version["spec"].get("channel", "unknown")
    version = cluster_version["status"]["desired"].get("version", "unknown")
    image = cluster_version["status"]["desired"].get("image", "unknown")

    # log the version
    logger.info("ClusterVersion .spec.channel: %s", channel)
    logger.info("ClusterVersion .status.desired.version: %s", version)
    logger.info("ClusterVersion .status.desired.image: %s", image)

    # write human readable version of the above
    print(f"cluster channel: {channel}", file=file_obj)
    print(f"cluster version: {version}", file=file_obj)
    print(f"cluster image: {image}", file=file_obj)

    for ns, ns_dict in image_dict.items():
        logger.info("storage namespace %s", ns)
        print(f"\nstorage namespace {ns}", file=file_obj)
        for image, image_ids in ns_dict.items():
            logger.info("image %s %s", image, image_ids)
            print(f"image {image}", file=file_obj)
            # TODO: should len(image_ids) == 1?
            if len(image_ids) > 1:
                logger.warning(
                    "there are at least 2 different imageIDs for image %s", image
                )
            for image_id in image_ids:
                print(f" * {image_id}", file=file_obj)


def main():
    """
    Main fuction of version reporting command line tool.
    used by entry point report-version from setup.py
    to invoke this function.

    """
    ap = argparse.ArgumentParser(description="report OCS version for QE purposes")
    ap.add_argument("--cluster-path", required=True, help="Path to cluster directory")
    ap.add_argument(
        "-l",
        "--loglevel",
        choices=["INFO", "DEBUG"],
        default=[],
        nargs=1,
        help="show log messages using given log level",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="show raw version data via pprint insteaf of plaintext",
    )
    args = ap.parse_args()

    if "INFO" in args.loglevel:
        logging.basicConfig(level=logging.INFO)
    elif "DEBUG" in args.loglevel:
        logging.basicConfig(level=logging.DEBUG)

    # make sure that bin dir is in PATH (for oc cli tool)
    utils.add_path_to_env_path(os.path.expanduser(framework.config.RUN["bin_dir"]))

    # set cluster path (for KUBECONFIG required by oc cli tool)
    from ocs_ci.ocs.openshift_ops import OCP

    OCP.set_kubeconfig(
        os.path.join(args.cluster_path, config.RUN["kubeconfig_location"])
    )

    cluster_version = get_ocp_version_dict()
    image_dict = get_ocs_version()

    if args.verbose:
        pprint.pprint(cluster_version)
        pprint.pprint(image_dict)
        return

    report_ocs_version(cluster_version, image_dict, file_obj=sys.stdout)
