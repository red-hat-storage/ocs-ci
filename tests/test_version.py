# -*- coding: utf8 -*-

# TODO: This is a demo of the way we can report he cluster version.
# Code here will be included in some python module when the current changes
# settles down. Also for this report to be actually useful, we need to run this
# as autouse fixture (again, exact way to do this needs to be aligned with
# others, eg. https://github.com/red-hat-storage/ocs-ci/pull/173).


import argparse
import logging
import os.path
import sys

from ocs.ocp import OCP
import ocsci.config

# TODO will require a change when helpers module will be moved somewhere else
# later
from .helpers import create_unique_resource_name

logger = logging.getLogger(__name__)


# TODO: convert this to a fixture later, align with setup, teardown and testing
# @pytest.fixture(scope="session", autouse=True)
def test_version():

    # note: besides reporting the version in the logs, we also create a version
    # file in 'cluster_path' directory, which can copypaste into bugzilla as a
    # base of "version of ocs components" section of a bugreport.
    version_filename = os.path.join(
        ocsci.config.ENV_DATA['cluster_path'],
        create_unique_resource_name("cluster", "version"))
    version_file = open(version_filename, "w")

    # We use ClusterVersion resource (which is acted upon by openshift
    # cluster-version operator, aka CVO) to get the version of our OCP
    # instance. See also:
    # https://github.com/openshift/cluster-version-operator/blob/master/docs/dev/clusterversion.md
    ocp = OCP(kind="clusterversion")
    cluster_version = ocp.get("version")

    # log the version
    logger.info("ClusterVersion .spec.channel: %s", cluster_version["spec"]["channel"])
    logger.info("ClusterVersion .status.desired.version: %s", cluster_version["status"]["desired"]["version"])
    logger.info("ClusterVersion .status.desired.image: %s", cluster_version["status"]["desired"]["image"])

    # write human readable version of the above
    print(f'cluster channel: {cluster_version["spec"]["channel"]}', file=version_file)
    print(f'cluster version: {cluster_version["status"]["desired"]["version"]}', file=version_file)
    print(f'cluster image: {cluster_version["status"]["desired"]["image"]}', file=version_file)
    print('', file=version_file)

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
    for image, image_ids in image_dict.items():
        logger.info("image %s %s", image, image_ids)
        print(f"image {image}", file=version_file)
        # TODO: should len(image_ids) == 1?
        if len(image_ids) > 1:
            logging.warning("there are at least 2 different imageIDs for image %s", image)
        for image_id in image_ids:
            print(f" * {image_id}", file=version_file)
