import tempfile
import argparse
import logging
import datetime
import threading
import os

from ocs_ci.framework import config
from ocs_ci.ocs.constants import CLEANUP_YAML, TEMPLATE_CLEANUP_DIR
from ocs_ci.utility.utils import get_openshift_installer, run_cmd
from ocs_ci.utility import templating
from ocs_ci.utility.aws import AWS
from ocs_ci.cleanup.aws import defaults


FORMAT = (
    '%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
)
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
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


def get_clusters_to_delete(time_to_delete, region_name, prefixes_hours_to_spare):
    """
    Get all cluster names that their EC2 instances running time is greater
    than the specified time to delete

    Args:
        time_to_delete (int): The maximum time in seconds that is allowed
            for clusters to continue running
        region_name (str): The name of the AWS region to delete the resources from
        prefixes_hours_to_spare (dict): Dictionaries of the cluster prefixes to spare
            along with the maximum time in hours that is allowed for spared
            clusters to continue running

    Returns:
        tuple: List of the cluster names (e.g ebenahar-cluster-gqtd4) to be provided to the
            ci-cleanup script and a list of VPCs that are part of cloudformations

    """
    aws = AWS(region_name=region_name)
    clusters_to_delete = list()
    cloudformation_vpcs = list()
    vpcs = aws.ec2_client.describe_vpcs()['Vpcs']
    vpc_ids = [vpc['VpcId'] for vpc in vpcs]
    vpc_objs = [aws.ec2_resource.Vpc(vpc_id) for vpc_id in vpc_ids]
    for vpc_obj in vpc_objs:
        vpc_tags = vpc_obj.tags
        vpc_cloudformation = [
            tag['Value'] for tag in vpc_tags if tag['Key'] == defaults.AWS_CLOUDFORMATION_TAG
        ]
        if vpc_cloudformation:
            cloudformation_vpcs.append(vpc_cloudformation)
            continue
        vpc_name = [tag['Value'] for tag in vpc_tags if tag['Key'] == 'Name'][0]
        cluster_name = vpc_name[:-4]
        vpc_instances = vpc_obj.instances.all()
        if not vpc_instances:
            clusters_to_delete.append(cluster_name)
        for instance in vpc_instances:
            allowed_running_time = time_to_delete
            if instance.state["Name"] == "running":
                for prefix, hours in prefixes_hours_to_spare.items():
                    if prefix in cluster_name:
                        allowed_running_time = int(hours) * 60 * 60
                        break
                launch_time = instance.launch_time
                current_time = datetime.datetime.now(launch_time.tzinfo)
                running_time = current_time - launch_time
                if running_time.seconds > allowed_running_time:
                    clusters_to_delete.append(cluster_name)
                break
    return clusters_to_delete, cloudformation_vpcs


def cluster_cleanup():
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


def aws_cleanup():
    parser = argparse.ArgumentParser(description='AWS overall resources cleanup according to running time')
    parser.add_argument(
        '--hours',
        type=int,
        nargs=1,
        action='append',
        required=True,
        help="Maximum running time of the cluster (in hours). Clusters older than this "
             "will be deleted. The minimum is 10 hours"
    )
    parser.add_argument(
        '--region',
        nargs=1,
        action='append',
        required=False,
        help="The name of the AWS region to delete the resources from"
    )
    parser.add_argument(
        '--force',
        action='store_true',
        required=False,
        help="Force cluster cleanup. "
             "User will not be prompted for confirmation. "
             "WARNING: this utility is destructive, only use this option if "
             "you know what you are doing."
    )
    parser.add_argument(
        '--skip-prefixes',
        action='store_true',
        required=False,
        help="Skip prompt for additional prefixes to spare"
    )
    logging.basicConfig(level=logging.DEBUG)
    args = parser.parse_args()

    if not args.force:
        confirmation = input(
            'Careful! This action could be highly destructive. '
            'Are you sure you want to proceed? '
        )
        assert confirmation == defaults.CONFIRMATION_ANSWER, (
            "Wrong confirmation answer. Exiting"
        )

    prefixes_hours_to_spare = defaults.CLUSTER_PREFIXES_TO_EXCLUDE_FROM_DELETION

    if not args.skip_prefixes:
        prefixes_hours = input(
            "Press Enter if there are no cluster prefixes to spare.\n"
            "If you would like the cleanup to spare specific cluster prefixes, "
            "please enter them along with the time allowed for these to be kept "
            "running, in a dictionary representation.\nAn example: "
            "{\'prefix1\': 36, \'prefix2\': 48}\" "
        )
        if prefixes_hours:
            prefixes_hours_to_spare = eval(prefixes_hours)
    time_to_delete = args.hours[0][0]
    assert time_to_delete > defaults.MINIMUM_CLUSTER_RUNNING_TIME_FOR_DELETION, (
        "Number of hours is lower than the required minimum. Exiting"
    )
    time_to_delete = time_to_delete * 60 * 60
    region = defaults.AWS_REGION if not args.region else args.region[0][0]
    clusters_to_delete, cloudformation_vpcs = get_clusters_to_delete(
        time_to_delete=time_to_delete, region_name=region,
        prefixes_hours_to_spare=prefixes_hours_to_spare,
    )

    if not clusters_to_delete:
        logger.info("No clusters to delete")
    else:
        get_openshift_installer()
    procs = []
    for cluster in clusters_to_delete:
        cluster_name = cluster.rsplit('-', 1)[0]
        logger.info(f"Deleting cluster {cluster_name}")
        proc = threading.Thread(target=cleanup, args=(cluster_name, cluster))
        proc.start()
        procs.append(proc)
    for p in procs:
        p.join()
    if cloudformation_vpcs:
        logger.warning(
            "The following cloudformation VPCs were found: %s",
            cloudformation_vpcs
        )
