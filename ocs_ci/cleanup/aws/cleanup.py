import tempfile
import argparse
import logging
import datetime
import threading
import os
import re

from dateutil.parser import parse
from botocore.exceptions import ClientError
from ocs_ci.framework import config

from ocs_ci.utility import utils
from ocs_ci.cleanup.aws.defaults import MANAGED_SERVICE_STRUCTURE
from datetime import timezone
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.constants import (
    CLEANUP_YAML,
    TEMPLATE_CLEANUP_DIR,
    AWS_CLOUDFORMATION_TAG,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import get_openshift_installer, destroy_cluster
from ocs_ci.utility import templating
from ocs_ci.utility.aws import (
    AWS,
    delete_cluster_buckets,
    destroy_volumes,
    get_rhel_worker_instances,
    StackStatusError,
    terminate_rhel_workers,
)

from ocs_ci.cleanup.aws import defaults


FORMAT = "%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def cleanup(cluster_name, cluster_id, upi=False, failed_deletions=None):
    """
    Cleanup existing cluster in AWS

    Args:
        cluster_name (str): Name of the cluster
        cluster_id (str): Cluster id to cleanup
        upi (bool): True for UPI cluster, False otherwise
        failed_deletions (list): list of clusters we failed to delete, used
            for reporting purposes

    """
    data = {"cluster_name": cluster_name, "cluster_id": cluster_id}
    template = templating.Templating(base_path=TEMPLATE_CLEANUP_DIR)
    cleanup_template = template.render_template(CLEANUP_YAML, data)
    cleanup_path = tempfile.mkdtemp(prefix="cleanup_")
    cleanup_file = os.path.join(cleanup_path, "metadata.json")
    with open(cleanup_file, "w") as temp:
        temp.write(cleanup_template)
    bin_dir = os.path.expanduser(config.RUN["bin_dir"])
    oc_bin = os.path.join(bin_dir, "openshift-install")

    if upi:
        aws = AWS()
        rhel_workers = get_rhel_worker_instances(cleanup_path)
        logger.info(f"{cluster_name}'s RHEL workers: {rhel_workers}")
        if rhel_workers:
            terminate_rhel_workers(rhel_workers)
        # Destroy extra volumes
        destroy_volumes(cluster_name)
        aws.delete_apps_record_set(cluster_name)

        stack_names = list()
        # Get master, bootstrap and security group stacks
        for stack_type in ["ma", "bs", "sg"]:
            try:
                stack_names.append(
                    aws.get_cloudformation_stacks(
                        pattern=f"{cluster_name}-{stack_type}"
                    )[0]["StackName"]
                )
            except ClientError:
                continue

        # Get the worker stacks
        worker_index = 0
        worker_stack_exists = True
        while worker_stack_exists:
            try:
                stack_names.append(
                    aws.get_cloudformation_stacks(
                        pattern=f"{cluster_name}-no{worker_index}"
                    )[0]["StackName"]
                )
                worker_index += 1
            except ClientError:
                worker_stack_exists = False

        logger.info(f"Deleting stacks: {stack_names}")
        aws.delete_cloudformation_stacks(stack_names)

        # Destroy the cluster
        logger.info(f"cleaning up {cluster_id}")
        destroy_cluster(installer=oc_bin, cluster_path=cleanup_path)

        for stack_type in ["inf", "vpc"]:
            try:
                stack_names.append(
                    aws.get_cloudformation_stacks(
                        pattern=f"{cluster_name}-{stack_type}"
                    )[0]["StackName"]
                )
            except ClientError:
                continue
        try:
            aws.delete_cloudformation_stacks(stack_names)
        except StackStatusError:
            logger.error("Failed to fully destroy cluster %s", cluster_name)
            if failed_deletions:
                failed_deletions.append(cluster_name)
            raise
    else:
        logger.info(f"cleaning up {cluster_id}")
        try:
            destroy_cluster(installer=oc_bin, cluster_path=cleanup_path)
        except CommandFailed:
            logger.error("Failed to fully destroy cluster %s", cluster_name)
            if failed_deletions:
                failed_deletions.append(cluster_name)
            raise

    delete_cluster_buckets(cluster_name)


def get_clusters(time_to_delete, region_name, prefixes_hours_to_spare):
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
            ci-cleanup script, a list of VPCs that are part of cloudformation,
            and a list of remaining clusters

    """

    def determine_cluster_deletion(ec2_instances, cluster_name):
        for instance in ec2_instances:
            allowed_running_time = time_to_delete
            do_not_delete = False
            if instance.state["Name"] == "running":
                for prefix, hours in prefixes_hours_to_spare.items():
                    # case insensitive 'startswith'
                    if bool(re.match(prefix, cluster_name, re.I)):
                        if hours == "never":
                            do_not_delete = True
                        else:
                            allowed_running_time = int(hours) * 60 * 60
                        break
                if do_not_delete:
                    logger.info(
                        "%s marked as 'do not delete' and will not be " "destroyed",
                        cluster_name,
                    )
                    return False
                else:
                    launch_time = instance.launch_time
                    current_time = datetime.datetime.now(launch_time.tzinfo)
                    running_time = current_time - launch_time
                    logger.info(
                        f"Instance {[tag['Value'] for tag in instance.tags if tag['Key'] == 'Name'][0]} "
                        f"(id: {instance.id}) running time is {running_time} hours while the allowed"
                        f" running time for it is {allowed_running_time/3600} hours"
                    )
                    if running_time.total_seconds() > allowed_running_time:
                        return True
        return False

    aws = AWS(region_name=region_name)
    clusters_to_delete = list()
    remaining_clusters = list()
    cloudformation_vpc_names = list()
    vpcs = aws.ec2_client.describe_vpcs()["Vpcs"]
    vpc_ids = [vpc["VpcId"] for vpc in vpcs]
    vpc_objs = [aws.ec2_resource.Vpc(vpc_id) for vpc_id in vpc_ids]
    for vpc_obj in vpc_objs:
        vpc_tags = vpc_obj.tags
        if vpc_tags:
            cloudformation_vpc_name = [
                tag["Value"] for tag in vpc_tags if tag["Key"] == AWS_CLOUDFORMATION_TAG
            ]
            if cloudformation_vpc_name:
                cloudformation_vpc_names.append(cloudformation_vpc_name[0])
                continue
            vpc_name = [tag["Value"] for tag in vpc_tags if tag["Key"] == "Name"][0]
            cluster_name = vpc_name.replace("-vpc", "")
            vpc_instances = vpc_obj.instances.all()
            if not vpc_instances:
                clusters_to_delete.append(cluster_name)
                continue

            # Append to clusters_to_delete if cluster should be deleted
            if determine_cluster_deletion(vpc_instances, cluster_name):
                clusters_to_delete.append(cluster_name)
            else:
                remaining_clusters.append(cluster_name)
        else:
            logger.info("No tags found for VPC")

    # Get all cloudformation based clusters to delete
    cf_clusters_to_delete = list()
    for vpc_name in cloudformation_vpc_names:
        instance_dicts = aws.get_instances_by_name_pattern(
            f"{vpc_name.replace('-vpc', '')}*"
        )
        ec2_instances = [
            aws.get_ec2_instance(instance_dict["id"])
            for instance_dict in instance_dicts
        ]
        if not ec2_instances:
            continue
        cluster_io_tag = None
        for instance in ec2_instances:
            cluster_io_tag = [
                tag["Key"]
                for tag in instance.tags
                if "kubernetes.io/cluster" in tag["Key"]
            ]
            if cluster_io_tag:
                break
        if not cluster_io_tag:
            logger.warning(
                "Unable to find valid cluster IO tag from ec2 instance tags "
                "for VPC %s. This is probably not an OCS cluster VPC!",
                vpc_name,
            )
            continue
        cluster_name = cluster_io_tag[0].replace("kubernetes.io/cluster/", "")
        if determine_cluster_deletion(ec2_instances, cluster_name):
            cf_clusters_to_delete.append(cluster_name)
        else:
            remaining_clusters.append(cluster_name)

    return clusters_to_delete, cf_clusters_to_delete, remaining_clusters


def cluster_cleanup():
    parser = argparse.ArgumentParser(description="Cleanup AWS Resource")
    parser.add_argument(
        "--cluster", nargs=1, action="append", required=True, help="Cluster name tag"
    )
    parser.add_argument(
        "--upi", action="store_true", required=False, help="For UPI cluster deletion"
    )
    logging.basicConfig(level=logging.DEBUG)
    args = parser.parse_args()
    procs = []
    for id in args.cluster:
        cluster_name = id[0].rsplit("-", 1)[0]
        logger.info(f"cleaning up {id[0]}")
        proc = threading.Thread(target=cleanup, args=(cluster_name, id[0], args.upi))
        proc.start()
        procs.append(proc)
    for p in procs:
        p.join()


def aws_cleanup():
    parser = argparse.ArgumentParser(
        description="AWS overall resources cleanup according to running time"
    )
    parser.add_argument(
        "--hours",
        type=hour_valid,
        action="store",
        required=True,
        help="""
            Maximum running time of the cluster (in hours).
            Clusters older than this will be deleted.
            The minimum is 10 hours
            """,
    )
    parser.add_argument(
        "--region",
        action="store",
        required=False,
        help="The name of the AWS region to delete the resources from",
    )
    parser.add_argument(
        "--prefix",
        action="append",
        required=False,
        type=prefix_hour_mapping,
        help="""
            Additional prefix:hour combo to treat as a special rule.
            Clusters starting with this prefix will only be cleaned up if
            their runtime exceeds the provided hour(this takes precedence
            over the value provided to --hours). Note: if you want to skip
            cleanup of a cluster entirely you can use 'never' for the hour.
            Example: --prefix foo:24 --prefix bar:48 --prefix foobar:never
            """,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        required=False,
        help="""
            Force cluster cleanup.
            User will not be prompted for confirmation.
            WARNING: this utility is destructive, only use this option if
            you know what you are doing.
            """,
    )
    args = parser.parse_args()

    if not args.force:
        confirmation = input(
            "Careful! This action could be highly destructive. "
            "Are you sure you want to proceed? "
        )
        assert (
            confirmation == defaults.CONFIRMATION_ANSWER
        ), "Wrong confirmation answer. Exiting"

    prefixes_hours_to_spare = defaults.CLUSTER_PREFIXES_SPECIAL_RULES

    if args.prefix:
        for prefix, hours in args.prefix:
            logger.info(
                "Adding special rule for prefix '%s' with hours %s", prefix, hours
            )
            prefixes_hours_to_spare.update({prefix: hours})

    time_to_delete = args.hours * 60 * 60
    region = defaults.AWS_REGION if not args.region else args.region
    clusters_to_delete, cf_clusters_to_delete, remaining_clusters = get_clusters(
        time_to_delete=time_to_delete,
        region_name=region,
        prefixes_hours_to_spare=prefixes_hours_to_spare,
    )

    if not clusters_to_delete:
        logger.info("No clusters to delete")
    else:
        logger.info("Deleting clusters: %s", clusters_to_delete)
        get_openshift_installer()
    procs = []
    failed_deletions = []
    for cluster in clusters_to_delete:
        cluster_name = cluster.rsplit("-", 1)[0]
        logger.info(f"Deleting cluster {cluster_name}")
        proc = threading.Thread(
            target=cleanup, args=(cluster_name, cluster, False, failed_deletions)
        )
        proc.start()
        procs.append(proc)
    for p in procs:
        p.join()
    for cluster in cf_clusters_to_delete:
        cluster_name = cluster.rsplit("-", 1)[0]
        logger.info(f"Deleting UPI cluster {cluster_name}")
        proc = threading.Thread(
            target=cleanup, args=(cluster_name, cluster, True, failed_deletions)
        )
        proc.start()
        procs.append(proc)
    for p in procs:
        p.join()
    logger.info("Remaining clusters: %s", remaining_clusters)
    filename = "failed_cluster_deletions.txt"
    content = "None\n"
    if failed_deletions:
        logger.error("Failed cluster deletions: %s", failed_deletions)
        content = ""
        for cluster in failed_deletions:
            content += f"{cluster}\n"
    with open(filename, "w") as f:
        f.write(content)


def prefix_hour_mapping(string):
    """
    Validate that the string provided to --prefix is properly formatted

    Args:
        string (str): input provided to --prefix

    Raises:
        argparse.ArgumentTypeError: if the provided string is not
            correctly formatted

    Returns:
        str, str: prefix, hours
    """
    msg = (
        f"{string} is not a properly formatted prefix:hour combination. "
        f"See the --help for more information."
    )
    try:
        prefix, hours = string.split(":")
        if not prefix or not hours:
            raise argparse.ArgumentTypeError(msg)
        # 'never' should be the only non-int value for hours
        if hours != "never":
            int(hours)
    except ValueError:
        raise argparse.ArgumentTypeError(msg)
    return prefix, hours


def hour_valid(string):
    """
    Validate that the hour value provided is an int and not lower than the
        minimum allowed running time

    Args:
        string: input provided to --hours

    Raises:
        argparse.ArgumentTypeError: if the provided hours value is not an int
            or lower than the minimum allowed running time

    Returns:
        int: valid hour value

    """
    try:
        hours = int(string)
        assert hours >= defaults.MINIMUM_CLUSTER_RUNNING_TIME
    except ValueError:
        msg = f"{string} is not an int, please provide an int value"
        raise argparse.ArgumentTypeError(msg)
    except AssertionError:
        msg = (
            f"Number of hours ({hours}) is lower than the required minimum "
            f"({defaults.MINIMUM_CLUSTER_RUNNING_TIME})."
        )
        raise argparse.ArgumentTypeError(msg)

    return hours


class ManagedServiceCleanup(object):
    def __init__(self):
        self.cluster_names = list()
        self.clusters_details = list()
        self.delete_clusters_list = list()

    def get_rosa_clusters(self):
        """
        Get Rosa Clusters

        """
        cmd = "rosa list clusters"
        cmd_output = utils.run_cmd(cmd)
        lines = cmd_output.splitlines()
        if len(lines) >= 2:
            for line in lines[1::]:
                self.cluster_names.append(line.split()[1])

    def get_clusters_details(self):
        """
        Get Clusters Details

        """
        for cluster_name in self.cluster_names:
            MANAGED_SERVICE_STRUCTURE["cluster_name"] = cluster_name
            cmd = f"rosa describe cluster -c {cluster_name}"
            cmd_output = utils.run_cmd(cmd)
            lines = cmd_output.splitlines()
            for line in lines:
                if "ID:" in line and "External ID:" not in line:
                    MANAGED_SERVICE_STRUCTURE["cluster_id"] = line.split()[1]
                if "Region:" in line:
                    MANAGED_SERVICE_STRUCTURE["region"] = line.split()[1]
                if "Created:" in line:
                    MANAGED_SERVICE_STRUCTURE["creation_time"] = parse(
                        line, fuzzy_with_tokens=True
                    )[0]
                if "State:" in line:
                    MANAGED_SERVICE_STRUCTURE["state"] = line.split()[1]
            try:
                cmd = f"rosa list addon -c {cluster_name}"
                cmd_output = utils.run_cmd(cmd)
                lines = cmd_output.splitlines()
            except Exception as e:
                logger.error(e)
            is_provider = False
            for line in lines:
                if "ocs-provider" in line and "not installed" not in line:
                    is_provider = True
            MANAGED_SERVICE_STRUCTURE["type"] = (
                "provider" if is_provider else "consumer"
            )
            self.clusters_details.append(MANAGED_SERVICE_STRUCTURE)

    def check_cluster_rules(self):
        """
        Check
        """
        dt = datetime.datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        for cluster_detail in self.clusters_details:
            delta = utc_time - cluster_detail["creation_time"]
            delta_hours = delta.seconds / 3600
            for prefix, hours in defaults.CLUSTER_PREFIXES_SPECIAL_RULES.items():
                expected_hours = (
                    hours if prefix in cluster_detail["cluster_name"] else 12
                )
                if delta_hours > expected_hours:
                    self.delete_clusters_list.append(cluster_detail)
        self.delete_expired_clusters()

    def verify_clusters_deleted(self, deleted_clusters):
        """
        Verify the relevant clusters

        Args:
            deleted_clusters (list): List of clusters that need to be deleted

        Returns:
            bool: True if all clusters deleted otherwise False

        """
        cmd = "rosa list clusters"
        cmd_output = utils.run_cmd(cmd)
        not_deleted_cluster = []
        for deleted_cluster in deleted_clusters:
            if deleted_cluster in cmd_output:
                not_deleted_cluster.append(deleted_cluster)
        logger.error(f"{not_deleted_cluster}")
        return len(not_deleted_cluster) == 0

    def delete_expired_clusters(self):
        """
        Delete the clusters that have passed the desired time

        """
        deleted_clusters = list()
        for cluster_detail in self.delete_clusters_list:
            if cluster_detail["type"] == "consumer":
                cmd = f"rosa delete cluster -c {cluster_detail['cluster_name']}"
            else:
                cmd = f"rosa delete service --id {cluster_detail['cluster_id']}"
            utils.run_cmd(cmd)
            deleted_clusters.append(cluster_detail["cluster_name"])

        sample = TimeoutSampler(
            timeout=1500,
            sleep=60,
            func=self.verify_clusters_deleted,
            deleted_clusters=deleted_clusters,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error("Some clusters are not deleted")
        else:
            self.delete_expired_clusters = list()
