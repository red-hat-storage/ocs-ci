import tempfile
import argparse
import logging
import datetime
import threading
import os
import re
import boto3
import time
import traceback

from botocore.exceptions import ClientError
from ocs_ci.framework import config


from ocs_ci.ocs.bucket_utils import delete_all_objects_in_batches
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


def is_vpc_protected(vpc_obj):
    """
    Check if a VPC is protected from deletion.

    Args:
        vpc_obj: boto3 VPC resource object

    Returns:
        bool: True if VPC is protected, False otherwise
    """
    if not vpc_obj.tags:
        return False

    # Check for do_not_delete tag
    for tag in vpc_obj.tags:
        if tag["Key"] == defaults.DO_NOT_DELETE_TAG:
            tag_value = tag["Value"].lower()
            if tag_value in ["true", "1", "yes"]:
                logger.info(
                    "VPC %s is protected by %s tag",
                    vpc_obj.id,
                    defaults.DO_NOT_DELETE_TAG,
                )
                return True

    # Check for protected VPC names
    vpc_name = None
    for tag in vpc_obj.tags:
        if tag["Key"] == "Name":
            vpc_name = tag["Value"]
            break

    if vpc_name:
        for protected_name in defaults.PROTECTED_VPC_NAMES:
            if protected_name in vpc_name:
                logger.info(
                    "VPC %s (name: %s) is protected by name pattern: %s",
                    vpc_obj.id,
                    vpc_name,
                    protected_name,
                )
                return True

    return False


def is_hypershift_or_capi_cluster(vpc_obj):
    """
    Detect if a VPC belongs to a HyperShift or CAPI managed cluster.

    Args:
        vpc_obj: boto3 VPC resource object

    Returns:
        bool: True if it's a HyperShift/CAPI cluster, False otherwise
    """
    # Check instances in the VPC for HyperShift/CAPI tags
    for instance in vpc_obj.instances.all():
        if not instance.tags:
            continue

        for tag in instance.tags:
            # Check for HyperShift tags
            if tag["Key"].startswith("hypershift.openshift.io/"):
                logger.info("Detected HyperShift cluster from tag: %s", tag["Key"])
                return True
            # Check for CAPI tags
            if tag["Key"].startswith("sigs.k8s.io/cluster-api-provider-aws/"):
                logger.info("Detected CAPI cluster from tag: %s", tag["Key"])
                return True

    return False


def cleanup_hypershift_vpc(cluster_name, vpc_id, region_name):
    """
    Clean up a HyperShift/CAPI VPC and all its dependencies in the correct order.

    Args:
        cluster_name (str): Name of the cluster
        vpc_id (str): VPC ID to clean up
        region_name (str): AWS region name

    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    aws = AWS(region_name=region_name)
    logger.info("Starting HyperShift/CAPI cleanup for VPC %s", vpc_id)

    try:
        vpc = aws.ec2_resource.Vpc(vpc_id)

        # Step 1: Terminate all EC2 instances
        instances = list(vpc.instances.all())
        if instances:
            logger.info("Terminating %d EC2 instances", len(instances))
            instances_dict = {}
            for instance in instances:
                instance_name = instance.id
                if instance.tags:
                    for tag in instance.tags:
                        if tag["Key"] == "Name":
                            instance_name = tag["Value"]
                            break
                instances_dict[instance.id] = instance_name
            aws.terminate_ec2_instances(instances_dict, wait=True)

        # Step 2: Delete NAT Gateways
        nat_gateways = aws.ec2_client.describe_nat_gateways(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["NatGateways"]
        nat_gateway_ids = []
        for nat_gw in nat_gateways:
            if nat_gw["State"] not in ["deleted", "deleting"]:
                logger.info("Deleting NAT Gateway %s", nat_gw["NatGatewayId"])
                aws.ec2_client.delete_nat_gateway(NatGatewayId=nat_gw["NatGatewayId"])
                nat_gateway_ids.append(nat_gw["NatGatewayId"])

        # Wait for NAT Gateways to be fully deleted
        if nat_gateway_ids:
            logger.info("Waiting for NAT Gateways to be deleted...")
            max_wait = 300  # 5 minutes
            wait_interval = 15
            for _ in range(max_wait // wait_interval):
                time.sleep(wait_interval)
                remaining = aws.ec2_client.describe_nat_gateways(
                    NatGatewayIds=nat_gateway_ids
                )["NatGateways"]
                if all(ngw["State"] == "deleted" for ngw in remaining):
                    logger.info("All NAT Gateways deleted")
                    break
                logger.info(
                    "Still waiting for %d NAT Gateways to delete...",
                    sum(1 for ngw in remaining if ngw["State"] != "deleted"),
                )
            else:
                logger.warning("NAT Gateway deletion timed out, continuing anyway")

        # Step 3: Release Elastic IPs
        # Get all addresses and filter by VPC
        all_addresses = aws.ec2_client.describe_addresses()["Addresses"]
        vpc_addresses = []
        for address in all_addresses:
            # Check if EIP is associated with this VPC
            network_interface_id = address.get("NetworkInterfaceId")
            if network_interface_id:
                try:
                    eni = aws.ec2_client.describe_network_interfaces(
                        NetworkInterfaceIds=[network_interface_id]
                    )["NetworkInterfaces"][0]
                    if eni.get("VpcId") == vpc_id:
                        vpc_addresses.append(address)
                except ClientError:
                    pass
            elif address.get("AssociationId"):
                # EIP has an association but no ENI, might be NAT Gateway
                vpc_addresses.append(address)

        for address in vpc_addresses:
            allocation_id = address.get("AllocationId")
            if not allocation_id:
                continue
            logger.info("Releasing Elastic IP %s", allocation_id)
            try:
                aws.ec2_client.release_address(AllocationId=allocation_id)
            except ClientError as e:
                logger.warning("Failed to release EIP %s: %s", allocation_id, e)

        # Step 4: Delete Load Balancers (ELBv2)
        try:
            elbv2_client = boto3.client("elbv2", region_name=region_name)
            lbs = elbv2_client.describe_load_balancers()["LoadBalancers"]
            for lb in lbs:
                if lb["VpcId"] == vpc_id:
                    logger.info("Deleting Load Balancer %s", lb["LoadBalancerArn"])
                    elbv2_client.delete_load_balancer(
                        LoadBalancerArn=lb["LoadBalancerArn"]
                    )
        except Exception as e:
            logger.warning("Error deleting load balancers: %s", e)

        # Step 5: Detach and delete Internet Gateways
        igws = list(vpc.internet_gateways.all())
        for igw in igws:
            logger.info("Detaching and deleting Internet Gateway %s", igw.id)
            # Retry detachment in case some IPs are still being released
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    igw.detach_from_vpc(VpcId=vpc_id)
                    logger.info("Successfully detached Internet Gateway %s", igw.id)
                    break
                except ClientError as e:
                    if "DependencyViolation" in str(e) and attempt < max_retries - 1:
                        logger.warning(
                            "IGW detach failed (attempt %d/%d), "
                            "waiting for dependencies: %s",
                            attempt + 1,
                            max_retries,
                            e,
                        )
                        time.sleep(30)
                    else:
                        raise
            igw.delete()

        # Step 6: Delete VPC Endpoints
        vpc_endpoints = aws.ec2_client.describe_vpc_endpoints(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["VpcEndpoints"]
        for endpoint in vpc_endpoints:
            logger.info("Deleting VPC Endpoint %s", endpoint["VpcEndpointId"])
            aws.ec2_client.delete_vpc_endpoints(
                VpcEndpointIds=[endpoint["VpcEndpointId"]]
            )

        # Step 7: Delete Network Interfaces
        network_interfaces = list(vpc.network_interfaces.all())
        for ni in network_interfaces:
            if ni.attachment:
                logger.info("Detaching Network Interface %s", ni.id)
                try:
                    ni.detach(Force=True)
                    time.sleep(5)
                except ClientError as e:
                    logger.warning("Failed to detach network interface: %s", e)

            logger.info("Deleting Network Interface %s", ni.id)
            try:
                ni.delete()
            except ClientError as e:
                logger.warning("Failed to delete network interface: %s", e)

        # Step 8: Delete subnets
        subnets = list(vpc.subnets.all())
        for subnet in subnets:
            logger.info("Deleting Subnet %s", subnet.id)
            try:
                subnet.delete()
            except ClientError as e:
                logger.warning("Failed to delete subnet %s: %s", subnet.id, e)

        # Step 9: Delete route tables (except main)
        route_tables = list(vpc.route_tables.all())
        for rt in route_tables:
            # Skip main route table
            is_main = any(assoc.main for assoc in rt.associations_attribute or [])
            if not is_main:
                logger.info("Deleting Route Table %s", rt.id)
                try:
                    rt.delete()
                except ClientError as e:
                    logger.warning("Failed to delete route table: %s", e)

        # Step 10: Delete security groups (except default)
        security_groups = list(vpc.security_groups.all())
        for sg in security_groups:
            if sg.group_name != "default":
                logger.info("Deleting Security Group %s", sg.id)
                try:
                    # Remove all rules first
                    if sg.ip_permissions:
                        sg.revoke_ingress(IpPermissions=sg.ip_permissions)
                    if sg.ip_permissions_egress:
                        sg.revoke_egress(IpPermissions=sg.ip_permissions_egress)
                    sg.delete()
                except ClientError as e:
                    logger.warning("Failed to delete security group: %s", e)

        # Step 11: Delete Network ACLs (except default)
        network_acls = list(vpc.network_acls.all())
        for acl in network_acls:
            if not acl.is_default:
                logger.info("Deleting Network ACL %s", acl.id)
                try:
                    acl.delete()
                except ClientError as e:
                    logger.warning("Failed to delete network ACL: %s", e)

        # Step 12: Delete VPC
        logger.info("Deleting VPC %s", vpc_id)
        vpc.delete()

        logger.info("Successfully cleaned up HyperShift/CAPI VPC %s", vpc_id)
        return True

    except Exception as e:
        logger.error("Failed to clean up HyperShift/CAPI VPC %s: %s", vpc_id, e)
        logger.error(traceback.format_exc())
        return False


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


def get_clusters(
    time_to_delete, region_name, prefixes_hours_to_spare, cluster_pattern=None
):
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
        cluster_pattern (str): The name of the ec2 instances

    Returns:
        tuple: List of the cluster names (e.g ebenahar-cluster-gqtd4) to be provided to the
            ci-cleanup script, a list of VPCs that are part of cloudformation,
            a list of HyperShift/CAPI clusters with VPC IDs,
            and a list of remaining clusters

    """

    def determine_cluster_deletion(ec2_instances, cluster_name):
        for instance in ec2_instances:
            # Skip terminated instances
            if instance.state["Name"] in ["terminated", "terminating"]:
                continue

            # Check if instance has do_not_delete tag
            if instance.tags:
                for tag in instance.tags:
                    if tag["Key"] == defaults.DO_NOT_DELETE_TAG:
                        tag_value = tag["Value"].lower()
                        if tag_value in ["true", "1", "yes"]:
                            logger.info(
                                "Instance %s has %s tag, skipping cluster deletion",
                                instance.id,
                                defaults.DO_NOT_DELETE_TAG,
                            )
                            return False

            allowed_running_time = time_to_delete
            do_not_delete = False

            # Check both running and stopped instances
            if instance.state["Name"] in ["running", "stopped"]:
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
                    instance_name = [
                        tag["Value"] for tag in instance.tags if tag["Key"] == "Name"
                    ][0]
                    logger.info(
                        f"Instance {instance_name} (id: {instance.id}, "
                        f"state: {instance.state['Name']}) running time is "
                        f"{running_time} hours while the allowed running time "
                        f"for it is {allowed_running_time / 3600} hours"
                    )
                    if running_time.total_seconds() > allowed_running_time:
                        return True
        return False

    def determine_cluster_deletion_base_name(ec2_instance_objs, vpc_id):
        """
        Determine cluster deletion base on name

        Args:
            ec2_instance_objs (list): list of ec2 instance obj
            vpc_id (str): vpc id

        Returns:
            bool: True if vpc_id exist and all ec2 instances on same vpc otherwise False

        """
        # Get all instances
        vpc_ids = [
            ec2_instance.get("Instances")[0].get("VpcId")
            for ec2_instance in ec2_instance_objs
        ]
        # Verify vpc_id exist and all ec2 instances on same vpc
        return True if vpc_id in vpc_ids and len(set(vpc_ids)) == 1 else False

    aws = AWS(region_name=region_name)
    clusters_to_delete = list()
    hypershift_clusters_to_delete = list()  # List of (cluster_name, vpc_id) tuples
    remaining_clusters = list()
    cloudformation_vpc_names = list()
    vpcs = aws.ec2_client.describe_vpcs()["Vpcs"]
    vpc_ids = [vpc["VpcId"] for vpc in vpcs]
    vpc_objs = [aws.ec2_resource.Vpc(vpc_id) for vpc_id in vpc_ids]
    ec2_instance_objs = None
    if cluster_pattern:
        worker_filter = [{"Name": "tag:Name", "Values": [f"{cluster_pattern}*"]}]
        ec2_instance_objs = aws.ec2_client.describe_instances(
            Filters=worker_filter
        ).get("Reservations")

    for vpc_obj in vpc_objs:
        # Check if VPC is protected
        if is_vpc_protected(vpc_obj):
            logger.info("Skipping protected VPC %s", vpc_obj.id)
            continue

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
            all_vpc_instances = list(vpc_obj.instances.all())
            # Filter out terminated/terminating instances for decision making
            vpc_instances = [
                inst
                for inst in all_vpc_instances
                if inst.state["Name"] not in ["terminated", "terminating"]
            ]

            # Check if this is a HyperShift/CAPI cluster
            is_hypershift = is_hypershift_or_capi_cluster(vpc_obj)

            if not vpc_instances:
                logger.info(
                    "VPC %s (%s) has no active instances, marking for deletion",
                    vpc_obj.id,
                    cluster_name,
                )
                if is_hypershift:
                    hypershift_clusters_to_delete.append((cluster_name, vpc_obj.id))
                else:
                    clusters_to_delete.append(cluster_name)
                continue

            # Determine if cluster should be deleted
            should_delete = False
            if cluster_pattern is not None:
                should_delete = determine_cluster_deletion_base_name(
                    ec2_instance_objs, vpc_obj.id
                )
            else:
                should_delete = determine_cluster_deletion(vpc_instances, cluster_name)

            # Append to appropriate list based on cluster type
            if should_delete:
                if is_hypershift:
                    hypershift_clusters_to_delete.append((cluster_name, vpc_obj.id))
                else:
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
        logger.info(f"cluster_name={cluster_name}")
        if cluster_pattern is not None:
            if cluster_pattern in cluster_name:
                cf_clusters_to_delete.append(cluster_name)
            else:
                remaining_clusters.append(cluster_name)
        else:
            if determine_cluster_deletion(ec2_instances, cluster_name):
                cf_clusters_to_delete.append(cluster_name)
            else:
                remaining_clusters.append(cluster_name)
    return (
        clusters_to_delete,
        cf_clusters_to_delete,
        hypershift_clusters_to_delete,
        remaining_clusters,
    )


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


def delete_buckets(bucket_prefix, hours):
    """
    Delete the S3 buckets with given prefix

    Args:
        bucket_prefix (dict): Bucket prefix as key and maximum hours to run/exist as value
        hours (int): hours older than this will be considered to delete

    """
    aws = AWS()
    buckets_to_delete = aws.get_buckets_to_delete(bucket_prefix, hours)
    logger.info(f"buckets to delete: {buckets_to_delete}")
    buckets_deletion_failed = []
    for bucket_name in buckets_to_delete:
        try:
            bucket = boto3.resource("s3").Bucket(bucket_name)
            try:
                delete_all_objects_in_batches(
                    s3_resource=boto3.resource("s3"), bucket_name=bucket_name
                )
                bucket.object_versions.all().delete()
            except Exception as e:
                logger.error(f"failed to list object in bucket {bucket_name} err:{e}")
            bucket.delete()
        except Exception as e:
            logger.error(e)
            buckets_deletion_failed.append(bucket_name)
    return buckets_deletion_failed


def aws_cleanup():
    parser = argparse.ArgumentParser(
        description="AWS overall resources cleanup according to running time",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--hours",
        type=hour_valid,
        action="store",
        required=False,
        help="""
            Maximum running time of the cluster (in hours).
            Clusters older than this will be deleted.
            The minimum is 10 hours.
            If sweep-buckets flag enabled:
            Running time for the buckets in hours.
            Buckets older than to this will be deleted.
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
    parser.add_argument(
        "--cluster-name",
        action="store",
        required=False,
        help="The name of the cluster to delete from AWS",
    )
    bucket_group = parser.add_argument_group("S3 Bucket Sweeping Options")
    bucket_group.add_argument(
        "--sweep-buckets", action="store_true", help="Deleting S3 buckets."
    )
    args = parser.parse_args()

    if args.sweep_buckets:
        bucket_hours = (
            args.hours
            if args.hours is not None
            else defaults.DEFAULT_BUCKET_RUNNING_TIME
        )
        buckets_deletion_failed = delete_buckets(
            defaults.BUCKET_PREFIXES_SPECIAL_RULES, bucket_hours
        )
        assert (
            len(buckets_deletion_failed) == 0
        ), f"No all buckets deleted\n buckets_deletion_failed={buckets_deletion_failed}"
        return

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
            prefixes_hours_to_spare = {**{prefix: hours}, **prefixes_hours_to_spare}

    time_to_delete = args.hours * 60 * 60 if args.hours else None
    region = defaults.AWS_REGION if not args.region else args.region
    (
        clusters_to_delete,
        cf_clusters_to_delete,
        hypershift_clusters_to_delete,
        remaining_clusters,
    ) = get_clusters(
        time_to_delete=time_to_delete,
        region_name=region,
        prefixes_hours_to_spare=prefixes_hours_to_spare,
        cluster_pattern=args.cluster_name,
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

    # Clean up HyperShift/CAPI clusters
    if hypershift_clusters_to_delete:
        logger.info(
            "Deleting HyperShift/CAPI clusters: %s",
            [name for name, _ in hypershift_clusters_to_delete],
        )
        for cluster_name, vpc_id in hypershift_clusters_to_delete:
            logger.info(f"Deleting HyperShift/CAPI cluster {cluster_name}")
            success = cleanup_hypershift_vpc(cluster_name, vpc_id, region)
            if not success:
                failed_deletions.append(cluster_name)
            # Also try to delete associated S3 buckets
            try:
                delete_cluster_buckets(cluster_name)
            except Exception as e:
                logger.error(
                    "Failed to delete buckets for cluster %s: %s", cluster_name, e
                )

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
