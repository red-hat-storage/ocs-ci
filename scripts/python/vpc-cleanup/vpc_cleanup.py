import argparse
import boto3
import time
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


def safe_print(msg, lock=None):
    if lock:
        with lock:
            print(msg)
    else:
        print(msg)


def wait_for_nat_gateways_deleted(
    ec2, nat_gateway_ids, lock=None, max_wait=300, poll_interval=5
):
    """Poll until all NAT gateways are deleted."""
    if not nat_gateway_ids:
        return

    safe_print(f"    Polling for NAT gateways to be deleted (max {max_wait}s)...", lock)
    start_time = time.time()

    while time.time() - start_time < max_wait:
        response = ec2.describe_nat_gateways(NatGatewayIds=nat_gateway_ids)
        remaining = [
            ng["NatGatewayId"]
            for ng in response.get("NatGateways", [])
            if ng["State"] not in ["deleted", "deleting"]
        ]

        if not remaining:
            elapsed = int(time.time() - start_time)
            safe_print(f"    All NAT gateways deleted (took {elapsed}s)", lock)
            return

        time.sleep(poll_interval)

    safe_print(
        f"    Warning: Some NAT gateways still not deleted after {max_wait}s", lock
    )


def wait_for_vpc_endpoints_deleted(
    ec2, vpc_id, vpc_endpoint_ids, lock=None, max_wait=180, poll_interval=5
):
    """Poll until VPC endpoint ENIs are fully cleaned up from subnets."""
    if not vpc_endpoint_ids:
        return

    safe_print(
        f"    Polling for VPC endpoint ENIs to be cleaned up (max {max_wait}s)...", lock
    )
    start_time = time.time()

    while time.time() - start_time < max_wait:
        response = ec2.describe_network_interfaces(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "interface-type", "Values": ["vpc_endpoint"]},
            ]
        )
        vpc_endpoint_enis = response.get("NetworkInterfaces", [])

        if not vpc_endpoint_enis:
            elapsed = int(time.time() - start_time)
            safe_print(f"    VPC endpoint ENIs cleaned up (took {elapsed}s)", lock)
            return

        if time.time() - start_time > 20:
            safe_print(
                f"    Still waiting for {len(vpc_endpoint_enis)} VPC endpoint ENIs...",
                lock,
            )

        time.sleep(poll_interval)

    safe_print(
        f"    Warning: {len(vpc_endpoint_enis)} VPC endpoint ENIs still present "
        f"after {max_wait}s",
        lock,
    )


def wait_for_enis_deleted(ec2, vpc_id, lock=None, max_wait=60, poll_interval=5):
    """Poll until user-created ENIs are deleted (ignores AWS-managed ENIs)."""
    managed_types = {
        "vpc_endpoint",
        "nat_gateway",
        "network_load_balancer",
        "gateway_load_balancer",
        "interface",
        "lambda",
        "efa",
    }

    safe_print(f"    Checking for user-created ENIs (max {max_wait}s)...", lock)
    start_time = time.time()

    while time.time() - start_time < max_wait:
        response = ec2.describe_network_interfaces(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        enis = response.get("NetworkInterfaces", [])
        user_enis = [
            eni
            for eni in enis
            if eni.get("InterfaceType", "standard") not in managed_types
        ]

        if not user_enis:
            elapsed = int(time.time() - start_time)
            managed_count = len(enis)
            if managed_count > 0:
                safe_print(
                    f"    User ENIs deleted (took {elapsed}s). "
                    f"{managed_count} AWS-managed ENIs will auto-cleanup.",
                    lock,
                )
            else:
                safe_print(f"    All ENIs deleted (took {elapsed}s)", lock)
            return

        if time.time() - start_time > 20:
            safe_print(
                f"    Still waiting for {len(user_enis)} user-created ENIs...", lock
            )

        time.sleep(poll_interval)

    safe_print(
        f"    Warning: {len(user_enis)} user ENIs still present after {max_wait}s", lock
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean up empty VPCs and their associated resources."
    )
    parser.add_argument(
        "--region", type=str, default="us-east-1", help="AWS Region (e.g., us-east-1)"
    )
    parser.add_argument("--vpc-id", type=str, help="Target specific VPC ID (optional)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resources that would be deleted without taking action.",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of VPCs to process in parallel (default: 1, max: 10)",
    )
    return parser.parse_args()


def get_vpcs(ec2_client, target_vpc_id=None):
    if target_vpc_id:
        return [target_vpc_id]
    vpcs = ec2_client.describe_vpcs()
    return [v["VpcId"] for v in vpcs.get("Vpcs", [])]


def has_active_instances(ec2_client, vpc_id):
    """Returns True if the VPC contains running/stopped EC2 instances."""
    res = ec2_client.describe_instances(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {
                "Name": "instance-state-name",
                "Values": [
                    "pending",
                    "running",
                    "shutting-down",
                    "stopping",
                    "stopped",
                ],
            },
        ]
    )
    instances = [
        i["InstanceId"]
        for r in res.get("Reservations", [])
        for i in r.get("Instances", [])
    ]
    return len(instances) > 0, instances


def find_vpc_resources(session, region, vpc_id):
    """Scans for all resources blocking VPC deletion."""
    ec2 = session.client("ec2", region_name=region)
    elbv2 = session.client("elbv2", region_name=region)

    resources = {
        "nat_gateways": [],
        "vpc_endpoints": [],
        "load_balancers": [],
        "enis": [],
        "internet_gateways": [],
        "subnets": [],
        "route_tables": [],
        "security_groups": [],
    }

    nats = ec2.describe_nat_gateways(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    resources["nat_gateways"] = [
        n["NatGatewayId"]
        for n in nats.get("NatGateways", [])
        if n["State"] not in ["deleted", "deleting"]
    ]

    vpces = ec2.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    resources["vpc_endpoints"] = [
        e["VpcEndpointId"]
        for e in vpces.get("VpcEndpoints", [])
        if e["State"] not in ["deleting", "deleted"]
    ]

    lbs = elbv2.describe_load_balancers()
    resources["load_balancers"] = [
        lb["LoadBalancerArn"]
        for lb in lbs.get("LoadBalancers", [])
        if lb.get("VpcId") == vpc_id
    ]

    enis = ec2.describe_network_interfaces(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    for eni in enis.get("NetworkInterfaces", []):
        resources["enis"].append(
            {
                "id": eni["NetworkInterfaceId"],
                "description": eni.get("Description", "No description"),
                "type": eni.get("InterfaceType", "standard"),
                "attachment": eni.get("Attachment", {}),
            }
        )

    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    resources["subnets"] = [s["SubnetId"] for s in subnets.get("Subnets", [])]

    igws = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )
    resources["internet_gateways"] = [
        igw["InternetGatewayId"] for igw in igws.get("InternetGateways", [])
    ]

    route_tables = ec2.describe_route_tables(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    for rt in route_tables.get("RouteTables", []):
        is_main = any(assoc.get("Main", False) for assoc in rt.get("Associations", []))
        if not is_main:
            resources["route_tables"].append(
                {
                    "id": rt["RouteTableId"],
                    "associations": [
                        a["RouteTableAssociationId"]
                        for a in rt.get("Associations", [])
                        if not a.get("Main", False)
                    ],
                }
            )

    sgs = ec2.describe_security_groups(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    resources["security_groups"] = [
        sg["GroupId"]
        for sg in sgs.get("SecurityGroups", [])
        if sg["GroupName"] != "default"
    ]

    return resources


def clean_vpc(session, region, vpc_id, dry_run=True, lock=None):
    """
    Clean a single VPC and all its resources.
    Returns: ('success'|'failure'|'skipped', vpc_id, message)
    """
    p = lambda msg: safe_print(msg, lock)  # noqa: E731

    ec2 = session.client("ec2", region_name=region)
    elbv2 = session.client("elbv2", region_name=region)

    has_instances, instances = has_active_instances(ec2, vpc_id)
    if has_instances:
        msg = f"Contains active EC2 instances ({', '.join(instances)})"
        p(f"[-] SKIPPING VPC {vpc_id}: {msg}")
        return ("skipped", vpc_id, msg)

    p(f"\n[+] Analyzing VPC: {vpc_id} (No EC2 instances found)")
    res = find_vpc_resources(session, region, vpc_id)

    if dry_run:
        p("    --- DRY RUN MODE (No changes will be made) ---")
        p(f"    Target Load Balancers: {res['load_balancers']}")
        p(f"    Target NAT Gateways:   {res['nat_gateways']}")
        p(f"    Target VPC Endpoints:  {res['vpc_endpoints']}")
        p(f"    Target ENIs ({len(res['enis'])} total):")
        for eni in res["enis"]:
            p(f"      - {eni['id']} | Type: {eni['type']} | {eni['description']}")
        p(f"    Target Route Tables:   {[rt['id'] for rt in res['route_tables']]}")
        p(f"    Target Subnets:        {res['subnets']}")
        p(f"    Target Security Groups: {res['security_groups']}")
        p(f"    Target IGWs:           {res['internet_gateways']}")
        p("    --------------------------------------------")
        return ("skipped", vpc_id, "Dry run mode")

    p("    [!] STARTING DELETION PROCESS...")

    # 1. Delete Load Balancers
    for lb_arn in res["load_balancers"]:
        p(f"    Deleting Load Balancer: {lb_arn}")
        try:
            elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
        except ClientError as e:
            p(f"    Warning: Could not delete Load Balancer {lb_arn}: {e}")

    # 2. Delete VPC Endpoints and wait for their ENIs to be released
    deleted_vpc_endpoints = []
    if res["vpc_endpoints"]:
        p(f"    Deleting VPC Endpoints: {res['vpc_endpoints']}")
        try:
            ec2.delete_vpc_endpoints(VpcEndpointIds=res["vpc_endpoints"])
            deleted_vpc_endpoints = res["vpc_endpoints"]
        except ClientError as e:
            p(f"    Warning: Could not delete VPC Endpoints: {e}")

    if deleted_vpc_endpoints:
        wait_for_vpc_endpoints_deleted(ec2, vpc_id, deleted_vpc_endpoints, lock)

    # 3. Delete NAT Gateways and wait for them to finish
    deleted_nat_gateways = []
    for nat_id in res["nat_gateways"]:
        p(f"    Deleting NAT Gateway: {nat_id}")
        try:
            ec2.delete_nat_gateway(NatGatewayId=nat_id)
            deleted_nat_gateways.append(nat_id)
        except ClientError as e:
            p(f"    Warning: Could not delete NAT Gateway {nat_id}: {e}")

    if deleted_nat_gateways:
        wait_for_nat_gateways_deleted(ec2, deleted_nat_gateways, lock)

    # 4. Delete user-created ENIs (skip AWS-managed ones)
    managed_eni_types = {
        "vpc_endpoint",
        "nat_gateway",
        "network_load_balancer",
        "gateway_load_balancer",
        "lambda",
        "efa",
    }

    managed_count = sum(1 for eni in res["enis"] if eni["type"] in managed_eni_types)
    if managed_count > 0:
        p(
            f"    Skipping {managed_count} AWS-managed ENIs "
            "(will auto-cleanup after parent resources deleted)"
        )

    deleted_enis = []
    for eni in res["enis"]:
        if eni["type"] in managed_eni_types:
            continue

        attachment_id = eni.get("attachment", {}).get("AttachmentId", "")
        if attachment_id.startswith("ela-attach-"):
            continue

        try:
            if attachment_id:
                p(f"    Detaching ENI: {eni['id']} (attachment: {attachment_id})")
                try:
                    ec2.detach_network_interface(AttachmentId=attachment_id, Force=True)
                    time.sleep(2)
                except ClientError as e:
                    if "OperationNotPermitted" not in str(e):
                        p(f"    Warning: Could not detach ENI {eni['id']}: {e}")

            p(f"    Deleting ENI: {eni['id']} (type: {eni['type']})")
            ec2.delete_network_interface(NetworkInterfaceId=eni["id"])
            deleted_enis.append(eni["id"])
        except ClientError as e:
            if "InvalidNetworkInterfaceID.NotFound" not in str(e):
                p(f"    Warning: Could not delete ENI {eni['id']}: {e}")

    if deleted_enis:
        wait_for_enis_deleted(ec2, vpc_id, lock)

    # 5. Delete custom route tables
    for rt in res["route_tables"]:
        for assoc_id in rt["associations"]:
            p(f"    Disassociating Route Table: {rt['id']} (association: {assoc_id})")
            try:
                ec2.disassociate_route_table(AssociationId=assoc_id)
            except ClientError as e:
                p(f"    Warning: Could not disassociate route table: {e}")

        p(f"    Deleting Route Table: {rt['id']}")
        try:
            ec2.delete_route_table(RouteTableId=rt["id"])
        except ClientError as e:
            p(f"    Warning: Could not delete route table {rt['id']}: {e}")

    # 6. Detach and delete Internet Gateways
    for igw_id in res["internet_gateways"]:
        p(f"    Detaching & Deleting IGW: {igw_id}")
        try:
            ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw_id)
        except ClientError as e:
            p(f"    Warning: Could not delete IGW {igw_id}: {e}")

    # 7. Delete Subnets (with retry)
    for subnet_id in res["subnets"]:
        p(f"    Deleting Subnet: {subnet_id}")
        max_retries = 5
        for attempt in range(max_retries):
            try:
                ec2.delete_subnet(SubnetId=subnet_id)
                break
            except ClientError as e:
                if "DependencyViolation" in str(e) and attempt < max_retries - 1:
                    p(
                        f"    Subnet {subnet_id} has dependencies, retrying in 10s "
                        f"(attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(10)
                else:
                    p(f"    Warning: Could not delete subnet {subnet_id}: {e}")
                    break

    # 8. Revoke all security group rules (breaks circular dependencies)
    for sg_id in res["security_groups"]:
        p(f"    Removing rules from Security Group: {sg_id}")
        try:
            sg_details = ec2.describe_security_groups(GroupIds=[sg_id])
            sg = sg_details["SecurityGroups"][0]
            if sg.get("IpPermissions"):
                ec2.revoke_security_group_ingress(
                    GroupId=sg_id, IpPermissions=sg["IpPermissions"]
                )
            if sg.get("IpPermissionsEgress"):
                ec2.revoke_security_group_egress(
                    GroupId=sg_id, IpPermissions=sg["IpPermissionsEgress"]
                )
        except ClientError as e:
            p(f"    Warning: Could not remove rules from security group {sg_id}: {e}")

    # 9. Delete Security Groups
    for sg_id in res["security_groups"]:
        p(f"    Deleting Security Group: {sg_id}")
        max_retries = 5
        for attempt in range(max_retries):
            try:
                ec2.delete_security_group(GroupId=sg_id)
                break
            except ClientError as e:
                if "DependencyViolation" in str(e) and attempt < max_retries - 1:
                    p(
                        f"    Security Group {sg_id} has dependencies, retrying in 8s "
                        f"(attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(8)
                else:
                    p(f"    Warning: Could not delete security group {sg_id}: {e}")
                    break

    # 10. Delete VPC (with retry)
    max_retries = 8
    for attempt in range(max_retries):
        try:
            p(f"    Deleting VPC: {vpc_id} (attempt {attempt + 1}/{max_retries})")
            ec2.delete_vpc(VpcId=vpc_id)
            p(f"    [SUCCESS] VPC {vpc_id} deleted successfully.")
            return ("success", vpc_id, "Deleted successfully")
        except ClientError as e:
            if "DependencyViolation" in str(e) and attempt < max_retries - 1:
                p("    VPC still has dependencies, retrying in 15s...")
                time.sleep(15)
            else:
                error_msg = str(e)
                p(f"    [ERROR] Could not delete VPC {vpc_id}: {error_msg}")
                p("    You may need to manually check for remaining dependencies.")
                return ("failure", vpc_id, error_msg)

    return ("failure", vpc_id, "Max retries exceeded")


def process_vpcs_parallel(session, region, vpc_list, dry_run, max_workers):
    """Process VPCs in parallel and return results."""
    results = []
    lock = Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_vpc = {
            executor.submit(clean_vpc, session, region, vpc, dry_run, lock): vpc
            for vpc in vpc_list
        }

        for future in as_completed(future_to_vpc):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                vpc = future_to_vpc[future]
                results.append(("failure", vpc, f"Exception: {str(e)}"))

    return results


def print_summary(results, retry_results=None):
    """Print a summary report of VPC cleanup results."""
    print("\n" + "=" * 70)
    print("VPC CLEANUP SUMMARY")
    print("=" * 70)

    all_results = results + (retry_results if retry_results else [])

    success = [r for r in all_results if r[0] == "success"]
    failures = [r for r in all_results if r[0] == "failure"]
    skipped = [r for r in all_results if r[0] == "skipped" and r[2] != "Dry run mode"]

    print(f"\n✓ Successfully deleted: {len(success)} VPC(s)")
    for _, vpc_id, _ in success:
        print(f"  - {vpc_id}")

    if skipped:
        print(f"\n⊘ Skipped (has instances): {len(skipped)} VPC(s)")
        for _, vpc_id, reason in skipped:
            print(f"  - {vpc_id}: {reason}")

    if failures:
        print(f"\n✗ Failed to delete: {len(failures)} VPC(s)")
        for _, vpc_id, reason in failures:
            print(f"  - {vpc_id}")
            print(f"    Reason: {reason[:120]}")

    print("\n" + "=" * 70)
    print(
        f"Total: {len(success)} success, {len(failures)} failed, {len(skipped)} skipped"
    )
    print("=" * 70 + "\n")


if __name__ == "__main__":
    args = parse_args()

    max_workers = min(max(1, args.parallel), 10)
    if args.parallel > 10:
        print(f"Warning: --parallel capped at 10 (you requested {args.parallel})")

    session = boto3.Session()
    vpc_list = get_vpcs(session.client("ec2", region_name=args.region), args.vpc_id)

    print(f"Found {len(vpc_list)} VPC(s) in region {args.region}.")
    if max_workers > 1:
        print(f"Processing {max_workers} VPCs in parallel...\n")

    # First pass
    results = process_vpcs_parallel(
        session, args.region, vpc_list, args.dry_run, max_workers
    )

    # Retry failed VPCs once
    retry_results = []
    if not args.dry_run:
        failed_vpcs = [vpc_id for status, vpc_id, _ in results if status == "failure"]
        if failed_vpcs:
            print(f"\n{'=' * 70}")
            print(f"RETRYING {len(failed_vpcs)} FAILED VPC(s)")
            print(f"{'=' * 70}\n")
            retry_results = process_vpcs_parallel(
                session, args.region, failed_vpcs, args.dry_run, max_workers
            )

    print_summary(results, retry_results)
