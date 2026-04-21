"""Analysis functions for ceph"""

from ..utils import (
    Colors,
    HEALTH_STATUS_UNKNOWN,
    NOT_AVAILABLE,
    UNKNOWN,
    ZERO,
    list_from,
    print_header,
    print_status,
)
from ..utils import read_json_file, read_file


def analyze_ceph_status(mg_dir):
    """Analyze Ceph cluster status"""
    print_header("CEPH CLUSTER STATUS")

    # Check ceph health
    health_file = mg_dir / "ceph/must_gather_commands/ceph_health_detail"
    if health_file.exists():
        raw = read_file(health_file)
        if raw is not None:
            print_status("Ceph Health", raw.strip())
        else:
            print(
                f"{Colors.YELLOW}Could not read Ceph health file "
                f"({health_file}){Colors.END}"
            )

    # Check ceph status JSON
    status_file = (
        mg_dir
        / "ceph/must_gather_commands_json_output/ceph_status_--format_json-pretty"
    )
    if status_file.exists():
        status = read_json_file(status_file)
        if status:
            health_status = status.get("health", {}).get(
                "status", HEALTH_STATUS_UNKNOWN
            )
            print_status("Overall Status", health_status)

            # Print health checks/warnings
            checks = status.get("health", {}).get("details", {})
            if checks:
                print(f"\n{Colors.YELLOW}Health Checks:{Colors.END}")
                for check_name, check_data in checks.items():
                    severity = check_data.get("severity", "INFO")
                    message = check_data.get("message", "No message")
                    print(
                        f"  {Colors.YELLOW}[{severity}] {check_name}:{Colors.END} {message}"
                    )

            # Monitor status
            mon_map = status.get("monmap", {})
            print(f"\n{Colors.CYAN}Monitors:{Colors.END}")
            print(f"  Total: {mon_map.get('num_mons', ZERO)}")
            print(f"  Quorum: {len(status.get('quorum', []))}")
            print(f"  Quorum names: {', '.join(status.get('quorum_names', []))}")

            # OSD status
            osd_map = status.get("osdmap", {})
            print(f"\n{Colors.CYAN}OSDs:{Colors.END}")
            print(f"  Total: {osd_map.get('num_osds', ZERO)}")
            print(f"  Up: {osd_map.get('num_up_osds', ZERO)}")
            print(f"  In: {osd_map.get('num_in_osds', ZERO)}")

            num_osds = osd_map.get("num_osds", ZERO)
            num_up = osd_map.get("num_up_osds", ZERO)
            if num_up < num_osds:
                print_status(
                    "  OSD Health",
                    f"{num_osds - num_up} OSDs DOWN",
                    "Some OSDs are not running",
                )
            else:
                print(f"  {Colors.GREEN}✓{Colors.END} OSD Health: All OSDs UP")

            # PG status
            pg_map = status.get("pgmap", {})
            print(f"\n{Colors.CYAN}Placement Groups:{Colors.END}")
            print(f"  Total PGs: {pg_map.get('num_pgs', ZERO)}")
            pgs_by_state = pg_map.get("pgs_by_state", [])
            for pg_state in pgs_by_state:
                state_name = pg_state.get("state_name", UNKNOWN)
                count = pg_state.get("count", ZERO)
                if "active+clean" in state_name:
                    print(f"  {Colors.GREEN}✓{Colors.END} {state_name}: {count}")
                elif "peering" in state_name or "inactive" in state_name:
                    print(f"  {Colors.YELLOW}⚠{Colors.END} {state_name}: {count}")
                else:
                    print(f"    {state_name}: {count}")

            # Storage capacity
            ceph_data = status.get("pgmap", {})
            bytes_total = ceph_data.get("bytes_total", ZERO)
            bytes_avail = ceph_data.get("bytes_avail", ZERO)
            bytes_used = ceph_data.get("bytes_used", ZERO)

            if bytes_total > 0:
                total_tb = bytes_total / (1024**4)
                avail_tb = bytes_avail / (1024**4)
                used_tb = bytes_used / (1024**4)
                used_pct = (bytes_used / bytes_total) * 100

                print(f"\n{Colors.CYAN}Storage:{Colors.END}")
                print(f"  Total: {total_tb:.2f} TiB")
                print(f"  Used: {used_tb:.2f} TiB ({used_pct:.1f}%)")
                print(f"  Available: {avail_tb:.2f} TiB")


def analyze_osd_tree(mg_dir):
    """Analyze OSD tree"""
    print_header("OSD TOPOLOGY")

    osd_tree_file = mg_dir / "ceph/must_gather_commands/ceph_osd_tree"
    if osd_tree_file.exists():
        tree = read_file(osd_tree_file)
        if tree:
            print(tree)

            # Count OSDs per host
            hosts = {}
            for line in tree.split("\n"):
                if "host" in line and "baremetal" in line:
                    parts = line.split()
                    for part in parts:
                        if "baremetal" in part:
                            hosts[part] = 0
                elif "osd." in line:
                    # Find which host this OSD belongs to
                    for host in hosts:
                        pass  # Simple display, detailed in the tree above


def analyze_ceph_pools(mg_dir):
    """Analyze Ceph pools"""
    print_header("CEPH POOLS")

    # Check ceph osd dump for pool info
    pool_file = (
        mg_dir
        / "ceph/must_gather_commands_json_output/ceph_osd_dump_--format_json-pretty"
    )
    if pool_file.exists():
        pool_data = read_json_file(pool_file)
        pools = list_from(pool_data, "pools") if pool_data else []
        if pools:
            print(f"{Colors.CYAN}Total Pools:{Colors.END} {len(pools)}\n")

            for pool in pools:
                pool_name = pool.get("pool_name", UNKNOWN)
                pool_id = pool.get("pool", NOT_AVAILABLE)
                size = pool.get("size", ZERO)
                min_size = pool.get("min_size", ZERO)
                pg_num = pool.get("pg_num", ZERO)
                pool_type = "replicated" if pool.get("type", 1) == 1 else "erasure"

                print(f"{Colors.CYAN}Pool: {pool_name}{Colors.END} (ID: {pool_id})")
                print(f"  Type: {pool_type}")
                print(f"  Size: {size}, Min Size: {min_size}")
                print(f"  PG Count: {pg_num}")

                # Check for issues
                if size < 3 and pool_type == "replicated":
                    print(f"  {Colors.YELLOW}⚠ Low replication (size < 3){Colors.END}")
                if min_size < 2 and pool_type == "replicated":
                    print(f"  {Colors.YELLOW}⚠ Low min_size (< 2){Colors.END}")

                print()
    else:
        print(f"{Colors.YELLOW}Ceph OSD dump not found{Colors.END}")
