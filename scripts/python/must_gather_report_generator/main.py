#!/usr/bin/env python3
"""
ODF Must-Gather Report Generator - Main Entry Point
Run as: python main.py <must-gather-dir> [options]
"""

import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from must_gather_report_generator.utils import Colors, find_must_gather_dir, read_file
from must_gather_report_generator.analyzers import (
    generate_summary,
    analyze_nodes,
    analyze_pods,
    analyze_csv,
    analyze_subscriptions,
    analyze_storagecluster,
    analyze_ceph_status,
    analyze_noobaa,
    analyze_backingstores,
    analyze_ceph_pools,
    analyze_osd_tree,
    analyze_storageclient,
    analyze_pvcs,
    analyze_csi_drivers,
    analyze_events,
)
from must_gather_report_generator.outputs import generate_xml_output


def main():
    """Main function"""
    logging.basicConfig(
        level=logging.WARNING,
        format="[must-gather-report] %(levelname)s: %(message)s",
        stream=sys.stderr,
        force=True,
    )

    parser = argparse.ArgumentParser(
        prog="must_gather_report_generator",
        description="ODF Must-Gather Health Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Console output (default)
  %(prog)s /path/to/must-gather/ocs_must_gather

  # Save text analysis to file
  %(prog)s /path/to/must-gather/ocs_must_gather --output-file analysis.txt

  # Generate XML output
  %(prog)s /path/to/must-gather/ocs_must_gather --xml-output analysis.xml

  # Both text and XML files
  %(prog)s /path/to/must-gather/ocs_must_gather --output-file analysis.txt --xml-output analysis.xml
        """,
    )

    parser.add_argument("must_gather_dir", help="Path to the must-gather directory")
    parser.add_argument(
        "--xml-output", metavar="FILE", help="Generate XML output to specified file"
    )
    parser.add_argument(
        "--output-file",
        metavar="FILE",
        help="Write text analysis to file instead of console",
    )

    args = parser.parse_args()

    mg_base = Path(args.must_gather_dir)

    if not mg_base.exists():
        print(f"{Colors.RED}Error: Directory not found: {mg_base}{Colors.END}")
        return 1

    # Find actual must-gather data directory
    mg_dir = find_must_gather_dir(mg_base)

    print(f"\n{Colors.BOLD}ODF Must-Gather Health Analyzer{Colors.END}")
    print(f"{Colors.CYAN}Base Directory: {mg_base}{Colors.END}")
    print(f"{Colors.CYAN}Data Directory: {mg_dir}{Colors.END}")

    # Verify key paths exist
    print(f"{Colors.CYAN}Checking for key files...{Colors.END}")
    ceph_dir = mg_dir / "ceph"
    ns_dir = mg_dir / "namespaces"
    print(f"  Ceph directory exists: {ceph_dir.exists()}")
    print(f"  Namespaces directory exists: {ns_dir.exists()}")

    # Check timestamp
    timestamp_file = mg_base / "timestamp"
    if timestamp_file.exists():
        timestamp = read_file(timestamp_file)
        if timestamp:
            parts = timestamp.split()
            if len(parts) >= 2:
                print(
                    f"{Colors.CYAN}Collection Time: {parts[0]} {parts[1]}{Colors.END}"
                )
            elif len(parts) == 1:
                print(f"{Colors.CYAN}Collection Time: {parts[0]}{Colors.END}")

    # Redirect output to file if requested
    original_stdout = None
    out_file_handle = None
    if args.output_file:
        try:
            out_file_handle = open(args.output_file, "w", encoding="utf-8")
        except OSError as exc:
            print(
                f"{Colors.RED}Error: cannot open output file "
                f"{args.output_file}: {exc}{Colors.END}",
                file=sys.stderr,
            )
            return 1
        original_stdout = sys.stdout
        sys.stdout = out_file_handle
        # Print info to stderr so user sees it
        print(
            f"\n{Colors.CYAN}Writing text analysis to: {args.output_file}{Colors.END}",
            file=sys.stderr,
        )

    try:
        # Run all analyses (console or file output)
        generate_summary(mg_dir)
        analyze_nodes(mg_dir)  # Node health & capacity
        analyze_pods(mg_dir)  # Moved up per user request
        analyze_csv(mg_dir)  # CSV analysis
        analyze_subscriptions(mg_dir)  # Operator subscriptions
        analyze_storagecluster(mg_dir)  # Will show rook-ceph-operator logs if not ready
        analyze_ceph_status(mg_dir)
        analyze_noobaa(mg_dir)  # Will show noobaa-core logs if unhealthy - MOVED UP
        analyze_backingstores(mg_dir)  # NooBaa BackingStores - MOVED UP
        analyze_ceph_pools(mg_dir)  # Ceph pool details
        analyze_osd_tree(mg_dir)
        analyze_storageclient(mg_dir)
        analyze_pvcs(mg_dir)  # PVC analysis
        analyze_csi_drivers(mg_dir)
        analyze_events(mg_dir)

        print(f"\n{Colors.BOLD}{Colors.GREEN}Analysis Complete!{Colors.END}\n")

    finally:
        # Restore stdout if it was redirected
        if original_stdout is not None and out_file_handle is not None:
            out_file_handle.close()
            sys.stdout = original_stdout
            print(
                f"{Colors.GREEN}✓ Text analysis written to: {args.output_file}{Colors.END}"
            )

    # Generate XML output if requested (after text output)
    if args.xml_output:
        generate_xml_output(mg_dir, mg_base, args.xml_output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
