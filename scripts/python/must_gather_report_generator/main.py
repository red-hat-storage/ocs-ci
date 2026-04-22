#!/usr/bin/env python3
"""
ODF Must-Gather Report Generator - Main Entry Point

Run as installed console script ``must-gather-report``, or:
``python -m must_gather_report_generator <must-gather-dir> [options]``.
"""

import argparse
import logging
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

from .analyzers import (
    analyze_backingstores,
    analyze_ceph_pools,
    analyze_ceph_status,
    analyze_csv,
    analyze_csi_drivers,
    analyze_events,
    analyze_noobaa,
    analyze_nodes,
    analyze_osd_tree,
    analyze_pods,
    analyze_pvcs,
    analyze_storageclient,
    analyze_storagecluster,
    analyze_subscriptions,
    generate_summary,
)
from .outputs import generate_xml_output
from .utils import Colors, detect_deployment_type, find_must_gather_dir, read_file


def _safe_extract_tar(tar: tarfile.TarFile, dest_dir: Path) -> None:
    """
    Safely extract a tar archive to dest_dir (prevents path traversal).
    """

    def is_within_directory(directory: Path, target: Path) -> bool:
        directory_resolved = directory.resolve()
        target_resolved = target.resolve()
        try:
            target_resolved.relative_to(directory_resolved)
            return True
        except ValueError:
            return False

    for member in tar.getmembers():
        member_path = dest_dir / member.name
        if not is_within_directory(dest_dir, member_path):
            raise ValueError(f"Unsafe path detected in archive member: {member.name!r}")

    tar.extractall(path=dest_dir)


def prepare_must_gather_base(must_gather_path: Path) -> tuple[Path, Path | None]:
    """
    Accept either a directory or a compressed archive path.

    Returns:
      (mg_base_dir, temp_extract_dir)
        - mg_base_dir is the directory to use as "base directory"
        - temp_extract_dir is a temp directory to optionally clean up later
    """
    if must_gather_path.is_dir():
        return must_gather_path, None

    if must_gather_path.is_file() and tarfile.is_tarfile(must_gather_path):
        tmp_dir = Path(tempfile.mkdtemp(prefix="ocs-ci-must-gather-"))
        try:
            with tarfile.open(must_gather_path, mode="r:*") as tar:
                _safe_extract_tar(tar, tmp_dir)

            # If archive has a single top-level directory, use it as base.
            children = [p for p in tmp_dir.iterdir() if p.name not in {".DS_Store"}]
            if len(children) == 1 and children[0].is_dir():
                return children[0], tmp_dir
            return tmp_dir, tmp_dir
        except (tarfile.TarError, OSError, ValueError):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

    return must_gather_path, None


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

    mg_input = Path(args.must_gather_dir)

    if not mg_input.exists():
        print(f"{Colors.RED}Error: Path not found: {mg_input}{Colors.END}")
        return 1

    tmp_extract_dir: Path | None = None
    try:
        mg_base, tmp_extract_dir = prepare_must_gather_base(mg_input)
    except (tarfile.TarError, OSError, ValueError) as exc:
        print(
            f"{Colors.RED}Error: failed to extract archive {mg_input}: {exc}{Colors.END}",
            file=sys.stderr,
        )
        return 1

    try:
        # Find actual must-gather data directory
        mg_dir = find_must_gather_dir(mg_base)

        # Detect deployment type
        deployment_type = detect_deployment_type(mg_dir)

        print(f"\n{Colors.BOLD}ODF Must-Gather Health Analyzer{Colors.END}")
        print(f"{Colors.CYAN}Input Path: {mg_input}{Colors.END}")
        print(f"{Colors.CYAN}Base Directory: {mg_base}{Colors.END}")
        print(f"{Colors.CYAN}Data Directory: {mg_dir}{Colors.END}")
        print(f"{Colors.CYAN}Deployment Type: {deployment_type.upper()}{Colors.END}")

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

        # Redirect output to file if requested; suppress ANSI when not a TTY
        original_stdout = None
        out_file_handle = None
        suppress_colors_restore_for_file = False
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
            Colors.disable()
            suppress_colors_restore_for_file = True
            sys.stdout = out_file_handle
            # Print info to stderr so user sees it
            print(
                f"\n{Colors.CYAN}Writing text analysis to: {args.output_file}{Colors.END}",
                file=sys.stderr,
            )
        elif not sys.stdout.isatty():
            Colors.disable()

        try:
            # Run all analyses (console or file output)
            generate_summary(mg_dir, deployment_type)
            analyze_nodes(mg_dir, deployment_type)  # Node health & capacity
            analyze_pods(mg_dir, deployment_type)  # Moved up per user request
            analyze_csv(mg_dir, deployment_type)  # CSV analysis
            analyze_subscriptions(mg_dir, deployment_type)  # Operator subscriptions
            analyze_storagecluster(
                mg_dir, deployment_type
            )  # Will show rook-ceph-operator logs if not ready
            analyze_ceph_status(mg_dir, deployment_type)
            analyze_noobaa(
                mg_dir, deployment_type
            )  # Will show noobaa-core logs if unhealthy - MOVED UP
            analyze_backingstores(
                mg_dir, deployment_type
            )  # NooBaa BackingStores - MOVED UP
            analyze_ceph_pools(mg_dir, deployment_type)  # Ceph pool details
            analyze_osd_tree(mg_dir, deployment_type)
            analyze_storageclient(mg_dir, deployment_type)
            analyze_pvcs(mg_dir, deployment_type)  # PVC analysis
            analyze_csi_drivers(mg_dir, deployment_type)
            analyze_events(mg_dir, deployment_type)

            print(f"\n{Colors.BOLD}{Colors.GREEN}Analysis Complete!{Colors.END}\n")

        finally:
            # Restore stdout if it was redirected
            if original_stdout is not None and out_file_handle is not None:
                out_file_handle.close()
                sys.stdout = original_stdout
            if suppress_colors_restore_for_file:
                Colors.enable()
            if (
                args.output_file
                and original_stdout is not None
                and out_file_handle is not None
            ):
                print(
                    f"{Colors.GREEN}✓ Text analysis written to: {args.output_file}{Colors.END}"
                )

        # Generate XML output if requested (after text output)
        if args.xml_output:
            generate_xml_output(mg_dir, mg_base, args.xml_output, deployment_type)

        return 0
    finally:
        if tmp_extract_dir is not None:
            shutil.rmtree(tmp_extract_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
