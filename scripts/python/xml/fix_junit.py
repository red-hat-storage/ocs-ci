#!/usr/bin/env python3
"""
Module to fix corrupted JUnit XML files caused by SIGTERM interruption.

The corruption happens when SIGTERM converts to SIGINT during log collection,
causing a Python traceback to be written into an unclosed <error> tag.

Can be used as:
1. Imported module: from scripts.python.xml.fix_junit import fix_xml_file
2. Standalone script: python3 scripts/python/xml/fix_junit.py <xml_file>
"""

import os
import sys
from datetime import datetime


def fix_xml_file(xml_file):
    """
    Fix corrupted JUnit XML file in-place.

    Args:
        xml_file: Path to the XML file to fix

    Returns:
        bool: True if successful, False otherwise
    """
    if not xml_file or not os.path.exists(xml_file):
        print(f"XML file not found: {xml_file}")
        return False

    print(f"Fixing corrupted XML file: {xml_file}")

    # Read the entire file
    try:
        with open(xml_file, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        print(f"ERROR: Failed to read file: {e}")
        return False

    # Create timestamped backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{xml_file}.backup_{timestamp}"
    try:
        with open(backup_file, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✓ Backup created: {backup_file}")
    except Exception as e:
        print(f"ERROR: Failed to create backup: {e}")
        return False

    # Find all </testcase> positions
    testcase_ends = []
    pos = 0
    while True:
        pos = content.find("</testcase>", pos)
        if pos == -1:
            break
        testcase_ends.append(pos)
        pos += 1

    if len(testcase_ends) < 2:
        print(f"WARNING: Found only {len(testcase_ends)} </testcase> tag(s)")
        print("File may not be corrupted or corruption is different than expected")
        # Try to validate as-is
        try:
            import xml.etree.ElementTree as ET

            ET.fromstring(content)
            print("✓ XML is already valid, no fix needed")
            return True
        except Exception:
            print("ERROR: XML is invalid but cannot be fixed automatically")
            return False

    print(f"Found {len(testcase_ends)} </testcase> tags")

    # The last </testcase> is part of the corrupted entry (with traceback)
    # We want the second-to-last one, which is the last valid testcase
    last_valid_testcase_end = testcase_ends[-2]

    print("Removing corrupted testcase (last one with SIGTERM traceback)")

    # Cut everything after the last valid </testcase> tag
    fixed_content = content[: last_valid_testcase_end + len("</testcase>")]

    # Add proper closing tags
    fixed_content += "\n</testsuite>\n</testsuites>\n"

    # Validate the fixed XML
    try:
        import xml.etree.ElementTree as ET

        ET.fromstring(fixed_content)
        print("✓ Fixed XML is valid and well-formed")
    except Exception as e:
        print(f"ERROR: Fixed XML is still invalid: {e}")
        return False

    # Write the fixed XML back to the original file
    try:
        with open(xml_file, "w", encoding="utf-8") as f:
            f.write(fixed_content)
        print(f"✓ Original file replaced with fixed version: {xml_file}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to write fixed file: {e}")
        return False


def main():
    """Main function for standalone script execution."""
    if len(sys.argv) != 2:
        print(__doc__)
        print("\nUsage: python3 fix_junit.py <xml_file>")
        print("Example: python3 fix_junit.py /path/to/test_results.xml")
        sys.exit(1)

    xml_file = sys.argv[1]

    try:
        success = fix_xml_file(xml_file)
        if success:
            print("\n" + "=" * 60)
            print("✓ XML file fixed successfully!")
            print("=" * 60)
            print(f"\nFixed file: {xml_file}")
            print(f"Backup: {xml_file}.backup_<timestamp>")
            print("\nNote: The last test case (interrupted by SIGTERM) was removed.")
            sys.exit(0)
        else:
            print("\n" + "=" * 60)
            print("✗ Failed to fix XML file")
            print("=" * 60)
            sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Unexpected exception: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
