"""
Live Cluster Debugger for OCS-CI.

When a test fails during execution, this module spawns a Claude Code session
that investigates the live cluster to determine the root cause. It reads the
test source code, test logs, and runs read-only ``oc`` commands to classify
the failure as product_bug, test_bug, infra_issue, or known_issue.

Usage::

    from ocs_ci.utility.live_debugger import LiveClusterDebugger

    debugger = LiveClusterDebugger(model="sonnet", max_budget_usd=1.00)
    result = debugger.investigate(
        test_name="test_create_pvc",
        test_nodeid="tests/functional/pv/test_pvc.py::TestPVC::test_create_pvc",
        test_source_path="/path/to/test_pvc.py",
        traceback_text="...",
        markers="green_squad,tier1",
        test_start_time="2025-01-15T10:30:00Z",
    )

The module is integrated into the pytest hooks via ``--live-debug`` CLI flag
in ``ocscilib.py``.
"""

from ocs_ci.utility.live_debugger.debugger import LiveClusterDebugger

__all__ = ["LiveClusterDebugger"]
