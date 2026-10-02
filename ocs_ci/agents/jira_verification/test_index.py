"""Match test names cited in a Jira bug to tests under tests/."""

import ast
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TEST_NAME = re.compile(r"(?<![A-Za-z0-9_])(test_[A-Za-z0-9_]+)")
_NODE_ID = re.compile(r"(?<![\w./])(tests/(?:[\w.-]+/)*test_[\w.-]+\.py(?:::[\w]+)+)")
_TEST_FILE = re.compile(r"(?<![\w./])((?:tests/)?(?:[\w.-]+/)*test_[\w.-]+\.py)(?!::)")
_POLARION = re.compile(r"\b(OCS-\d+)\b")
_catalog = None


def matching_tests(issue, tests_root=None):
    """
    List tests under tests/ that the bug names exactly.

    A match is a pytest node id, a test module path, a test function name,
    or a Polarion id defined on a test. A name that is only part of a longer
    test name is not a match.

    Args:
        issue (dict): Jira verification payload, or report fields when the
            payload was not cached.
        tests_root (Path): tests directory. Defaults to the repo tests folder.

    Returns:
        list: name and location for each match, in the order named in the bug.

    """
    text = bug_report_text(issue)
    catalog = test_catalog(tests_root)
    covered = _covered_spans(text)
    found = []
    seen = set()

    def add(node):
        location = node["location"]
        if location in seen:
            return
        seen.add(location)
        found.append({"name": node["name"], "location": location})

    for match in _NODE_ID.finditer(text):
        node = catalog["by_location"].get(match.group(1))
        if node:
            add(node)
    for match in _TEST_FILE.finditer(text):
        for node in _nodes_for_file(match.group(1), catalog):
            add(node)
    for match in _TEST_NAME.finditer(text):
        if match.start() in covered:
            continue
        for node in catalog["by_name"].get(match.group(1), []):
            add(node)
    for match in _POLARION.finditer(text):
        for node in catalog["by_polarion"].get(match.group(1), []):
            add(node)
    locations = [item["location"] for item in found]
    key = (issue or {}).get("key") or "the issue"
    if not locations:
        logger.info(f"Matched no tests for {key}")
    elif len(locations) <= 10:
        logger.info(f"Matched {len(locations)} tests for {key}: {', '.join(locations)}")
    else:
        shown = ", ".join(locations[:10])
        logger.info(f"Matched {len(locations)} tests for {key}: {shown}, ...")
    return found


def bug_report_text(issue):
    """
    Return the bug text that can name a test.

    Args:
        issue (dict): Verification payload or report fields.

    Returns:
        str: Title, description, sections, comments, and source issues.

    """
    issue = issue or {}
    parts = [
        issue.get("summary") or "",
        issue.get("description") or "",
        issue.get("environment") or "",
        issue.get("bug_description") or "",
        issue.get("additional_info") or "",
    ]
    sections = issue.get("sections") or {}
    if isinstance(sections, dict):
        parts.extend(str(value) for value in sections.values())
    for comment in issue.get("comments") or []:
        if isinstance(comment, dict):
            parts.append(comment.get("body") or "")
    for note in issue.get("verification_notes") or []:
        if isinstance(note, dict):
            parts.append(note.get("body") or "")
    for step in issue.get("verification_steps") or []:
        if isinstance(step, dict):
            parts.append(step.get("action") or "")
            parts.append(step.get("command") or "")
    for source in issue.get("source_issues") or []:
        if isinstance(source, dict):
            parts.append(bug_report_text(source))
    return "\n".join(part for part in parts if part)


def test_catalog(tests_root=None):
    """
    Index test functions under tests/.

    Args:
        tests_root (Path): tests directory. The default index is cached.

    Returns:
        dict: by_location, by_name, by_file, by_basename, and by_polarion.

    """
    global _catalog
    if tests_root is None and _catalog is not None:
        return _catalog
    root = Path(tests_root) if tests_root else _REPO_ROOT / "tests"
    catalog = {
        "by_location": {},
        "by_name": {},
        "by_file": {},
        "by_basename": {},
        "by_polarion": {},
    }
    if root.is_dir():
        for path in sorted(root.rglob("test_*.py")):
            _index_file(path, root, catalog)
    if tests_root is None:
        _catalog = catalog
    return catalog


def _index_file(path, tests_root, catalog):
    """
    Add test functions from one module to the catalog.

    Args:
        path (Path): Python test module.
        tests_root (Path): tests directory.
        catalog (dict): Index being built.

    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeError):
        return
    relative = "tests/" + path.relative_to(tests_root).as_posix()
    nodes = []

    class Visitor(ast.NodeVisitor):
        def __init__(self):
            self.stack = []
            self.polarion = []

        def visit_ClassDef(self, node):
            self.stack.append(node.name)
            self.polarion.append(_polarion_ids(node))
            self.generic_visit(node)
            self.polarion.pop()
            self.stack.pop()

        def visit_FunctionDef(self, node):
            self._function(node)

        def visit_AsyncFunctionDef(self, node):
            self._function(node)

        def _function(self, node):
            if not node.name.startswith("test_"):
                return
            location = "::".join([relative, *self.stack, node.name])
            record = {
                "name": node.name,
                "location": location,
            }
            nodes.append(record)
            catalog["by_location"][location] = record
            catalog["by_name"].setdefault(node.name, []).append(record)
            ids = []
            for group in self.polarion:
                ids.extend(group)
            ids.extend(_polarion_ids(node))
            ids.extend(_polarion_ids_in_body(node))
            for polarion_id in ids:
                catalog["by_polarion"].setdefault(polarion_id, [])
                if record not in catalog["by_polarion"][polarion_id]:
                    catalog["by_polarion"][polarion_id].append(record)

    Visitor().visit(tree)
    catalog["by_file"][relative] = nodes
    catalog["by_basename"].setdefault(path.name, []).extend(nodes)


def _nodes_for_file(mention, catalog):
    """
    Return tests in a module the bug named.

    Args:
        mention (str): File path or file name from the bug.
        catalog (dict): Test index.

    Returns:
        list: Test records in that module.

    """
    norm = mention.lstrip("./")
    if norm in catalog["by_file"]:
        return catalog["by_file"][norm]
    if "/" not in norm:
        return catalog["by_basename"].get(norm, [])
    hits = []
    for path, nodes in catalog["by_file"].items():
        if path.endswith("/" + norm):
            hits.extend(nodes)
    return hits


def _covered_spans(text):
    """
    Return character offsets that belong to a node id or a test file path.

    Args:
        text (str): Bug text.

    Returns:
        set: Offsets that must not be read as a bare test function name.

    """
    covered = set()
    for pattern in (_NODE_ID, _TEST_FILE):
        for match in pattern.finditer(text):
            covered.update(range(match.start(), match.end()))
    return covered


def _polarion_ids(node):
    """
    Read Polarion ids from decorators on a class or test.

    Args:
        node (ast.AST): Class or function.

    Returns:
        list: Polarion ids, such as OCS-5871.

    """
    found = []
    for decorator in node.decorator_list:
        call = decorator
        if not isinstance(call, ast.Call):
            continue
        name = _call_name(call.func)
        if name not in {"polarion_id", "pytest.mark.polarion_id"}:
            continue
        for arg in call.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                if arg.value not in found:
                    found.append(arg.value)
    return found


def _polarion_ids_in_body(node):
    """
    Read Polarion ids used inside a test, including parameter marks.

    Args:
        node (ast.AST): Test function.

    Returns:
        list: Polarion ids.

    """
    found = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        name = _call_name(child.func)
        if name not in {"polarion_id", "pytest.mark.polarion_id"}:
            continue
        for arg in child.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                if arg.value not in found:
                    found.append(arg.value)
    return found


def _call_name(func):
    """
    Return the dotted name of a decorator call.

    Args:
        func (ast.AST): Function portion of a Call node.

    Returns:
        str: Name, or an empty string when it is not a simple name.

    """
    if isinstance(func, ast.Name):
        return func.id
    parts = []
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if isinstance(func, ast.Name):
        parts.append(func.id)
        return ".".join(reversed(parts))
    return ""
