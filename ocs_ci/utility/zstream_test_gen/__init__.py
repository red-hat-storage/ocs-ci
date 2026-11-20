"""
Z-Stream Test Generator
=======================

AI-driven test generation for Lane C z-stream bug fix verification.

This tool queries Jira for bugs in a z-stream release, enriches each bug
with context from Rovo/Teamwork Graph and upstream fix PRs, classifies
them for automatability, generates ocs-ci verification tests using Claude,
validates the generated code, and opens draft PRs.

Usage::

    python -m ocs_ci.utility.zstream_test_gen --fix-version odf-4.22.1

"""
