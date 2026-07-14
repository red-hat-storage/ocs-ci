How to report test results from ocs-ci not executed by ocs-ci jenkins?
======================================================================

There are multiple report and log files created during ocs-ci test run, and for
failure analysis of the results, QE team requires all of these files to be
provided.

That said, if you want to just list names of the tests executed along with
their results, and don't expect QE team or anybody else to ever analyze a test
failure(s) in this test run, sharing sheer `JUnit XML`_ test report file is
enough.

.. _`JUnit XML`: https://docs.pytest.org/en/stable/usage.html#creating-junitxml-format-files

XML file with JUnit test report
-------------------------------

Providing this file is **mandatory**.

You need to specify a filename of `JUnit XML`_ report file as
``--junit-xml`` command line option of ``run-ci`` tool. When you forget to
specify this option, the file is not generated and you won't be able to share
the results with others.

ocs-ci log files
----------------

For each test run executed by ocs-ci (via ``run-ci`` command), a unique
directory named ``/tmp/ocs-ci-logs-${UNIX_TIMESTAMP}`` is created. The last
part of the name is a number representing unix timestamp taken when the test
run was started.

For example, after executing a single run, you will have one ocs-ci-logs
directory:

.. code-block:: console

    $ ls -ltr | grep 'ocs-ci-logs-[0-9]*$'
    drwxrwxr-x. 3 ocsqe ocsqe   60 Oct  7 23:25 ocs-ci-logs-1602105881

This directory contains a tree structure which follows a ocs-ci test case
hierarchy, with a log file for each test:

.. code-block:: console

    $ tree ocs-ci-logs-1602105881 | head
    ocs-ci-logs-1602105881
    └── tests
        ├── e2e
        │   ├── logging
        │   │   └── test_openshift-logging.py
        │   │       └── Testopenshiftloggingonocs
        │   │           └── test_create_new_project_to_verify_logging
        │   │               └── logs
        │   └── registry
        │       └── test_pod_from_registry.py

These log files are crucial to understand why a test case failed.

To share it with QE team, just create a tarball with the whole ocs-ci-logs
directory. Like in our example:

.. code-block:: console

    $ cd /tmp
    $ tar caf ~/ocs-ci-logs-1602105881.tar.gz ocs-ci-logs-1602105881
    $ ls  -lh ~/ocs-ci-logs-1602105881.tar.gz
    -rw-rw-r--. 1 ocsqe ocsqe 60M Oct  8 16:17 /home/ocsqe/ocs-ci-logs-1602105881.tar.gz

must gather data
----------------

Based on a request or because of a test failure, ocs-ci also tries to run `must
gather`_ to get additional evidence. Must gather data fetched this way are stored
in the following directories:

- OCP: ``/tmp/testcases_${UNIX_TIMESTAMP}/ocp_must_gather`` 
- OCS: ``/tmp/ocs-ci-logs-${UNIX_TIMESTAMP}_ocs_logs/ocs_must_gather``

.. note::

   There is ocs-ci issue `#3088`_ discussing simplification and unification of
   this naming scheme.

.. _`must gather`: https://docs.openshift.com/container-platform/4.5/support/gathering-cluster-data.html#about-must-gather_gathering-cluster-data
.. _`#3088`: https://github.com/red-hat-storage/ocs-ci/issues/3088

These directories contains a tree structure which follows product component
organization. For example:

.. code-block:: console

    $ tree /tmp/testcases_1602105881/ocp_must_gather | head
    /tmp/testcases_1602105881/ocp_must_gather
    └── quay-io-openshift-origin-must-gather-sha256-c5b27546b5bb33e0af0bdd7610a0f19075bb68c78f39233db743671b9f043f6b
        ├── cluster-scoped-resources
        │   ├── admissionregistration.k8s.io
        │   │   └── validatingwebhookconfigurations
        │   │       ├── autoscaling.openshift.io.yaml
        │   │       └── multus.openshift.io.yaml
        │   ├── apiextensions.k8s.io
        │   │   └── customresourcedefinitions
        │   │       ├── alertmanagers.monitoring.coreos.com.yaml
    $ tree /tmp/ocs-ci-logs-1602105881_ocs_logs/ocs_must_gather | head
    /tmp/ocs-ci-logs-1602105881_ocs_logs/ocs_must_gather
    └── quay-io-rhceph-dev-ocs-must-gather-sha256-02a84d07c42197311b6361097aa7e48156c6a186b75d8e9e32311c073736d1c0
        ├── ceph
        │   ├── cluster-scoped-resources
        │   │   └── storage.k8s.io
        │   │       └── storageclasses
        │   │           ├── ocs-storagecluster-cephfs.yaml
        │   │           ├── ocs-storagecluster-ceph-rbd.yaml
        │   │           ├── openshift-storage.noobaa.io.yaml
        │   │           └── standard.yaml

To share it with QE team, just create a tarball with all must gather
directories.

pytest temporary directory
--------------------------

When monitoring test cases are executed, additional evidence can be found in
pytest temporary directory.

Here is an example of pytest temporary directory right after execution of all
tier1 monitoring test cases:

.. code-block:: console

    $ tree /tmp/pytest-of-ocsqe/pytest-current/
    /tmp/pytest-of-ocsqe/pytest-current/
    ├── measurement_results
    │   ├── measure_workload_idle.json
    │   ├── workload_storageutilization_10G_cephfs.json
    │   └── workload_storageutilization_10G_rbd.json
    ├── test_monitoring_reporting_ok_w0
    ├── test_monitoring_reporting_ok_wcurrent -> /tmp/pytest-of-ocsqe/pytest-0/test_monitoring_reporting_ok_w0
    ├── test_workload_rbd_cephfs_10g0
    │   ├── objectconfig.workload_storageutilization_10G_cephfs.yaml
    │   └── objectconfig.workload_storageutilization_10G_rbd.yaml
    └── test_workload_rbd_cephfs_10gcurrent -> /tmp/pytest-of-ocsqe/pytest-0/test_workload_rbd_cephfs_10g0
    
    5 directories, 5 files

Measurement results files contain a timestamps of a measurement range and
archive of all k8s alerts active during this time. Workload files contain a
full deployment specification of the workload executed during metrics tests.
