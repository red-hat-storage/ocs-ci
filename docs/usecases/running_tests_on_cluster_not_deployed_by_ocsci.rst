How to execute tests on a cluster not deployed by ocs-ci?
=========================================================

Running ocs-ci test cases on clusters not deployed by ocs-ci itself is
possible, but because ocs-ci was not maintained with this use case in
mind, it requires few additional steps as explained below. Without
paying attention to these details, test run could fail or report invalid
results (both false negative or false positive results are possible).

.. note::

    When the following issues are fixed, most of the *additional steps*
    explained here won’t be necessary:

    - `#2347`_ platform detection/validation
    - `#2523`_ remove platform specific values from default_config.yaml file
    - `#3042`_ allow test execution on already installed IPI cluster without
      specifying platform details

.. _`#2347`: https://github.com/red-hat-storage/ocs-ci/issues/2347
.. _`#2523`: https://github.com/red-hat-storage/ocs-ci/issues/2523
.. _`#3042`: https://github.com/red-hat-storage/ocs-ci/issues/3042

Prerequisites
-------------

We assume that you have:

- Admin access to OCP cluster, where OCS has been already installed.
- Admin access to cloud platform account where OCP cluster is running, if you
  plan to execute platform specific or disruption test cases on cloud platform
  already supported by ocs-ci.
- Cluster directory of this OCP/OCS cluster on your machine, with all
  files as produced by ``openshift-install`` tool.
- Local copy of ocs-ci, installed in virtualenv on your machine, see
  *Installing* section in :doc:`/getting_started` guide for details.
- `Red Hat bugzilla`_ account.

.. _`Red Hat bugzilla`: https://bugzilla.redhat.com/

Preparation of test execution command
-------------------------------------

On your machine, go to ocs-ci directory and activate it’s virtual env.
Check that ``oc`` tool is available (if not, update ``PATH`` environment
variable or you can create symlink for it in ``bin`` directory where you have
ocs-ci installed). You don’t need to set ``KUBECONFIG`` environment variable.

Tests are executed via ``run-ci`` command (a wrapper of `pytest cli tool`_).
But since ocs-ci was not used to deploy the cluster (and issues like `#2347`_),
you have to specify the following information via command line options of
``run-ci`` command.

================================================================ ======================
Information                                                      ``run-ci`` option
================================================================ ======================
Path to openshift cluster dir (from ``openshift-install`` tool)  ``--cluster-path=PATH``
Path to ocs-ci config file with platform and deployment info     ``--ocsci-conf=PATH``
Openshift cluster name (chosen during OCP installation)          ``--cluster-name=NAME``
OCP version                                                      ``--ocp-version=VERSION``
OCS version (check OCS operator)                                 ``--ocs-version=VERSION``
Is OCS using already released builds? Use option if yes.         ``--live-deploy``
================================================================ ======================

Besides that, you will also need to specify which test cases to run and enable
`JUnit XML`_ test report (which is important for sharing the test results). For
example, to run ``tier1`` test suite, you will use the following ``run-ci``
options::

    --junit-xml=clustername.2020-10-02.tier1.xml -m tier1 tests

Moreover you should also configure and enable `bugzilla pytest plugin`_,
so that tests which are affected by known bugs will be skipped (see
:doc:`/usage` for example of expected config file).

Putting it all together, a full example of ocs-ci execution command would look
like this:

.. code-block:: console

   (venv) $ run-ci --cluster-path=~/cluster-2020-10-02 \
                   --ocsci-conf=conf/deployment/gcp/ipi_1az_rhcos_3m_3w.yaml \
                   --cluster-name=mbukatov-2020-10-02 \
                   --ocp-version=4.5 \
                   --ocs-version=4.5 \
                   --bugzilla \
                   -m tier1 \
                   --junit-xml=mbukatov.2020-10-02.tier1.xml \
                   tests

This example assumes that:

- ``~/cluster-2020-10-02`` is a cluster dir, as created by
  ``openshift-install ... --dir=~/cluster-2020-10-02`` command
- Config file ``bugzilla.cfg`` (in ocs-ci directory or home directory) contains
  reference and credentials to `Red Hat bugzilla`_
- Config file ``conf/deployment/gcp/ipi_1az_rhcos_3m_3w.yaml`` describes both
  platform the cluster is deployed on, and it's configuration - see details
  in a section below to learn how to select or create it.
- OCP version is ``4.5.1``, so we specify it as ``--ocp-version 4.5``.
- OCS version is ``4.5.0-543.ci``, so we specify it as ``--ocs-version 4.5``
  and don't use ``--live-deploy`` option.

.. _`JUnit XML`: https://docs.pytest.org/en/stable/usage.html#creating-junitxml-format-files
.. _`pytest cli tool`: https://docs.pytest.org/en/stable/usage.html
.. _`bugzilla pytest plugin`: https://github.com/eanxgeek/pytest_marker_bugzilla

How do I specify ocs-ci config with platform and deployment info?
-----------------------------------------------------------------

Check if platform where the cluster is deployed is available in
`ocs-ci/conf/deployment`_ directory.

If yes, go to the directory of the platform and check which config files
matches your environment best. Then copy it into a new config file, open it
and tweak it. Update at least the following values to match your cluster:

- ``deployment_type``: either ``ipi`` or ``upi`` (this is important because
  some tests cases are executed on particular type only)
- ``region``: cloud region as specified during openshift-install driven
  deployment (this is necessary to be correctly set for some MCG/NooBaa test
  cases)
- ``base_domain``: DNS zone as specified during openshift-install driven
  deployment (this is necessary to be correctly set for some MCG/NooBaa test
  cases)

This is not a full list though, it's possible that further tweaks are necessary
in special cases (such as LSO, external cluster, ...).

When you are done with tweaking, pass this new config file to ``--ocsci-conf``
option as explained in previous section.

.. _`ocs-ci/conf/deployment`: https://github.com/red-hat-storage/ocs-ci/tree/master/conf/deployment

Access to platform where the cluster is deployed
------------------------------------------------

If you are trying to run tests on a platfrom which is already supported by
ocs-ci, and are planning to run platform specific test cases, you need to make
sure you configure access to the underlying platform.

For example you need to run a test case which kills Azure node directly via
Azure API, and then observes how OCS handles that. Obviously, you need to have
admin access to Azure project where the cluster is running. See an overview of
expected file paths of platform credential files (these paths are based on file
paths used by both platform native cli admin tools and openshift-install).

========  ================
Platform  Credentials file
--------  ----------------
AWS       ``~/.aws/credentials``
Azure     ``~/.azure/osServicePrincipal.json``
GCP       ``~/.gcp/osServiceAccount.json``
========  ================

See also description of credentials setup in :doc:`/getting_started` guide,
including additional files in ``ocs-ci/data/`` directory.

Access to cloud object storage (MCG only)
-----------------------------------------

If you are going to run some of `MCG test cases`_, you also need to create
``data/auth.yaml`` file inside of ocs-ci directory, with credentials for cloud
object storage accounts used by MCG.

Minimal structure of the ``auth.yaml`` file is currently described in `this
github comment
<https://github.com/red-hat-storage/ocs-ci/issues/2649#issuecomment-671037451>`_.

.. note::

    Known issues related to ``auth.yaml`` file:

    - `#2649`_ Document changes to auth.yaml to be used by CloudManager
    - `#2623`_ Pick a source of truth for auth.yaml

.. _`MCG test cases`: https://github.com/red-hat-storage/ocs-ci/tree/master/tests/manage/mcg
.. _`#2649`: https://github.com/red-hat-storage/ocs-ci/issues/2649
.. _`#2623`: https://github.com/red-hat-storage/ocs-ci/issues/2623


What if I need to run tests on a new platform?
----------------------------------------------

Then you need to either try to reuse config for a similar platform, but whether
such option is valid depends on a nature of a test cases to be executed and the
platform itself.

Another option is to create a minimal patch for ocs-ci which defines the new
platform enough to allow a test execution. See example of `such patch for Azure
platform <https://github.com/red-hat-storage/ocs-ci/pull/2056/files>`_. Again,
such quick approach is not sufficient for some platform specific test cases.

What if I see lot of warnings about catalogsource?
--------------------------------------------------

If you are using stable (aka live or released) version of OCS and specified OCS
version via ``run-ci`` options (including ``--live-deploy`` option) as
explained above, warnings similar to the ones show below the are a red
herring::

    Failedtogetresource:ocs-catalogsourceofkind:CatalogSource,selector:None,Error:Errorduringexecutionofcommand:oc-nopenshift-marketplace--kubeconfig/home/ocsqe/data/mbukatov-dc2-cluster_20200226T131741/auth/kubeconfiggetCatalogSourceocs-catalogsource-n
    openshift-marketplace-oyaml.
    ErrorisErrorfromserver(NotFound):catalogsources.operators.coreos.com"ocs-catalogsource"notfound

    Numberofattemptstogetresourcereached!
    Failedtogetresource:ocs-operatorsourceofkind:OperatorSource,selector:None,Error:Errorduringexecutionofcommand:oc-nopenshift-marketplace--kubeconfig/home/ocsqe/data/mbukatov-dc2-cluster_20200226T131741/auth/kubeconfiggetOperatorSourceocs-operatorsourc
    e-nopenshift-marketplace-oyaml.
    ErrorisErrorfromserver(NotFound):operatorsources.operators.coreos.com"ocs-operatorsource"notfound

    Numberofattemptstogetresourcereached!

And as such can be safely ignored. This is a known ocs-ci issue `#1556`_.

So if you are running into some problem and see this error, you need to inspect
the logs further to find the real problem.

.. _`#1556`: https://github.com/red-hat-storage/ocs-ci/issues/1556
