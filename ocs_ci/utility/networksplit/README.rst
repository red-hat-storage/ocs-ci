=====================
 Network split setup
=====================

This utility module provides functionality to block (and unblock) network
traffic between k8s zones, implemented by updating firewall rules on all nodes
of OpenShift 4 cluster. It is expected to be used by test setup and teardown
fixtures.

This is useful when you need to separate network between given zones, without
affecting other traffic and with no assumptions about networking configuration
of the platform the cluster is deployed on (under normal conditions, network
separation like this could be done by tweaking network components between
zones).

Assumptions
===========

We assume that the cluster has 3 zones, ``a``, ``b`` and ``c``. The actual
zone names are not important, it's a convention followed in code of this
utility module and also in stretch  cluster test plan, which also assumes that
``a`` represents arbiter zone, while ``b`` and ``c`` represents data zones.

While Linux kernel of RHEL CoreOS (RHCOS) uses ``nftables`` internally, the
``iptables`` cli tool which uses ``nftables`` backed is preinstalled on RHCOS
hosts of OCP 4 clusters. The firewall script (see below) thus assumes that
``iptables`` cli tool is available on the nodes of the cluster.

Nodes of openshift cluster keep it's time synchronized via ntp.

We assume that there are only ``master`` and ``worker`` MachineConfigPools.
If you configured storage machines to run in dedicated ``infra`` machine
config pool, the network split configuration would not have any effect on
storage nodes.

Overview of the approach
========================

Network split firewall script
-----------------------------

Traffic from zone ``a`` to zone ``b`` is blocked by inserting ``DROP`` rules
for each machine of zone ``b`` into ``INPUT`` and ``OUTPUT`` chains of default
``iptables`` table on all machines of zone ``a`` via ``iptables`` tool.

This is implemented via ``network-split.sh`` script, which consumes zone
configuration via ``ZONE_A``, ``ZONE_B`` and ``ZONE_C`` env variables, detects
zone it is running within and applies firewall changes based on the split
configuration which it received from the command line.

Split configuration specifies list of zone tuples, and the network split is
made for traffic between each zone tuple. For example:

- ``ab`` means that traffic between zone ``a`` and ``b`` will be dropped in
  both directions (via changes in firewall configuration of zone ``a``)
- ``ab-bc`` means that communication in both directions is blocked between
  zone ``a`` and zone ``b``, and also between zone ``b`` and zone ``c``

One can see what changes will be made via ``-d`` option:

::

    $ export ZONE_A="10.1.161.27"
    $ export ZONE_B="10.1.160.175 10.1.160.180 10.1.160.188 10.1.160.198"
    $ export ZONE_C="10.1.161.115 10.1.160.192 10.1.160.174 10.1.160.208"
    $ ./network-split.sh -d setup ab-ac
    ZONE_A="10.40.195.21"
    ZONE_B="10.1.160.175 10.1.160.180 10.1.160.188 10.1.160.198"
    ZONE_C="10.1.161.115 10.1.160.192 10.1.160.174 10.1.160.208"
    current zone: ZONE_A
    ab: ZONE_B will be blocked from ZONE_A
    iptables -A INPUT -s 10.1.160.175 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.175 -j DROP -v
    iptables -A INPUT -s 10.1.160.180 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.180 -j DROP -v
    iptables -A INPUT -s 10.1.160.188 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.188 -j DROP -v
    iptables -A INPUT -s 10.1.160.198 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.198 -j DROP -v
    ac: ZONE_C will be blocked from ZONE_A
    iptables -A INPUT -s 10.1.161.115 -j DROP -v
    iptables -A OUTPUT -d 10.1.161.115 -j DROP -v
    iptables -A INPUT -s 10.1.160.192 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.192 -j DROP -v
    iptables -A INPUT -s 10.1.160.174 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.174 -j DROP -v
    iptables -A INPUT -s 10.1.160.208 -j DROP -v
    iptables -A OUTPUT -d 10.1.160.208 -j DROP -v

Systemd Units
-------------

The firewall script is not used directly, but through *stoppable oneshot
service* template ``network-split@.service``. To use it, we need to chose
particular network split configuration, eg. ``ab-bc``,  and then form so
called "instantiated" service name ``network-split@ab-ac.service``.
When such "instantiated" service is started, firewall changes to achieve
selected network split are applied and since then systemd is tracking this
service as started. Stopping the service reverts the firewall changes back,
removing the network split. The logs from the firewall script available via
journald as expected.

Example of starting network split for ``ab-bc`` and checking it's status::

    # systemctl start  network-split@ab-bc
    # systemctl status network-split@ab-bc
    ● network-split@ab-bc.service - Firewall configuration for a network split
       Loaded: loaded (/etc/systemd/system/network-split@.service; disabled; vendor preset: disabled)
       Active: active (exited) since Sat 2021-03-06 00:23:18 UTC; 4min 49s ago
      Process: 16380 ExecStart=/usr/bin/bash -c /etc/network-split.sh setup ab-bc (code=exited, status=0/SUCCESS)
     Main PID: 16380 (code=exited, status=0/SUCCESS)
          CPU: 8ms

    Mar 06 00:23:18 compute-5 systemd[1]: Starting Firewall configuration for a network split...
    Mar 06 00:23:18 compute-5 bash[16380]: ZONE_A="10.1.161.27"
    Mar 06 00:23:18 compute-5 bash[16380]: ZONE_B="10.1.160.175 10.1.160.180 10.1.160.188 10.1.160.198"
    Mar 06 00:23:18 compute-5 bash[16380]: ZONE_C="10.1.161.115 10.1.160.192 10.1.160.174 10.1.160.208"
    Mar 06 00:23:18 compute-5 bash[16380]: current zone: ZONE_C
    Mar 06 00:23:18 compute-5 bash[16380]: ab: ZONE_B will be blocked from ZONE_A
    Mar 06 00:23:18 compute-5 bash[16380]: bc: ZONE_C will be blocked from ZONE_B
    Mar 06 00:23:18 compute-5 systemd[1]: Started Firewall configuration for a network split.

This would work well on a single node, but in our case we need to apply this
on multiple machines at the same time. Moreover we also need to make sure that
the service is stopped after some time, reverting the network split issue.
For this reason, we don't start the network split service directly, but via
systemd timers, which allows us to schedule start and stop of the network split
service in advance at the same time on all nodes of the cluster.

For each network split configuration we have in stretch cluster test plan,
there is one setup timer template which starts the service at given time:

- ``network-split-ab-ac-setup@.timer``
- ``network-split-ab-setup@.timer``
- ``network-split-ab-bc-setup@.timer``
- ``network-split-bc-setup@.timer``

And then single teardown timer template ``network-split-teardown@.timer``,
which is used to schedule stop of any of the network split services to revert
the firewall changes back into original state.

Parameter of these timer templates is a unix epoch timestamp of the time when
we intend to start or stop the network split, eg.
``network-split-teardown@1614990498.timer``.

This is how a network split configuration is applied during test setup,
and restored during test teardown.

References:

- `systemd.service(5) <https://www.freedesktop.org/software/systemd/man/systemd.service.html>`_
  (for details about service templates or example of stoppable oneshot service)
- `systemd.timer(5) <https://www.freedesktop.org/software/systemd/man/systemd.timer.html>`_

MachineConfig
-------------

For the approach explained above to work, we need to deploy firewall script,
file with ``ZONE_{A,B,C}`` environment variables and systemd service and timer
units. We achieve this via MachineConfig, which allows us to deploy files in
``/etc`` directory and system units on all nodes of both ``master`` and
``worker`` MachineConfigPools.

Using openshift interface has an advantage of better visibility of such
changes, which can be easily inspected via machine config operator (MCO) API.
Downside of this approach is that MCO is going to drain and reboot every node
one by one, which increases time necessary to deploy the configuration.

For this reason, we use MachineConfig only to deploy the script and unit files,
while scheduling of the timers to setup and teardown a network split is done
via direct connection (using ssh or oc debug) to each node.

References:

- `How does Machine Config Pool work? <https://www.redhat.com/en/blog/openshift-container-platform-4-how-does-machine-config-pool-work>`_
- `Post-installation machine configuration tasks <https://docs.openshift.com/container-platform/4.6/post_installation_configuration/machine-configuration-tasks.html#using-machineconfigs-to-change-machines>`_
- `machine-config-operator docs <https://github.com/openshift/machine-config-operator/tree/master/docs>`_
- `Ignition Configuration Specification v3.1.0 <https://coreos.github.io/ignition/configuration-v3_1/>`_

Usage
=====

See  :py:mod:`ocs_ci.utility.networksplit.main` module for functions you can
use in test setup and teardown code. Check which fixtures already use these
functions to see if you can use existing fixtures.

High level overview of usage steps:

- deploy ``MachineConfig`` with network split script and unit files (this needs
  to done only once)
- schedule network split via systemd timer units on every node of the cluster
- wait for the 1st timer to trigger given network split scenario
- wait for the 2nd timer to trigger teardown, restoring the network
  configuration back
- optionally schedule another network split again

There are also 2 command line tools implementing this process, which can be
used to setup a network split outside of ocs-ci tests for (semi) manual
testing:

- ``ocs-network-split-setup``: fetches node IP addresses for ``ZONE_A``,
  ``ZONE_B`` and ``ZONE_C`` env variables, and creates yaml file with
  MachineConfig deploying firewall script and systemd unit files. This is done
  only once.

- ``ocs-network-split-sched``: schedules given network split configuration at
  given time

First we check that env file is generated fine, and then we have the machine
config yaml file generated and deployed::

    $ ocs-network-split-setup --zone-label-names foo-arbiter,data-a,data-b --print-env-only
    ZONE_A="10.1.160.40"
    ZONE_B="10.1.160.48 10.1.160.54 10.1.160.55 10.1.160.70"
    ZONE_C="10.1.160.38 10.1.160.51 10.1.160.53 10.1.160.57"
    $ ocs-network-split-setup --zone-label-names foo-arbiter,data-a,data-b -o network-split.mc.yaml
    $ oc create -f network-split.mc.yaml
    machineconfig.machineconfiguration.openshift.io/99-master-network-split created
    machineconfig.machineconfiguration.openshift.io/99-worker-network-split created

When the machine config is applied (check ``oc  get mcp`` if both pools are
updated), we can schedule 15 minute log network split of particular
configuration ``ab`` at given time::

    $ ocs-network-split-sched ab 2021-03-18T22:45 --split-len 15
