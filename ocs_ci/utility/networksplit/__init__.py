# -*- coding: utf8 -*-
"""Network split setup

This module provides functionality to block (and unblock) network traffic
between k8s zones, implemented by updating firewall rules on all nodes of
OpenShift 4 cluster. It is expected to be used by test setup and teardown
fixtures.

.. moduleauthor:: Martin Bukatovič
"""
