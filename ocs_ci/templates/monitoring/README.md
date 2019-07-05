# OCS Monitoring

This directory contains yaml files necessary to deploy OCS cluster with
monitoring enabled.

The yaml files come from [Rook project](https://github.com/rook/rook/), and as
such were originally created by Rook community under Apache 2.0 license.

If you want to have your cluster deployed with monitoring, set
`monitoring_enabled` to `true`, and use `openshift-storage` as namespace for
OCS components, as shown in `conf/examples/monitoring.yaml` example config
file.

Based on:

- [comment #10 of BZ 1731551](https://bugzilla.redhat.com/show_bug.cgi?id=1731551#c10)
- [comment #18 of BZ 1731551](https://bugzilla.redhat.com/show_bug.cgi?id=1731551#c18)
- [rook commit 1b6fe840f6ae7](https://github.com/rook/rook/commit/1b6fe840f6ae7372a9675ba727ecc65326708aa8)
