# Simple log reader/writer IO workload

Files in this directory are k8s yaml files, not jinja templates.

This workload can write a line with timestamp and a checksum of a previous line
into a log file periodically, and read such log file and verify that content of
the file is consistent (verifying the checksums there).

The goal of this workload is to test:

- IO running on all nodes of a cluster via k8s `Deployment`
- that there are no holes in the log during disruptions a cluster should handle
  (as evidence whether IO was affected)
- data consistency of the written data
- read/write IO stress such workload can create

Assumptions:

- all nodes have `topology.kubernetes.io/zone` label defined
