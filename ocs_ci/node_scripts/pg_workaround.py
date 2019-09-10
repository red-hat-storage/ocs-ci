#!/usr/bin/env python
import subprocess
import time

all_pools = subprocess.check_output('ceph osd pool ls', shell=True)
pools = all_pools.split('\n')
print("Enabling pg_autoscaler")
subprocess.check_output('ceph mgr module enable pg_autoscaler', shell=True)
for pool in pools:
    if pool != '':
        print("modifying pool: " + pool)
        cmd = "ceph osd pool set " + pool + " pg_autoscale_mode on"
        print subprocess.check_output(cmd, shell=True)
# wait for few seconds for status update
time.sleep(5)
print subprocess.check_output('ceph osd pool autoscale-status', shell=True)
