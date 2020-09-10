from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs.mcg_workload import wait_for_active_pods


@libtest
def test_2_jobs(mcg_job_factory):
    """
    Create 2 jobs, one with custom runtime. Both should work at the same time.

    """
    custom_options = {'create': [('runtime', '48h')]}
    job1 = mcg_job_factory()
    job2 = mcg_job_factory(custom_options=custom_options)
    wait_for_active_pods(job1, 1)
    wait_for_active_pods(job2, 1)
