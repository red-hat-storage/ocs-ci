from ocs_ci.framework import config
import time

def test_pagerduty_logging():
    """
    Collect pagerduty alerts
    """
    config.RUN["thread_pagerduty_secret_update"] = "required"
    time.sleep(172800)
