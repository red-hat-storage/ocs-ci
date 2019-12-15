"""
In this pytest plugin we will keep all our pytest marks used in our tests and
all related hooks/plugins to markers.
"""
import os
import pytest
from funcy import compose

from ocs_ci.ocs.constants import (
    ORDER_AFTER_UPGRADE,
    ORDER_BEFORE_UPGRADE,
    ORDER_UPGRADE,
)
from ocs_ci.framework import config
from ocs_ci.utility.utils import check_if_executable_in_path

# tier marks

tier1 = pytest.mark.tier1(value=1)
tier2 = pytest.mark.tier2(value=2)
tier3 = pytest.mark.tier3(value=3)
tier4 = pytest.mark.tier4(value=4)

tier_marks = [tier1, tier2, tier3, tier4]

# build acceptance
acceptance = pytest.mark.acceptance

# team marks

e2e = pytest.mark.e2e
ecosystem = pytest.mark.ecosystem
manage = pytest.mark.manage
libtest = pytest.mark.libtest

team_marks = [manage, ecosystem, e2e]

# components  and other markers
ocp = pytest.mark.ocp
rook = pytest.mark.rook
ui = pytest.mark.ui
csi = pytest.mark.csi
monitoring = pytest.mark.monitoring
workloads = pytest.mark.workloads
performance = pytest.mark.performance
scale = pytest.mark.scale
deployment = pytest.mark.deployment
destroy = pytest.mark.destroy
polarion_id = pytest.mark.polarion_id
bugzilla = pytest.mark.bugzilla

# upgrade related markers
# Requires pytest ordering plugin installed
order_pre_upgrade = pytest.mark.run(order=ORDER_BEFORE_UPGRADE)
order_upgrade = pytest.mark.run(order=ORDER_UPGRADE)
order_post_upgrade = pytest.mark.run(order=ORDER_AFTER_UPGRADE)
upgrade = compose(pytest.mark.upgrade, order_upgrade)
pre_upgrade = compose(pytest.mark.pre_upgrade, order_pre_upgrade)
post_upgrade = compose(pytest.mark.post_upgrade, order_post_upgrade)

# mark the test class with marker below to ignore leftover check
ignore_leftovers = pytest.mark.ignore_leftovers

# testing marker this is just for testing purpose if you want to run some test
# under development, you can mark it with @run_this and run pytest -m run_this
run_this = pytest.mark.run_this

# Skipif marks
google_api_required = pytest.mark.skipif(
    not os.path.exists(os.path.expanduser(
        config.RUN['google_api_secret'])
    ), reason="Google API credentials don't exist"
)

noobaa_cli_required = pytest.mark.skipif(
    not check_if_executable_in_path('noobaa'),
    reason='MCG CLI was not found'
)

aws_platform_required = pytest.mark.skipif(
    config.ENV_DATA['platform'].lower() != 'aws',
    reason="Tests are not running on AWS deployed cluster"
)

skip_always = pytest.mark.skipif(True, reason="Test was marked for intentional skipping")

# Filter warnings
filter_insecure_request_warning = pytest.mark.filterwarnings(
    'ignore::urllib3.exceptions.InsecureRequestWarning'
)

# here is the place to implement some plugins hooks which will process marks
# if some operation needs to be done for some specific marked tests.
