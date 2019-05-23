"""
In this pytest plugin we will keep all our pytest marks used in our tests and
all related hooks/plugins to markers.
"""

import pytest

# tier marks
tier1 = pytest.mark.tier1(value=1)
tier2 = pytest.mark.tier2(value=2)
tier3 = pytest.mark.tier3(value=3)
tier4 = pytest.mark.tier4(value=4)

tier_marks = [tier1, tier2, tier3, tier4]

# team marks

e2e = pytest.mark.e2e
ecosystem = pytest.mark.ecosystem
manage = pytest.mark.manage

tier_marks = [manage, ecosystem, e2e]

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
upgrade = pytest.mark.upgrade

# testing marker this is just for testing purpose if you want to run some test
# under development, you can mark it with @run_this and run pytest -m run_this
run_this = pytest.mark.run_this


# here is the place to implement some plugins hooks which will process marks
# if some operation neeeds to be done for some specific marked tests.
