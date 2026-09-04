import pytest

from ocs_ci.utility import rgwutils


@pytest.mark.parametrize(
    "arbiter_deployment, expected",
    [
        (False, 1),
        (True, 2),
    ],
)
def test_get_rgw_count(monkeypatch, arbiter_deployment, expected):
    monkeypatch.setitem(
        rgwutils.config.DEPLOYMENT, "arbiter_deployment", arbiter_deployment
    )
    rgw_count = rgwutils.get_rgw_count()
    assert rgw_count == expected
