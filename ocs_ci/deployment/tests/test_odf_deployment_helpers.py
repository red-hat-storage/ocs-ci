# -*- coding: utf8 -*-

import copy

import pytest

from ocs_ci.deployment.helpers.odf_deployment_helpers import apply_csv_image_overrides


CSV_DATA = {
    "metadata": {"name": "ocs-operator.v4.19.0"},
    "spec": {
        "install": {
            "spec": {
                "deployments": [
                    {
                        "name": "ocs-operator",
                        "spec": {
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "ocs-operator",
                                            "image": "registry.redhat.io/odf4/ocs:1",
                                            "env": [
                                                {
                                                    "name": "WATCH_NAMESPACE",
                                                    "value": "",
                                                },
                                                {
                                                    "name": "PROVIDER_API_SERVER_IMAGE",
                                                    "value": "registry.redhat.io/odf4/ocs:1",
                                                },
                                            ],
                                        },
                                    ],
                                },
                            },
                        },
                    },
                ],
            },
        },
    },
}


def test_apply_csv_image_overrides_env_only():
    """
    Test that overriding an environment variable doesn't touch the container
    image, even though both reference the very same image.
    """
    csv_data = copy.deepcopy(CSV_DATA)
    custom_image = "quay.io/my-user/ocs:custom"

    applied = apply_csv_image_overrides(
        csv_data, {"env": {"PROVIDER_API_SERVER_IMAGE": custom_image}}
    )

    container = csv_data["spec"]["install"]["spec"]["deployments"][0]["spec"][
        "template"
    ]["spec"]["containers"][0]
    assert container["image"] == "registry.redhat.io/odf4/ocs:1"
    assert container["env"][1]["value"] == custom_image
    assert applied == {
        "ocs-operator/ocs-operator/env/PROVIDER_API_SERVER_IMAGE": custom_image
    }


def test_apply_csv_image_overrides_container_and_env():
    """
    Test that both the container image and an environment variable can be
    overridden at once, each one with a different image.
    """
    csv_data = copy.deepcopy(CSV_DATA)
    container_image = "quay.io/my-user/ocs:container"
    env_image = "quay.io/my-user/ocs:server"

    applied = apply_csv_image_overrides(
        csv_data,
        {
            "containers": {"ocs-operator": container_image},
            "env": {"PROVIDER_API_SERVER_IMAGE": env_image},
        },
    )

    container = csv_data["spec"]["install"]["spec"]["deployments"][0]["spec"][
        "template"
    ]["spec"]["containers"][0]
    assert container["image"] == container_image
    assert container["env"][1]["value"] == env_image
    assert len(applied) == 2


def test_apply_csv_image_overrides_drops_value_from():
    """
    Test that valueFrom is removed from an overridden environment variable, as
    value and valueFrom are mutually exclusive.
    """
    csv_data = copy.deepcopy(CSV_DATA)
    env = csv_data["spec"]["install"]["spec"]["deployments"][0]["spec"]["template"][
        "spec"
    ]["containers"][0]["env"][0]
    del env["value"]
    env["valueFrom"] = {"fieldRef": {"fieldPath": "metadata.namespace"}}

    apply_csv_image_overrides(csv_data, {"env": {"WATCH_NAMESPACE": "some-image"}})

    assert env == {"name": "WATCH_NAMESPACE", "value": "some-image"}


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"containers": {"no-such-container": "image"}}, id="container"),
        pytest.param({"env": {"NO_SUCH_ENV": "image"}}, id="env"),
    ],
)
def test_apply_csv_image_overrides_missing_target(overrides):
    """
    Test that overriding something which is not defined in the CSV fails
    instead of silently deploying the default images.
    """
    csv_data = copy.deepcopy(CSV_DATA)

    with pytest.raises(ValueError):
        apply_csv_image_overrides(csv_data, overrides)
