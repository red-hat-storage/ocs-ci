import contextlib
from unittest.mock import MagicMock

import pytest

from ocs_ci.ocs.resources import storageconsumer
from ocs_ci.ocs import version


@pytest.fixture
def patched_consumer(monkeypatch):
    """A StorageConsumer whose cluster I/O boundaries are mocked."""
    # bypass @if_version(">4.18")
    monkeypatch.setattr(version, "skipif_ocs_version", lambda expr: True)
    # make config context a no-op
    monkeypatch.setattr(
        storageconsumer.config,
        "RunWithConfigContext",
        lambda ctx: contextlib.nullcontext(),
    )
    # minimal consumer CR template
    monkeypatch.setattr(
        storageconsumer.templating,
        "load_yaml",
        lambda path: {"metadata": {}, "spec": {}},
    )
    dumped = {}
    monkeypatch.setattr(
        storageconsumer,
        "dump_data_to_temp_yaml",
        lambda data, path: dumped.update(data),
    )
    created_cms = []
    monkeypatch.setattr(
        storageconsumer,
        "create_ca_configmap",
        lambda name, namespace, ca_pem, key="ca.crt": created_cms.append(
            {"name": name, "namespace": namespace, "ca_pem": ca_pem, "key": key}
        ),
    )

    consumer = storageconsumer.StorageConsumer("consumer-a", namespace="prov-ns")
    consumer.ocp = MagicMock()
    return consumer, dumped, created_cms


def test_mtls_args_set_spec_and_create_configmap(patched_consumer):
    consumer, dumped, created_cms = patched_consumer
    consumer.create_storage_consumer(
        client_ca_pem="CA-PEM",
        client_san="consumer-a.client.example.com",
        client_ca_configmap_name="consumer-a-client-ca",
    )
    assert dumped["spec"]["clientCAConfigMap"] == {"name": "consumer-a-client-ca"}
    assert dumped["spec"]["clientSAN"] == "consumer-a.client.example.com"
    assert created_cms == [
        {
            "name": "consumer-a-client-ca",
            "namespace": "prov-ns",
            "ca_pem": "CA-PEM",
            "key": "ca.crt",
        }
    ]


def test_no_mtls_args_leaves_spec_untouched(patched_consumer):
    consumer, dumped, created_cms = patched_consumer
    consumer.create_storage_consumer()
    assert "clientCAConfigMap" not in dumped["spec"]
    assert "clientSAN" not in dumped["spec"]
    assert created_cms == []


def test_partial_material_raises(patched_consumer):
    consumer, _, _ = patched_consumer
    with pytest.raises(ValueError):
        consumer.create_storage_consumer(client_ca_pem="CA-PEM")
