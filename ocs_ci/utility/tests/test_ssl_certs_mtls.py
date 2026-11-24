import pytest
from OpenSSL import crypto
from ocs_ci.utility.ssl_certs import generate_client_mtls_material, ClientMTLSMaterial


def test_material_fields_populated():
    mat = generate_client_mtls_material("client-a.example.com")
    assert isinstance(mat, ClientMTLSMaterial)
    for pem in (mat.ca_pem, mat.cert_pem, mat.key_pem):
        assert "-----BEGIN" in pem
    assert mat.san == "client-a.example.com"


def test_leaf_is_ca_signed():
    mat = generate_client_mtls_material("client-a.example.com")
    ca = crypto.load_certificate(crypto.FILETYPE_PEM, mat.ca_pem)
    leaf = crypto.load_certificate(crypto.FILETYPE_PEM, mat.cert_pem)
    assert leaf.get_issuer().CN == ca.get_subject().CN
    store = crypto.X509Store()
    store.add_cert(ca)
    crypto.X509StoreContext(store, leaf).verify_certificate()


def test_leaf_has_san_and_client_auth():
    mat = generate_client_mtls_material("client-a.example.com")
    leaf = crypto.load_certificate(crypto.FILETYPE_PEM, mat.cert_pem)
    exts = {
        leaf.get_extension(i).get_short_name().decode(): str(leaf.get_extension(i))
        for i in range(leaf.get_extension_count())
    }
    assert "DNS:client-a.example.com" in exts["subjectAltName"]
    assert "TLS Web Client Authentication" in exts["extendedKeyUsage"]


def test_empty_san_raises():
    with pytest.raises(ValueError):
        generate_client_mtls_material("")
