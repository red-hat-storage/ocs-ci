# Using multiple instances of Vault KMS in OCS-CI

Vault is one of the Key Management Systems, currently supported by ODF for encryption. QE maintains two instances of Vault: the community version of Vault and the enterprise version of Vault hosted on the Hashicorp Cloud Platform. The configuration and authentication details for these instances are defined in `data/auth.yaml` file under `vault` and `vault_hcp` sections respectively.
The community version of vault is free to use but does not have the enterprise features, like namespaces available. The HCP Vault instance provides access to enterprise features and is billed based on usage.

Each vault instance has one or more of these variables defined:
- `VAULT_ADDR` : Hostname of the vault instance
- `PORT` : The port used by Vault. Usually defaults to 8200
- `VAULT_CACERT_BASE64` : Base64 encoded CA certificate
- `VAULT_CLIENT_CERT_BASE64` : Base64 encoded client certificate
- `VAULT_CLIENT_KEY_BASE64` : Base64 encoded client key
- `VAULT_TLS_SERVER_NAME` : TLS server name for the vault instance, if applicable
- `VAULT_ROOT_TOKEN` : Vault root token or admin token
- `UNSEAL_KEY{1..5}` : Unseal keys for vault

Example:
```yaml
vault:
  VAULT_ADDR:
  PORT:
  VAULT_CACERT_BASE64:
  VAULT_CLIENT_CERT_BASE64:
  VAULT_CLIENT_KEY_BASE64:
  VAULT_TLS_SERVER_NAME: ''
  VAULT_ROOT_TOKEN:
  UNSEAL_KEY1:
  UNSEAL_KEY2:
  UNSEAL_KEY3:
  UNSEAL_KEY4:
  UNSEAL_KEY5:

vault_hcp:
  VAULT_ADDR:
  PORT:
  VAULT_CACERT_BASE64:
  VAULT_TLS_SERVER_NAME: ''
  VAULT_ROOT_TOKEN:
  VAULT_HCP_NAMESPACE:
```
