"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/helpers.py
Insertion point: end of file
Description: Create a Kubernetes Secret containing S3 (AWS-style) credentials

Review and merge this into ocs_ci/helpers/helpers.py before running the test.
"""

def create_s3_credentials_secret(secret_name, namespace, access_key, secret_key):
    """
    Create a Kubernetes Opaque secret with S3 credentials.

    Args:
        secret_name (str): Name of the secret to create
        namespace (str): Namespace in which to create the secret
        access_key (str): AWS access key ID value
        secret_key (str): AWS secret access key value

    Returns:
        OCP: The OCP object for the created secret
    """
    import base64
    from ocs_ci.ocs import ocp

    secret_data = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": secret_name,
            "namespace": namespace,
        },
        "type": "Opaque",
        "data": {
            "AWS_ACCESS_KEY_ID": base64.b64encode(access_key.encode()).decode(),
            "AWS_SECRET_ACCESS_KEY": base64.b64encode(secret_key.encode()).decode(),
        },
    }
    secret_obj = ocp.OCP(kind="Secret", namespace=namespace)
    secret_obj.create(body=secret_data)
    return secret_obj
