#!/usr/bin/env python
import argparse
import base64
import binascii
import os
import yaml

from os import environ as env
from configparser import ConfigParser
from semantic_version import Version


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-aws",
        dest="aws",
        action="store_false",
    )
    parser.add_argument(
        "--skip-pull-secret",
        dest="pull_secret",
        action="store_false",
    )
    parser.add_argument(
        "--skip-ocsci-conf",
        dest="ocsci_conf",
        action="store_false",
    )
    parser.add_argument(
        "--skip-bugzilla-conf",
        dest="bugzilla_conf",
        action="store_false",
    )
    return parser.parse_args()


def write_aws_creds():
    aws_profile = env["AWS_PROFILE"]
    # Write the credentials file
    creds = ConfigParser()
    creds[aws_profile] = dict(
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
    )
    creds_path = env["AWS_SHARED_CREDENTIALS_FILE"]
    os.makedirs(
        os.path.dirname(creds_path),
        exist_ok=True,
    )
    with open(creds_path, "w") as creds_file:
        creds.write(creds_file)

    # Write the config file
    conf = ConfigParser()
    conf[aws_profile] = dict(
        region=env["AWS_REGION"],
        output="text",
    )
    conf_path = env["AWS_CONFIG_FILE"]
    os.makedirs(
        os.path.dirname(conf_path),
        exist_ok=True,
    )
    with open(conf_path, "w") as conf_file:
        conf.write(conf_file)


def write_pull_secret():
    secret_dir = os.path.abspath(os.path.join(".", "data"))
    os.makedirs(secret_dir, exist_ok=True)
    secret = env["PULL_SECRET"]
    # In Jenkins, this is a JSON string. In GitLab CI, the JSON will be
    # base64-encoded.
    try:
        secret = base64.b64decode(secret).decode()
    except (binascii.Error, UnicodeDecodeError):
        pass
    with open(os.path.join(secret_dir, "pull-secret"), "w") as secret_file:
        secret_file.write(secret)
    with open(os.path.join(secret_dir, "auth.yaml"), "w") as auth_file:
        auth = dict(quay=dict(access_token=env["QUAY_TOKEN"]))
        auth_file.write(yaml.safe_dump(auth))


def get_ocsci_conf(upgrade_run=False, pre_upgrade=False):
    if pre_upgrade and not upgrade_run:
        raise ValueError("pre_upgrade implies upgrade_run")
    cluster_user = env["CLUSTER_USER"]
    pipeline_id = env["BUILD_ID"]
    conf_obj = dict(
        RUN=dict(
            log_dir=os.path.join(env["WORKSPACE"], "logs"),
        ),
        ENV_DATA=dict(
            platform="AWS",
            cluster_name=f"{cluster_user}-ocs-ci-{pipeline_id}",
            region=env["AWS_REGION"],
            base_domain=env["AWS_DOMAIN"],
            worker_instance_type="m5.4xlarge",
            cluster_namespace="openshift-storage",
        ),
        DEPLOYMENT=dict(),
        REPORTING=dict(
            gather_on_deploy_failure=True,
        ),
    )
    if env.get("DOWNSTREAM") == "true":
        conf_obj["REPORTING"]["us_ds"] = "DS"
    if env.get("SMTP_SERVER"):
        conf_obj["REPORTING"]["email"] = dict(smtp_server=env["SMTP_SERVER"])
    if env.get("SAVE_MEM_REPORT").lower() == "true":
        conf_obj["REPORTING"]["save_mem_report"] = True
    if upgrade_run:
        version = Version.coerce(env["OCS_REGISTRY_IMAGE"].split(":")[1]).truncate(
            "minor"
        )
        preup_version = f"{version.major}.{version.minor - 1}"
        # ocs-ci needs ENV_DATA.ocs_version to be equal to the *pre-upgrade*
        # version even during the upgrade phase.
        conf_obj["ENV_DATA"]["ocs_version"] = preup_version
        if pre_upgrade:
            ocp_version = ocp_version = f"{version.major}.{version.minor}-ga"
            conf_obj["DEPLOYMENT"]["ocp_version"] = ocp_version
            conf_obj["DEPLOYMENT"]["installer_version"] = ocp_version
            conf_obj["RUN"]["client_version"] = ocp_version
        else:
            conf_obj["UPGRADE"] = dict(
                upgrade_ocs_registry_image=env["OCS_REGISTRY_IMAGE"],
                upgrade_to_latest=False,
            )
    else:
        conf_obj["DEPLOYMENT"] = dict(ocs_registry_image=env["OCS_REGISTRY_IMAGE"])
    if env.get("OCP_VERSION"):
        conf_obj["DEPLOYMENT"]["ocp_version"] = env["OCP_VERSION"]
    return conf_obj


def write_ocsci_conf():
    upgrade = bool(env.get("UPGRADE"))
    ocp_conf = get_ocsci_conf(upgrade_run=upgrade, pre_upgrade=upgrade)
    ocp_conf["ENV_DATA"]["skip_ocs_deployment"] = True
    ocp_conf_path = os.path.join(env["WORKSPACE"], "ocs-ci-ocp.yaml")
    with open(ocp_conf_path, "w") as ocp_conf_file:
        ocp_conf_file.write(yaml.safe_dump(ocp_conf))

    if upgrade:
        ocs_pre_conf = get_ocsci_conf(upgrade_run=True, pre_upgrade=True)
        ocs_pre_conf["ENV_DATA"]["skip_ocp_deployment"] = True
        ocs_pre_conf_path = os.path.join(env["WORKSPACE"], "ocs-ci-pre-ocs.yaml")
        with open(ocs_pre_conf_path, "w") as ocs_pre_conf_file:
            ocs_pre_conf_file.write(yaml.safe_dump(ocs_pre_conf))

    ocs_conf = get_ocsci_conf(upgrade_run=upgrade, pre_upgrade=False)
    ocs_conf["ENV_DATA"]["skip_ocp_deployment"] = True
    ocs_conf_path = os.path.join(env["WORKSPACE"], "ocs-ci-ocs.yaml")
    with open(ocs_conf_path, "w") as ocs_conf_file:
        ocs_conf_file.write(yaml.safe_dump(ocs_conf))

    test_conf = get_ocsci_conf(upgrade_run=False)
    test_conf_path = os.path.join(env["WORKSPACE"], "ocs-ci-test.yaml")
    with open(test_conf_path, "w") as test_conf_file:
        test_conf_file.write(yaml.safe_dump(test_conf))


def write_bugzilla_conf():
    with open("bugzilla.cfg", "w") as bz_cfg_file:
        bz_cfg_file.write(env["BUGZILLA_CFG"])


if __name__ == "__main__":
    args = parse_args()
    if args.aws:
        write_aws_creds()
    if args.pull_secret:
        write_pull_secret()
    if args.ocsci_conf:
        write_ocsci_conf()
    if args.bugzilla_conf:
        write_bugzilla_conf()
