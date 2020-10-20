#!/usr/bin/env python
import argparse
import base64
import binascii
import os
import yaml

from os import environ as env
from configparser import ConfigParser


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--skip-aws',
        dest='aws',
        action='store_false',
    )
    parser.add_argument(
        '--skip-pull-secret',
        dest='pull_secret',
        action='store_false',
    )
    parser.add_argument(
        '--skip-ocsci-conf',
        dest='ocsci_conf',
        action='store_false',
    )
    parser.add_argument(
        '--skip-bugzilla-conf',
        dest='bugzilla_conf',
        action='store_false',
    )
    return parser.parse_args()


def write_aws_creds():
    aws_profile = env['AWS_PROFILE']
    # Write the credentials file
    creds = ConfigParser()
    creds[aws_profile] = dict(
        aws_access_key_id=env['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=env['AWS_SECRET_ACCESS_KEY'],
    )
    creds_path = env['AWS_SHARED_CREDENTIALS_FILE']
    os.makedirs(
        os.path.dirname(creds_path),
        exist_ok=True,
    )
    with open(creds_path, 'w') as creds_file:
        creds.write(creds_file)

    # Write the config file
    conf = ConfigParser()
    conf[aws_profile] = dict(
        region=env['AWS_REGION'],
        output='text',
    )
    conf_path = env['AWS_CONFIG_FILE']
    os.makedirs(
        os.path.dirname(conf_path),
        exist_ok=True,
    )
    with open(conf_path, 'w') as conf_file:
        conf.write(conf_file)


def write_pull_secret():
    secret_dir = os.path.join(env['WORKSPACE'], 'data')
    os.makedirs(secret_dir, exist_ok=True)
    secret = env['PULL_SECRET']
    # In Jenkins, this is a JSON string. In GitLab CI, the JSON will be
    # base64-encoded.
    try:
        secret = base64.b64decode(secret).decode()
    except (binascii.Error, UnicodeDecodeError):
        pass
    with open(os.path.join(secret_dir, 'pull-secret'), 'w') as secret_file:
        secret_file.write(secret)


def get_ocsci_conf():
    cluster_user = env['CLUSTER_USER']
    pipeline_id = env['BUILD_ID']
    conf_obj = dict(
        RUN=dict(
            log_dir=os.path.join(env['WORKSPACE'], 'logs'),
        ),
        ENV_DATA=dict(
            platform='AWS',
            cluster_name=f"{cluster_user}-ocs-ci-{pipeline_id}",
            region=env['AWS_REGION'],
            base_domain=env['AWS_DOMAIN'],
            worker_instance_type='m5.4xlarge',
            cluster_namespace="openshift-storage",

        ),
        REPORTING=dict(
            gather_on_deploy_failure=True,
        )
    )
    if env.get("DOWNSTREAM") == "true":
        conf_obj['REPORTING']['us_ds'] = 'DS'
    if env.get("SMTP_SERVER"):
        conf_obj['REPORTING']['email'] = dict(smtp_server=env['SMTP_SERVER'])
    conf_obj['DEPLOYMENT'] = dict(
        ocs_registry_image=env['OCS_REGISTRY_IMAGE'],
    )
    return conf_obj


def write_ocsci_conf():
    ocp_conf = get_ocsci_conf()
    ocp_conf['ENV_DATA']['skip_ocs_deployment'] = True
    ocp_conf_path = os.path.join(env['WORKSPACE'], 'ocs-ci-ocp.yaml')
    with open(ocp_conf_path, 'w') as ocp_conf_file:
        ocp_conf_file.write(yaml.safe_dump(ocp_conf))

    ocs_conf = get_ocsci_conf()
    ocs_conf['ENV_DATA']['skip_ocp_deployment'] = True
    ocs_conf_path = os.path.join(env['WORKSPACE'], 'ocs-ci-ocs.yaml')
    with open(ocs_conf_path, 'w') as ocs_conf_file:
        ocs_conf_file.write(yaml.safe_dump(ocs_conf))


def write_bugzilla_conf():
    with open('bugzilla.cfg', 'w') as bz_cfg_file:
        bz_cfg_file.write(env['BUGZILLA_CFG'])


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
