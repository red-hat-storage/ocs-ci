# Config directory

In this directory we store all the configuration for cluster and OCSCI config
files.

During the execution we are loading different config files passed by
--ocsci-conf parameter which we merge together. The last one passed config file
overwrite previous file.

Each config file can contain different sections (DEFAULTS, ENV_DATA, RUN, etc).

For more information please read the rest of the documentation.

## OCS CI Config

We moved most of the OCSCI framework related config under
[ocsci folder](https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocsci/).

You can pass those config files by `--ocsci-conf` parameter.

## Custom config

If you would like to overwrite cluster default data you can create file
similar to
[this example](https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocs_basic_install.yml).
Example shown overwrites below ENV data:

* `platform` - Platform the cluster was created in or will be created in
* `worker_replicas` - Number of replicas of worker nodes
* `master_replicas` - Number of replicas of master nodes

### Sections in our configs

All of the below sections, will be available from the ocsci config dataclass.

#### RUN

Framework RUN related config parameters. If the parameter is for the complete
run it belongs here.

* `username` - Kubeadmin username
* `password_location` - Filepath (under the cluster path) where the kubeadmin password is located
* `log_dir` - Directory where logs are placed
* `logs_url` - URL where the logs will be available for remote access, used for Jenkins runs and configured by Jenkins
* `run_id` - Timestamp ID that is used for log directory naming
* `kubeconfig_location` - Filepath (under the cluster path) where the kubeconfig is located
* `cli_params` - Dict that holds onto all CLI parameters
* `client_version` - OCP client version
* `bin_dir` - Directory where binaries are downloaded to
* `google_api_secret` - Filepath to google api secret json file
* `force_chrome_branch_base` - Chrome base branch for openshift console UI testing
* `force_chrome_branch_sha256sum` - Chrome branch sha256sum for openshift console UI testing
* `chrome_binary_path` - Filepath to the chrome browser binary
* `io_in_bg` - Run IO in background (Default: false)
* `io_load` - Target percentage for IO in background
* `log_utilization` - Enable logging of cluster utilization metrics every 10 seconds. Set via --log-cluster-utilization
* `use_ocs_worker_for_scale` - Use OCS workers for scale testing (Default: false)
* `load_status` - Current status of IO load

#### DEPLOYMENT

Deployment related parameters. Only deployment related params not used
anywhere else.

* `installer_version` - OCP installer version
* `force_download_installer` - Download the OCP installer even if one already exists in the bin_dir
* `force_download_client` - Download the OCP client even if one already exists in the bin_dir
* `default_latest_tag` - OCS latest tag to be used by default if one is not provided
* `external_mode` - If OCS cluster is setup in external mode (Default: false)
* `ocs_csv_channel` - Channel used to install OCS CSV
* `default_ocs_registry_image` - Default OCS registry image (e.g. "quay.io/rhceph-dev/ocs-olm-operator:latest-4.6")
* `ocs_operator_nodes_to_label` - Number of OCS operator nodes to label
* `ocs_operator_nodes_to_taint` - Number of OCS operator nodes to taint
* `ssh_key` - Filepath to the public SSH key used to authenticate with OCP nodes
* `ssh_key_private` - Filepath to the private SSH key used to auth with OCP nodes
* `force_deploy_multiple_clusters` - Allow multiple clusters to be deployed with the same prefix (vmware)
* `allow_lower_instance_requirements` Allow instance requirements lower than the documented recommended values (Default: false)
* `ui_deployment` - Utilize openshift-console to deploy OCS via the UI (Default: false)
* `live_deployment` - Deploy OCS from live content (Default: false)
* `live_content_source` - Content source to use for live deployment
* `preserve_bootstrap_node` - Preserve the bootstrap node rather than deleting it after deployment (Default: false)
* `terraform_version` - Version of terraform to download
* `infra_nodes` - Add infrastructure nodes to the cluster
* `openshift_install_timeout` - Time (in seconds) to wait before timing out during OCP installation
* `local_storage` - Deploy OCS with the local storage operator (Default: false)
* `disconnected` - Set if the cluster is deployed in a disconnected environment
* `mirror_registry` - Hostname of the mirror registry
* `mirror_registry_user` - Username for disconnected cluster mirror registry
* `mirror_registry_password` - Password for disconnected cluster mirror registry
* `min_noobaa_endpoints` - Sets minimum noobaa endpoints (Workaround for https://github.com/red-hat-storage/ocs-ci/issues/2861)
* `host_network` - Enable host network in the storage cluster CR and prepare rules needed in AWS for host network during OCP deployment
* `subscription_plan_approval` - 'Manual' or 'Automatic' subscription approval for OCS upgrade
* `stage_rh_osbs` - Deploy rh-osbs-operator (Default: false)
* `stage_index_image_tag` - Image tag to use for rh-osbs-operator deployment
* `type` - Type of VMWare LSO deployment
* `kms_deployment` - Deploy OCS with KMS (Default: false)
* `create_ibm_cos_secret`: If this value is set to True (by default), the COS
  secret is created. If False, it will not be created. Relevant only for IBM
  Cloud deployment.
* `ceph_dubg` - Deploy OCS with Ceph in debug log level. Available starting OCS 4.7 (Default: false)


#### REPORTING

Reporting related config. (Do not store secret data in the repository!).

* `email` - Subsection for email reporting configuration
    * `address` - Address to send results to
    * `smtp_server` - Hostname for SMTP server
* `polarion` - Subsection for polarion reporting configuration
    * `project_id` - Polarion project ID
* `us_ds` - 'DS' or 'US', specify downstream or upstream OCS deployment
* `ocp_must_gather_image` - Image used for OCP must-gather (e.g. "quay.io/openshift/origin-must-gather")
* `ocs_must_gather_image` - Image used for OCS must-gather (e.g. "quay.io/ocs-dev/origin-must-gather")
* `default_ocs_must_gather_latest_tag` - Latest tag to use by default for OCS must-gather
* `ocs_must_gather_latest_tag` - Latest tag to use for OCS must-gather
* `gather_on_deploy_failure` - Run must-gather on deployment failure or not (Default: true)
* `collect_logs_on_success_run` - Run must-gather on successful run or not (Default: false)
* `must_gather_timeout` - Time (in seconds) to wait before timing out during must-gather

#### ENV_DATA

Environment specific data. This section is meant to be overwritten by own
cluster config file, but can be overwritten also here (But cluster config has
higher priority).

* `cluster_name` - Defaults to null, is set by the --cluster-name CLI argument
* `storage_cluster_name` - OCS storage cluster name
* `storage_device_sets_name` - OCS storage device sets name
* `cluster_namespace` - Namespace where OCS pods are created
* `local_storage_namespace` - Namespace where local storage operator pods are created
* `monitoring_enabled` - For testing OCS monitoring based on Prometheus (Default: false)
* `persistent-monitoring` - Change monitoring backend to OCS (Default: true)
* `platform` - Platform the cluster was created in or will be created in
* `deployment_type` - 'ipi' or 'upi', Installer provisioned installation or user provisioned installation
* `region` - Platform region the cluster nodes are created in
* `base_domain` - Base domain used for routing
* `master_instance_type` - Instance type used for master nodes
* `worker_instance_type` - Instance type used for worker nodes
* `master_replicas` - Number of replicas of master nodes
* `worker_replicas` - Number of replicas of worker nodes
* `master_availability_zones` - List of availability zones to create master nodes in
* `worker_availability_zones` - List of availability zones to create worker nodes in
* `skip_ocp_deployment` - Skip the OCP deployment step or not (Default: false)
* `skip_ocs_deployment` - Skip the OCS deployment step or not (Default: false)
* `ocs_version` - Version of OCS that is being deployed
* `vm_template` - VMWare template to use for RHCOS images
* `fio_storageutilization_min_mbps` - Minimal write speed of FIO used in workload_fio_storageutilization
* `TF_LOG_LEVEL` - Terraform log level
* `TF_LOG_FILE` - Terraform log file
* `cluster_host_prefix` - Subnet prefix length to assign to each individual node
* `flexy_deployment` - Deploy OCP via flexy or not (Default: false)
* `flexy_template` - Template from openshift-misc repo for the flexy deployment
* `local_storage_allow_rotational_disks` - Enable rotational disk devices for LSO deployment (Default: false)
* `disk_enable_uuid` - Enable the disk UUID on VMs, this is required for VMDK
* `ignition_data_encoding` - Encoding type used for the ignition config data
* `device_size` - Size (in GB) to use for storage device sets
* `rhel_workers` - Use RHEL workers instead of RHCOS, for UPI deployments (Default: false)
* `rhcos_ami` - AMI to use for RHCOS workers, for UPI deployments
* `skip_ntp_configuration` - Skip NTP configuration during flexy deployment (Default: false)
* `encryption_at_rest` - Enable encryption at rest (OCS >= 4.6 only) (Default: false)
* `fips` - Enable FIPS (Default: false)
* `master_num_cpus` - Number of CPUs for each master node
* `worker_num_cpus` - Number of CPUs for each worker node
* `memory` - Amount of memory used for each node (vmware)
* `disk_pattern` - Specify disk pattern used when determining device paths for LSO deployment
* `number_of_storage_nodes` - Number of storage nodes
* `master_memory` - The amount of memory for each master node
* `compute_memory` - The amount of memory for each compute node
* `scale_up` - Add nodes to the cluster (vmware)
* `nodes_scaleup_count` - Number of nodes to add to the cluster
* `rhel_template` - The VMWare template to use to spin up RHEL nodes
* `rhel_worker_prefix` - RHEL worker node name prefix
* `rhel_user` - RHEL node username
* `rhel_num_cpus` - Number of CPUs for each RHEL node
* `rhel_memory` - The amount of memory RHEL nodes will have
* `mixed_cluster` - Whether or not a cluster has a mix of RHEL and RHCOS nodes (Default: false)
* `vault_deploy_mode` - The mode in which vault service is deployed (external OR internal)
* `KMS_PROVIDER` - KMS provider name
* `KMS_SERVICE_NAME` - KMS service name
* `VAULT_ADDR` - Address of vault server
* `VAULT_CACERT` - Name of the ca certificate ocp resource for vault
* `VAULT_CLIENT_CERT` - Name of the client certificate ocp resource for vault
* `VAULT_CLIENT_KEY` - Client key for vault
* `VAULT_SKIP_VERIFY` - Skip SSL check (Default: false)
* `VAULT_BACKEND_PATH` - Vault path name used in ocs cluster
* `VAULT_POLICY` - Vault policy name used in ocs cluster

#### UPGRADE

Upgrade related configuration data.

* `upgrade` - Set to true if upgrade is being executed  (Default: false)
* `upgrade_to_latest` - Upgrade to the latest OCS version (Default: true)
* `ocp_channel` - OCP channel to upgrade with
* `ocp_upgrade_path` - OCP image to upgrade with
* `ocp_arch` - Architecture type of the OCP image
* `upgrade_logging_channel` - OCP logging channel to upgrade with

#### AUTH

This section of the config is used for storing secret data that is read from a local
auth file or pulled from s3.

* `test_quay_auth` - Config variable used during unit_testing

##### ibmcloud

IBM Cloud related section under AUTH config.

* `api_key` - IBM Cloud API key for authentication. See
  [documentation](https://cloud.ibm.com/docs/openshift?topic=openshift-access_cluster#access_api_key).
* `account_id` - Account ID to be used for login to IBM Cloud
* `ibm_cos_access_key_id` - COS (Cloud Object Storage) key ID. See
  [documentation](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-uhc-hmac-credentials-main).
* `ibm_cos_secret_access_key` - COS secret key (Follow the same documentation link above!)

> All the configuration values mentioned above are required for IBM Cloud
> deployment!

#### FLEXY

Configuration specific to flexy OCP cluster deployments

* `LAUNCHER_VARS` - dict of arguments to pass to flexy
* `OPENSHIFT_SSHKEY_PATH` - Filepath to SSH key used by flexy
* `GIT_PRIVATE_OPENSHIFT_MISC_URI` - URL for the flexy-templates repository


#### EXTERNAL_MODE

Configuration specific to external Ceph cluster

* `admin_keyring`
    * `key` - Admin keyring value used for the external Ceph cluster
* `external_cluster_details` - base64 encoded data of json output from exporter script

#### UI_SELENIUM

Configuration specific to ui testing with selenium

* `browser_type` - The type of browser (chrome,firefox)
* `chrome_type` - The type of chrome browser (google-chrome,chromium,edge)
* `headless` - Browser simulation program that does not have a user interface.
* `screenshot` - A Screenshot in Selenium Webdriver is used for bug analysis.
* `ignore_ssl` - Ignore the ssl certificate

## Example of accessing config/default data

```python
from ocs_ci.framework import config
from ocs_ci.ocs import defaults

# From you code you can access those data like

# Taking data from ENV_DATA will always use right cluster_namespace passed via
# `--ocsci-conf` config file or default one defined in `default_config.yaml`.
function_that_uses_namespace(namespace=config.ENV_DATA['cluster_namespace'])

# Defaults data you can access like in this example:
print(f"Printing some default data like API version: {defaults.API_VERSION}")
```

## Priority of loading configs:

Lower number == higher priority

1) **CLI args** - sometime we can pass some variables by CLI parameters, in
    this case those arguments should overwrite everything and have the highest
    priority.
2) **ocsci config file** - ocsci related config passed by `--ocsci-conf`
    parameter.
3) **default configuration** - default values and the lowest priority. You can
    see [default config here](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/conf/default_config.yaml).
