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
* `cluster_dir_full_path` - cluster dir full path on NFS share starting with `/mnt/`
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
* `skip_reason_test_found` - In the case the cluster left unhealthy, this param is used to determine the
  test case that is likely to cause that
* `skipped_tests_ceph_health` - The number of tests that got skipped due to Ceph being unhealthy
* `number_of_tests` - The number of tests being collected for the test execution
* `skipped_on_ceph_health_ratio` - The ratio of tests skipped due to Ceph unhealthy against the
  number of tests being collected for the test execution
* `skipped_on_ceph_health_threshold` - The allowed threshold for the ratio of tests skipped due to Ceph unhealthy against the
  number of tests being collected for the test execution. The default value is set to 0.
  For acceptance suite, the value would be always overwritten to 0.

#### DEPLOYMENT

Deployment related parameters. Only deployment related params not used
anywhere else.

* `installer_version` - OCP installer version
* `custom_ocp_image` - Custom OCP image from which extract the installer and
  client and isntall OCP
* `force_download_installer` - Download the OCP installer even if one already exists in the bin_dir
* `force_download_client` - Download the OCP client even if one already exists in the bin_dir
* `skip_download_client` - Skip the openshift client download step or not (Default: false)
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
* `ui_acm_import` - Import clusters to ACM via the UI (Default: false)
* `live_deployment` - Deploy OCS from live content (Default: false)
* `live_content_source` - Content source to use for live deployment
* `preserve_bootstrap_node` - Preserve the bootstrap node rather than deleting it after deployment (Default: false)
* `terraform_version` - Version of terraform to download
* `infra_nodes` - Add infrastructure nodes to the cluster
* `openshift_install_timeout` - Time (in seconds) to wait before timing out during OCP installation
* `local_storage` - Deploy OCS with the local storage operator (aka LSO) (Default: false)
* `local_storage_storagedeviceset_count` - This option allows one to control `spec.storageDeviceSets[0].count` of LSO backed StorageCluster.
* `optional_operators_image` - If provided, it is used for LSO installation on unreleased OCP version
* `disconnected` - Set if the cluster is deployed in a disconnected environment
* `proxy` - Set if the cluster is deployed in a proxy environment
* `mirror_registry` - Hostname of the mirror registry
* `mirror_registry_user` - Username for disconnected cluster mirror registry
* `mirror_registry_password` - Password for disconnected cluster mirror registry
* `opm_index_prune_binary_image` - Required only for IBM Power Systems and IBM Z images: Operator Registry base image with the tag that matches the target OpenShift Container Platform cluster major and minor version.
  (for example: `registry.redhat.io/openshift4/ose-operator-registry:v4.9`)
  [doc](https://access.redhat.com/documentation/en-us/openshift_container_platform/4.9/html/operators/administrator-tasks#olm-pruning-index-image_olm-managing-custom-catalogs)
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
* `ignition_version` - Ignition Version is the version used in MachineConfigs.
* `dummy_zone_node_labels`: When set to `True`, ocs-ci will try to label all
  master and worker nodes based on values of `worker_availability_zones` and
  `master_availability_zones` options, but only if there are no zone labels
  already defined. Labeling happens during post OCP deployment procedures.
  If proper labeling is not possible, an exception (which will fail OCP
  deployment) is raised. The default is False.
* `rook_log_level` - If defined, it will change rook_log_level to specified value (e.g. DEBUG),
   after the subscription to the OCS.
* `use_custom_ingress_ssl_cert` - Replace the default ingress certificate by custom one. (default: `False`)
* `ingress_ssl_cert` - Path for the custom ingress ssl certificate. (default: `data/ingress-cert.crt`)
* `ingress_ssl_key` - Path for the key for custom ingress ssl certificate. (default: `data/ingress-cert.key`)
* `ingress_ssl_ca_cert` - Path for the CA certificate used for signing the ingress_ssl_cert. (default: `data/ca.crt`)
* `cert_signing_service_url` - Automatic Certification Authority signing service URL.
* `proxy_http_proxy`, `proxy_https_proxy` - proxy configuration used for installation of cluster behind proxy (vSphere deployment via Flexy)
* `disconnected_http_proxy`, `disconnected_https_proxy`, `disconnected_no_proxy` - proxy configuration used for installation of disconnect cluster (vSphere deployment via Flexy)
* `disconnected_env_skip_image_mirroring` - skip index image prune and mirroring on disconnected environment (this expects that all the required images will be mirrored outside of ocs-ci)
* `disconnected_dns_server` - DNS server accessible from disconnected cluster (should be on the same network)
* `disconnected_false_gateway` - false gateway used to make cluster effectively disconnected
* `customized_deployment_storage_class` - Customize the storage class type in the deployment.
* `ibmcloud_disable_addon` - Disable OCS addon
* `in_transit_encryption` - Enable in-transit encryption.
* `sc_encryption` - Enable StorageClass encryption.
* `skip_ocp_installer_destroy` - Skip OCP installer to destroy the cluster -
  useful for enforcing force deploy steps only.
* `sts_enabled` - Enable STS deployment functionality.
* `metallb_operator` - Enable MetalLB operator installation during OCP deployment.
* `multi_storagecluster` - Enable multi-storagecluster deployment when set to true.

#### REPORTING

Reporting related config. (Do not store secret data in the repository!).

* `email` - Subsection for email reporting configuration
    * `address` - Address to send results to
    * `smtp_server` - Hostname for SMTP server
* `polarion` - Subsection for polarion reporting configuration
    * `project_id` - Polarion project ID
* `us_ds` - 'DS' or 'US', specify downstream or upstream OCS deployment
* `ocp_must_gather_image` - Image used for OCP must-gather (e.g. "quay.io/openshift/origin-must-gather")
* `default_ocs_must_gather_image` - Default OCS must gather image used for OCS must-gather, can be overwritten by ocs_must_gather_image
* `ocs_must_gather_image` - Image used for OCS must-gather (e.g. "quay.io/ocs-dev/origin-must-gather")
* `default_ocs_must_gather_latest_tag` - Latest tag to use by default for OCS must-gather, can be ovewritten by ocs_must_gather_latest_tag
* `ocs_must_gather_latest_tag` - Latest tag to use for OCS must-gather
* `gather_on_deploy_failure` - Run must-gather on deployment failure or not (Default: true)
* `collect_logs_on_success_run` - Run must-gather on successful run or not (Default: false)
* `must_gather_timeout` - Time (in seconds) to wait before timing out during must-gather
* `post_upgrade` - If True, post-upgrade will be reported in the test suite
  name in the mail subject.
* `save_mem_report` - If True, test run memory report CSV file will be saved in `RUN["log_dir"]/stats_log_dir_<run_id>`
  directory along with <test name>.peak_rss_table, <test name>.peak_vms_table reports. The option may be enforced by
  exporting env variable: export SAVE_MEM_REPORT=true

#### ENV_DATA

Environment specific data. This section is meant to be overwritten by own
cluster config file, but can be overwritten also here (But cluster config has
higher priority).

* `cluster_name` - Defaults to null, is set by the --cluster-name CLI argument
* `storage_cluster_name` - OCS storage cluster name
* `external_storage_cluster_name` - External storagecluster name
* `storage_device_sets_name` - OCS storage device sets name
* `cluster_namespace` - Namespace where OCS pods are created
* `external_storage_cluster_namespace` - Namespace for external storageSystem incase multi-storagecluster
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
* `rhel_version` - For AWS UPI deployment over RHEL. Based on this value we
  will select one of rhelX.Y RHEL AMI mentioned below. (e.g 7.9 or 8.4)
* `rhel_version_for_ansible` - This RHEL version will be used for running
  ansible playbook for adding RHEL nodes.
* `rhelX.Y_worker_ami` - AMI to use for AWS deployment over RHEL X.Y worker nodes
  (X.Y replace with valid version e.g 7.9: rhel7.9_worker_ami).
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
* `hpcs_deploy_mode` - The mode in which hpcs service is deployed (external only)
* `KMS_PROVIDER` - KMS provider name
* `KMS_SERVICE_NAME` - KMS service name
* `VAULT_ADDR` - Address of vault server
* `VAULT_CACERT` - Name of the ca certificate ocp resource for vault
* `VAULT_CLIENT_CERT` - Name of the client certificate ocp resource for vault
* `VAULT_CLIENT_KEY` - Client key for vault
* `VAULT_SKIP_VERIFY` - Skip SSL check (Default: false)
* `VAULT_BACKEND_PATH` - Vault path name used in ocs cluster
* `VAULT_POLICY` - Vault policy name used in ocs cluster
* `IBM_KP_SERVICE_INSTANCE_ID` - ID of the HPCS service instance.
* `IBM_KP_BASE_URL` - HPCS Service's public endpoint URL.
* `IBM_KP_TOKEN_URL` - IBM endpoint for exchanging token for API key.
* `IBM_KP_SERVICE_API_KEY` - API key to access HPCS service.
* `IBM_KP_CUSTOMER_ROOT_KEY` - ID of the root key generated by customer under HPCS service.
* `huge_pages` - True if you would like to enable HUGE PAGES.
* `http_proxy`, `https_proxy`, `no_proxy` - proxy configuration used for accessing external resources
* `client_http_proxy` - proxy configuration used by client to access OCP cluster
* `ibm_flash` - Set to `true` if you are running on the system with IBM Flash storageSystem.
* `ms_env_type` - to choose managed service environment type staging or production, default set to staging
* `lvmo` - set to True if it's LVMO deployment - mainly used for reporting purpose.
* `nb_nfs_server` - NFS server used for testing noobaa db NFS mount test
* `nb_nfs_mount` - NFS mount point used specifically for testing noobaa db NFS mount test
* `custom_default_storageclass_names` - Set to true if custom storageclass names use instead of default one.
* `storageclassnames` - Under this key, custom storage class names for `cephFilesystems`, `cephObjectStores`, `cephBlockPools`, `cephNonResilientPools`, `nfs` and for `encryption` are defined.
* `submariner_source` - Source from which we take submariner build, ex: upstream, downstream
* `submariner_release_type` - Released OR Unreleased submariner build
* `enable_globalnet` - enable or disable globalnet for submariner, default: true
* `submariner_unreleased_channel` - submariner channel for unreleased downstream build
* `enable_hw_virtualization` - enable hardware virtualization for vSphere platform.
* `performance_profile` - performance profile to be used (balanced, lean, performance).
* `noobaa_external_pgsql` - Set to True if external PgSQL server for noobaa should be used.
  See AUTH and pgsql section there for additional data you need to provide via config.
* `baremetal` - sub-section related to Bare Metal platform
    * `env_name` - name of the Bare Metal environment (used mainly for identification of configuration specific for the particular environment, e.g. _dnsmasq_ or _iPXE_ configuration)
    * `bm_httpd_server` - hostname or IP of helper/provisioning node (publicly accessible)
    * `bm_path_to_upload` - used by UPI deployment - place where to upload files accessible via http
    * `bm_httpd_document_root` - Apache document root, where to place files accessible via http (usually `/var/www/html/`)
    * `bm_install_files` - used by UPI deployment - base link to the files accessible via http
    * `bm_httpd_server_user` - user name used to ssh to the helper node
    * `bm_tftp_base_dir` - TFTP root dir where are placed files for PXE boot (usually `/tftpboot/`)
    * `bm_dnsmasq_dir` - _dnsmasq_ configuration files place
    * `bm_status_check` - link to status service for BM environment (deprecated in favor of Resource Locker, but still used for one environment)
    * `bm_provisioning_network` - which network is used as provisioning (`public` or `private`)
    * `bm_httpd_provision_server` - IP or hostname of the helper/provisioning server (http server) accessible from the provisioning network
    * `servers` - definition of the servers in the BM environment (map where key is the name of the server)
        * `<server-name>`
            * `mgmt_provider` - defines how the server should be managed (`ipmitool` or `ibmcloud`)
            * `mgmt_console` - IP or link of management console of the BM server (required for `mgmt_provider == ipmitool`)
            * `mgmt_username` - login for the mgmt console (required for `mgmt_provider == ipmitool`)
            * `mgmt_password` - password for the mgmt console (required for `mgmt_provider == ipmitool`)
            * `role` - role of the server (`master`, `worker`, `bootstrap`)
            * `public_mac` - MAC address of public interface
            * `private_mac` - MAC address of private interface
            * `ip` - (deprecated in favor of `public_ip`/`private_ip`)
            * `gw` - (deprecated in favor of `public_gw`/`private_gw`)
            * `public_ip` - IP address of the public interface
            * `public_prefix_length` - Subnet prefix length for the public network
            * `public_gw` - GW for the public interface
            * `private_ip` - IP address of the private interface
            * `private_prefix_length` - Subnet prefix length for the private network
            * `private_gw` - GW for the private interface
            * `root_disk_id` - ID of the root disk
            * `root_disk_sn` - Serial number of the root disk
            * `node_network_configuration_policy_name` - The NodeNetworkConfigurationPolicy CR name
            * `node_network_configuration_policy_ip` - The ip address of NodeNetworkConfigurationPolicy CR
            * `node_network_configuration_policy_prefix_length` - The subnetmask of NodeNetworkConfigurationPolicy CR
            * `node_network_configuration_policy_destination_route` - The destination route of NodeNetworkConfigurationPolicy CR
* `hcp_version` - version of HCP client to be deployed on machine running the tests
* `metallb_version` - MetalLB operator version to install
* `install_hypershift_upstream` - Install hypershift from upstream or not (Default: false). Necessary for unreleased OCP/CNV versions
* `clusters` - section for hosted clusters
    * `<cluster name>` - name of the cluster
      * `hosted_cluster_path` - path to the cluster directory to store auth_path, credentials files or cluster related files
      * `ocp_version` - OCP version of the hosted cluster (e.g. "4.15.13")
      * `cpu_cores_per_hosted_cluster` - number of CPU cores per hosted cluster
      * `memory_per_hosted_cluster` - amount of memory per hosted cluster
      * `nodepool_replicas` - number of replicas of nodepool for each cluster
      * `hosted_odf_registry` - registry for hosted ODF
      * `hosted_odf_version` - version of ODF to be deployed on hosted clusters
      * `cp_availability_policy` - "HighlyAvailable" or "SingleReplica"; if not provided the default value is "SingleReplica"
* `wait_timeout_for_healthy_osd_in_minutes` - timeout waiting for healthy OSDs before continuing upgrade (see https://bugzilla.redhat.com/show_bug.cgi?id=2276694 for more details)
* `odf_provider_mode_deployment` - True if you would like to enable provider mode deployment.
* `client_subcription_image` - ODF subscription image details for the storageclients.
* `channel_to_client_subscription` - Channel value for the odf subscription image for storageclients.
* `custom_vpc` - Applicable only for IMB Cloud IPI deployment where we want to create custom VPC and networking
  with specific Address prefixes to prevent /18 CIDR to be used.
* `ip_prefix` - Applicable only for IMB Cloud IPI deployment when custom_vpc, if not specified: 27 prefix will be used.
* `ceph_threshold_backfill_full_ratio` - Configure backfillFullRatio the ceph osd full thresholds value in the StorageCluster CR.
* `ceph_threshold_full_ratio` - Configure fullRatio the ceph osd full thresholds value in the StorageCluster CR.
* `ceph_threshold_near_full_ratio` - Configure nearFullRatio the ceph osd full thresholds value in the StorageCluster CR.

#### UPGRADE

Upgrade related configuration data.

* `upgrade` - Set to true if upgrade is being executed  (Default: false)
* `upgrade_to_latest` - Upgrade to the latest OCS version (Default: true)
* `ocp_channel` - OCP channel to upgrade with
* `ocp_upgrade_path` - OCP image to upgrade with
* `ocp_arch` - Architecture type of the OCP image
* `upgrade_logging_channel` - OCP logging channel to upgrade with
* `upgrade_ui` - Perform upgrade via UI (Not all the versions are supported, please look at the code)

#### AUTH

This section of the config is used for storing secret data that is read from a local
auth file or pulled from s3.

* `test_quay_auth` - Config variable used during unit_testing
* `pgsql` - Section for PostgreSQL section
  * `host` - IP or hostname of PgSQL server
  * `username` - username for database
  * `password` - password of database user
  * `port` - port where PgSQL server listen to

#### MULTICLUSTER

This section of the config is used for multicluster specific configuration data.
Scenarios that use this data include MDR and RDR deployments.

* `multicluster_index` - Index of the cluster, used to differentiate between other cluster configurations.
* `acm_cluster` - True if the cluster is an ACM hub cluster, otherwise False.
* `primary_cluster` - True if the cluster is the primary cluster, otherwise False.
* `active_acm_cluster` - True if the cluster is the active ACM hub cluster, False if passive.

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
* `rgw_secure` - boolean parameter which defines if external Ceph cluster RGW is secured using SSL
* `rgw_cert_ca` - url pointing to CA certificate used to sign certificate for RGW with SSL

##### login

Login section under EXTERNAL_MODE with auth details for SSH to the host of RHCS
Cluster.

* `username` - user to be used for SSH access to the node
* `password` - password for the ssh user (optional if ssh_key provided)
* `ssh_key` - path to SSH private key (optional if password is provided)

#### UI_SELENIUM

Configuration specific to ui testing with selenium

* `browser_type` - The type of browser (chrome,firefox)
* `chrome_type` - The type of chrome browser (google-chrome,chromium,edge)
* `headless` - Browser simulation program that does not have a user interface.
* `screenshot` - A Screenshot in Selenium Webdriver is used for bug analysis.
* `ignore_ssl` - Ignore the ssl certificate

#### COMPONENTS

Configurations specific to disable/enable OCS components

* `disable_rgw` - Disable RGW component deployment (Default: False)
* `disable_noobaa` - Disable noobaa component deployment (Default: False)
* `disable_cephfs` - Disable cephfs component deployment (Default: False)
* `disable_blockpools` - Disable blockpools (rbd) component deployment (Default: False)

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
