from selenium.webdriver.common.by import By

osd_sizes = ("512", "2048", "4096")

OCS_OPERATOR = "OpenShift Container Storage"
ODF_OPERATOR = "OpenShift Data Foundation"

login = {
    "ocp_page": "Overview · Red Hat OpenShift Container Platform",
    "username": ("inputUsername", By.ID),
    "password": ("inputPassword", By.ID),
    "click_login": ("//button[text()='Log in']", By.XPATH),
    "kubeadmin_login_approval": ('a[title="Log in with kube:admin"]', By.CSS_SELECTOR),
}

deployment = {
    "click_install_ocs": ('a[data-test-id="operator-install-btn"]', By.CSS_SELECTOR),
    "choose_ocs_version": (
        'a[data-test="ocs-operator-redhat-operators-openshift-marketplace"]',
        By.CSS_SELECTOR,
    ),
    "search_operators": ('input[placeholder="Filter by keyword..."]', By.CSS_SELECTOR),
    "operators_tab": ("//button[text()='Operators']", By.XPATH),
    "operatorhub_tab": ("OperatorHub", By.LINK_TEXT),
    "installed_operators_tab": ("Installed Operators", By.LINK_TEXT),
    "storage_cluster_tab": (
        'a[data-test-id="horizontal-link-Storage Cluster"]',
        By.CSS_SELECTOR,
    ),
    "ocs_operator_installed": (
        'a[data-test-operator-row="OpenShift Container Storage"]',
        By.CSS_SELECTOR,
    ),
    "search_operator_installed": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
    "thin_sc": ('a[id="thin-link"]', By.CSS_SELECTOR),
    "gp2_sc": ('a[id="gp2-link"]', By.CSS_SELECTOR),
    "managed-premium_sc": ('a[id="managed-premium-link"]', By.CSS_SELECTOR),
    "osd_size_dropdown": ('button[data-test-id="dropdown-button"]', By.CSS_SELECTOR),
    "512": ('button[data-test-dropdown-menu="512Gi"]', By.CSS_SELECTOR),
    "2048": ('button[data-test-dropdown-menu="2Ti"]', By.CSS_SELECTOR),
    "4096": ('button[data-test-dropdown-menu="4Ti"]', By.CSS_SELECTOR),
    "all_nodes": ('input[aria-label="Select all rows"]', By.CSS_SELECTOR),
    "wide_encryption": ('//*[@id="cluster-wide-encryption"]', By.XPATH),
    "class_encryption": ('//*[@id="storage-class-encryption"]', By.XPATH),
    "advanced_encryption": ('//*[@id="advanced-encryption"]', By.XPATH),
    "kms_service_name": ('//*[@id="kms-service-name"]', By.XPATH),
    "kms_address": ('//*[@id="kms-address"]', By.XPATH),
    "kms_address_port": ('//*[@id="kms-address-port"]', By.XPATH),
    "kms_token": ('//*[@id="kms-token"]', By.XPATH),
    "create_on_review": ("//button[text()='Create']", By.XPATH),
    "search_ocs_installed": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
    "all_nodes_lso": (
        'input[id="auto-detect-volume-radio-all-nodes"]',
        By.CSS_SELECTOR,
    ),
    "lv_name": ('input[id="create-lvs-volume-set-name"]', By.CSS_SELECTOR),
    "sc_name": ('input[id="create-lvs-storage-class-name"]', By.CSS_SELECTOR),
    "all_nodes_create_sc": ('input[id="create-lvs-radio-all-nodes"]', By.CSS_SELECTOR),
    "storage_class_dropdown_lso": (
        'button[id="storage-class-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "localblock_sc": ('a[id="localblock-link"]', By.CSS_SELECTOR),
    "choose_local_storage_version": (
        'a[data-test="local-storage-operator-redhat-operators-openshift-marketplace"]',
        By.CSS_SELECTOR,
    ),
    "click_install_lso": ('a[data-test-id="operator-install-btn"]', By.CSS_SELECTOR),
    "yes": ("//*[contains(text(), 'Yes')]", By.XPATH),
    "next": ("//*[contains(text(), 'Next')]", By.XPATH),
}

deployment_4_6 = {
    "click_install_ocs_page": ("//button[text()='Install']", By.XPATH),
    "create_storage_cluster": ("//button[text()='Create Storage Cluster']", By.XPATH),
    "internal_mode": ('input[value="Internal"]', By.CSS_SELECTOR),
    "internal-attached_devices": (
        'input[value="Internal - Attached Devices"]',
        By.CSS_SELECTOR,
    ),
    "storage_class_dropdown": (
        'button[id="ceph-sc-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "enable_encryption": ('//span[@class="pf-c-switch__toggle"]', By.XPATH),
    "click_install_lso_page": ("//button[text()='Install']", By.XPATH),
    "project_dropdown": (
        'button[class="pf-c-dropdown__toggle pf-m-plain"]',
        By.CSS_SELECTOR,
    ),
    "OpenShift Container Storage": ('a[id="openshift-storage-link"]', By.CSS_SELECTOR),
    "Local Storage": ('a[id="openshift-local-storage-link"]', By.CSS_SELECTOR),
}

deployment_4_7 = {
    "click_install_ocs_page": ('button[data-test="install-operator"]', By.CSS_SELECTOR),
    "create_storage_cluster": ('button[data-test="item-create"]', By.CSS_SELECTOR),
    "internal_mode": ('input[data-test="Internal-radio-input"]', By.CSS_SELECTOR),
    "internal-attached_devices": (
        'input[data-test="Internal - Attached Devices-radio-input"]',
        By.CSS_SELECTOR,
    ),
    "storage_class_dropdown": (
        'button[data-test="storage-class-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "enable_encryption": ('input[data-test="encryption-checkbox"]', By.CSS_SELECTOR),
    "click_install_lso_page": ('button[data-test="install-operator"]', By.CSS_SELECTOR),
}

deployment_4_9 = {
    "drop_down_projects": (
        'button[class="pf-c-menu-toggle co-namespace-dropdown__menu-toggle"]',
        By.CSS_SELECTOR,
    ),
    "enable_default_porjects": ('span[class="pf-c-switch__toggle"]', By.CSS_SELECTOR),
    "choose_openshift-storage_project": (
        "//span[text()='openshift-storage']",
        By.XPATH,
    ),
    "choose_all_projects": ("//span[text()='All Projects']", By.XPATH),
    "click_odf_operator": (
        'a[data-test="odf-operator-redhat-operators-openshift-marketplace"]',
        By.CSS_SELECTOR,
    ),
    "enable_console_plugin": ('input[data-test="Enable-radio-input"]', By.CSS_SELECTOR),
    "odf_operator_installed": (
        'a[data-test-operator-row="OpenShift Data Foundation"]',
        By.CSS_SELECTOR,
    ),
    "storage_system_tab": (
        'a[data-test-id="horizontal-link-Storage System"]',
        By.CSS_SELECTOR,
    ),
    "internal_mode_odf": ('input[id="bs-existing"]', By.CSS_SELECTOR),
    "create_storage_system": ("//button[text()='Create StorageSystem']", By.XPATH),
    "choose_lso_deployment": ('input[id="bs-local-devices"]', By.CSS_SELECTOR),
    "refresh_popup": ("//button[text()='Refresh web console']", By.XPATH),
    "advanced_deployment": ("//span[text()='Advanced']", By.XPATH),
    "expand_advanced_mode": ('button[class="pf-c-select__toggle"]', By.CSS_SELECTOR),
    "mcg_only_option": ("//button[text()='MultiCloud Object Gateway']", By.XPATH),
    "plugin-available": ("//*[text()='Plugin available']", By.XPATH),
}

deployment_4_10 = {
    "mcg_only_option_4_10": ("//span[text()='MultiCloud Object Gateway']", By.XPATH),
    "enable_taint_node": ('input[id="taint-nodes"]', By.CSS_SELECTOR),
    "gp2-csi_sc": ('a[id="gp2-csi-link"]', By.CSS_SELECTOR),
    "gp3-csi_sc": ('a[id="gp3-csi-link"]', By.CSS_SELECTOR),
}

generic_locators = {
    "project_selector": (
        'button[class="pf-c-dropdown__toggle pf-m-plain"]',
        By.CSS_SELECTOR,
    ),
    "select_openshift-storage_project": (
        'a[id="openshift-storage-link"]',
        By.CSS_SELECTOR,
    ),
    "create_resource_button": ("yaml-create", By.ID),
    "search_resource_field": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
    "first_dropdown_option": (
        'a[data-test="dropdown-menu-item-link"]',
        By.CSS_SELECTOR,
    ),
    "actions": ('button[data-test-id="actions-menu-button"]', By.CSS_SELECTOR),
    "confirm_action": ("confirm-action", By.ID),
    "submit_form": ('button[type="submit"]', By.CSS_SELECTOR),
    "ocs_operator": ('//h1[text()="OpenShift Container Storage"]', By.XPATH),
    "kebab_button": ('button[data-test-id="kebab-button"', By.CSS_SELECTOR),
    "resource_status": ('span[data-test="status-text"]', By.CSS_SELECTOR),
    "check_first_row_checkbox": ('input[name="checkrow0"]', By.CSS_SELECTOR),
    "remove_search_filter": ('button[aria-label="close"]', By.CSS_SELECTOR),
    "delete_resource_kebab_button": ('//*[contains(text(), "Delete")]', By.XPATH),
}

ocs_operator_locators = {
    "backingstore_page": (
        'a[data-test-id="horizontal-link-Backing Store"]',
        By.CSS_SELECTOR,
    ),
    "namespacestore_page": (
        'a[data-test-id="horizontal-link-Namespace Store"]',
        By.CSS_SELECTOR,
    ),
    "bucketclass_page": (
        'a[data-test-id="horizontal-link-Bucket Class"]',
        By.CSS_SELECTOR,
    ),
}

mcg_stores = {
    "store_name": ('input[data-test*="store-name"]', By.CSS_SELECTOR),
    "provider_dropdown": ('button[data-test*="store-provider"]', By.CSS_SELECTOR),
    "aws_provider": ("AWS S3-link", By.ID),
    "aws_region_dropdown": ("region", By.ID),
    "us_east_2_region": ("us-east-2-link", By.ID),
    "aws_secret_dropdown": ("secret-dropdown", By.ID),
    "aws_secret_search_field": (
        'input[data-test-id="dropdown-text-filter"]',
        By.CSS_SELECTOR,
    ),
    "target_bucket": ("target-bucket", By.ID),
}

bucketclass = {
    "standard_type": ("Standard", By.ID),
    "namespace_type": ("Namespace", By.ID),
    "bucketclass_name": ("bucketclassname-input", By.ID),
    "spread_policy": ('input[data-test="placement-policy-spread1"]', By.CSS_SELECTOR),
    "mirror_policy": ('input[data-test="placement-policy-mirror1"]', By.CSS_SELECTOR),
    "single_policy": ("Single", By.ID),
    "multi_policy": ("Multi", By.ID),
    "cache_policy": ("Cache", By.ID),
    "nss_dropdown": ('button[data-test="nns-dropdown-toggle"]', By.CSS_SELECTOR),
    "nss_option_template": ('button[data-test="{}"]', By.CSS_SELECTOR),
    "bs_dropdown": ('button[data-test="nbs-dropdown-toggle"]', By.CSS_SELECTOR),
    "first_bs_dropdown_option": (
        'button[data-test="mybs-dropdown-item"]',
        By.CSS_SELECTOR,
    ),
    "ttl_input": ("ttl-input", By.ID),
    "ttl_time_unit_dropdown": ("timetolive-input", By.ID),
    "ttl_minute_time_unit_button": ("MIN-link", By.ID),
}

obc = {
    "storageclass_dropdown": ("sc-dropdown", By.ID),
    "storageclass_text_field": (
        'input[placeholder="Select StorageClass"]',
        By.CSS_SELECTOR,
    ),
    "bucketclass_dropdown": ("bc-dropdown", By.ID),
    "bucketclass_text_field": (
        'input[placeholder="Select BucketClass"]',
        By.CSS_SELECTOR,
    ),
    "default_bucketclass": ("noobaa-default-bucket-class-link", By.ID),
    "obc_name": ("obc-name", By.ID),
    "first_obc_link": ('a[class="co-resource-item__resource-name"]', By.CSS_SELECTOR),
    "delete_obc": (
        'button[data-test-action="Delete Object Bucket Claim"]',
        By.CSS_SELECTOR,
    ),
}

pvc = {
    "pvc_project_selector": (
        'button[class="pf-c-dropdown__toggle pf-m-plain"]',
        By.CSS_SELECTOR,
    ),
    "select_openshift-storage_project": (
        'a[id="openshift-storage-link"]',
        By.CSS_SELECTOR,
    ),
    "pvc_create_button": ('button[data-test="item-create"]', By.CSS_SELECTOR),
    "pvc_storage_class_selector": (
        'button[data-test="storageclass-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "storage_class_name": ('//*[text()="{}"]', By.XPATH),
    "ocs-storagecluster-ceph-rbd": (
        'a[id="ocs-storagecluster-ceph-rbd-link"]',
        By.CSS_SELECTOR,
    ),
    "ocs-storagecluster-cephfs": (
        'a[id="ocs-storagecluster-cephfs-link"]',
        By.CSS_SELECTOR,
    ),
    "ocs-storagecluster-ceph-rbd-thick": (
        "a[id='ocs-storagecluster-ceph-rbd-thick-link'] div[class='text-muted small']",
        By.CSS_SELECTOR,
    ),
    "pvc_name": ('input[data-test="pvc-name"]', By.CSS_SELECTOR),
    "ReadWriteOnce": (
        'input[data-test="Single User (RWO)-radio-input"]',
        By.CSS_SELECTOR,
    ),
    "ReadWriteMany": (
        'input[data-test="Shared Access (RWX)-radio-input"]',
        By.CSS_SELECTOR,
    ),
    "ReadOnlyMany": ('input[data-test="Read Only (ROX)-radio-input"]', By.CSS_SELECTOR),
    "pvc_size": ('input[data-test="pvc-size"]', By.CSS_SELECTOR),
    "pvc_create": ('button[data-test="create-pvc"]', By.CSS_SELECTOR),
    "pvc_actions": ('button[data-test-id="actions-menu-button"]', By.CSS_SELECTOR),
    "pvc_delete": (
        'button[data-test-action="Delete PersistentVolumeClaim"]',
        By.CSS_SELECTOR,
    ),
    "confirm_pvc_deletion": ('button[data-test="confirm-action"]', By.CSS_SELECTOR),
    "search_pvc": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
    "clone_pvc": ("button[data-test-action='Clone PVC']", By.CSS_SELECTOR),
    "clone_name_input": ("//input[@aria-label='Clone PVC']", By.XPATH),
}

pvc_4_7 = {
    "test-pvc-fs": ('a[data-test-id="test-pvc-fs"]', By.CSS_SELECTOR),
    "test-pvc-rbd": ("a[title='test-pvc-rbd']", By.CSS_SELECTOR),
    "Block": ("input[value='Block']", By.CSS_SELECTOR),
    "Filesystem": ("input[value='Filesystem']", By.CSS_SELECTOR),
    "search-project": ("input[placeholder='Select Project...']", By.CSS_SELECTOR),
    "expand_pvc": ("button[data-test-action='Expand PVC']", By.CSS_SELECTOR),
    "resize-value": ("//input[@name='requestSizeValue']", By.XPATH),
    "expand-btn": ("#confirm-action", By.CSS_SELECTOR),
    "pvc-status": (
        "dd[data-test-id='pvc-status'] span[data-test='status-text']",
        By.CSS_SELECTOR,
    ),
    "test-project-link": ("//a[normalize-space()='{}']", By.XPATH),
    "expected-capacity": (
        "//dd[contains(text(),'{}') and @data-test='pvc-requested-capacity']",
        By.XPATH,
    ),
    "new-capacity": (
        "//dd[contains(text(),'{}') and @data-test-id='pvc-capacity']",
        By.XPATH,
    ),
}

pvc_4_8 = {
    "ReadWriteMany": ("input[value='ReadWriteMany']", By.CSS_SELECTOR),
    "pvc_actions": ("button[aria-label='Actions']", By.CSS_SELECTOR),
    "ReadWriteOnce": ("input[value='ReadWriteOnce']", By.CSS_SELECTOR),
    "test-pvc-fs": ("a[title='test-pvc-fs']", By.CSS_SELECTOR),
    "test-pvc-rbd-thick": ("a[title='test-pvc-rbd-thick']", By.CSS_SELECTOR),
    "resize-pending": (
        "div[class ='col-xs-4 col-sm-2 col-md-2'] span",
        By.CSS_SELECTOR,
    ),
    "search_pvc": ("input[placeholder='Search by name...']", By.CSS_SELECTOR),
}

pvc_4_9 = {
    "pvc_project_selector": (".pf-c-menu-toggle__text", By.CSS_SELECTOR),
    "test-project-link": ("//span[contains(text(),'{}')]", By.XPATH),
    "search-project": ("input[placeholder='Select project...']", By.CSS_SELECTOR),
}

page_nav = {
    "Home": ("//button[text()='Home']", By.XPATH),
    "overview_page": ("Overview", By.LINK_TEXT),
    "projects_page": ("Projects", By.LINK_TEXT),
    "search_page": ("Search", By.LINK_TEXT),
    "explore_page": ("Explore", By.LINK_TEXT),
    "events_page": ("Events", By.LINK_TEXT),
    "Operators": ("//button[text()='Operators']", By.XPATH),
    "operatorhub_page": ("OperatorHub", By.LINK_TEXT),
    "installed_operators_page": ("Installed Operators", By.LINK_TEXT),
    "Storage": ("//button[text()='Storage']", By.XPATH),
    "persistentvolumes_page": ("PersistentVolumes", By.LINK_TEXT),
    "persistentvolumeclaims_page": ("PersistentVolumeClaims", By.LINK_TEXT),
    "storageclasses_page": ("StorageClasses", By.LINK_TEXT),
    "volumesnapshots_page": ("VolumeSnapshots", By.LINK_TEXT),
    "volumesnapshotclasses_page": ("VolumeSnapshotClasses", By.LINK_TEXT),
    "volumesnapshotcontents_page": ("VolumeSnapshotContents", By.LINK_TEXT),
    "object_buckets_page": ("Object Buckets", By.LINK_TEXT),
    "object_bucket_claims_page": ("Object Bucket Claims", By.LINK_TEXT),
    "Monitoring": ("//button[text()='Monitoring']", By.XPATH),
    "alerting_page": ("Alerting", By.LINK_TEXT),
    "metrics_page": ("Metrics", By.LINK_TEXT),
    "dashboards_page": ("Dashboards", By.LINK_TEXT),
    "Workloads": ("//button[text()='Workloads']", By.XPATH),
    "Pods": ("Pods", By.LINK_TEXT),
    "quickstarts": ('a[href="/quickstart"]', By.CSS_SELECTOR),
    "block_pool_link": (
        'a[data-test-id="horizontal-link-Block Pools"]',
        By.CSS_SELECTOR,
    ),
    "odf_tab": ("OpenShift Data Foundation", By.LINK_TEXT),
    "drop_down_projects": (
        'button[class="pf-c-menu-toggle co-namespace-dropdown__menu-toggle"]',
        By.CSS_SELECTOR,
    ),
    "choose_all_projects": ("//span[text()='All Projects']", By.XPATH),
}

acm_page_nav = {
    "Home": ("//button[text()='Home']", By.XPATH),
    "Welcome_page": ("Welcome", By.LINK_TEXT),
    "Overview_page": ("Overview", By.LINK_TEXT),
    "Infrastructure": ("//button[normalize-space()='Infrastructure']", By.XPATH),
    "Clusters_page": ("Clusters", By.LINK_TEXT),
    "Bare_metal_assets_page": ("Bare metal assets", By.LINK_TEXT),
    "Automation_page": ("Automation", By.LINK_TEXT),
    "Infrastructure_environments_page": ("Infrastructure environments", By.LINK_TEXT),
    "Applications": ("Applications", By.LINK_TEXT),
    "Governance": ("Governance", By.LINK_TEXT),
    "Credentials": ("Credentials", By.LINK_TEXT),
    "Import_cluster": ("importCluster", By.ID),
    "Import_cluster_enter_name": ("clusterName", By.ID),
    "Import_mode": ('button[class="pf-c-select__toggle"]', By.CSS_SELECTOR),
    "choose_kubeconfig": ("//button[text()='Kubeconfig']", By.XPATH),
    "Kubeconfig_text": ("kubeConfigEntry", By.ID),
    "Submit_import": ("//button[text()='Import']", By.XPATH),
}

acm_configuration = {
    "cluster-sets": ("//a[normalize-space()='Cluster sets']", By.XPATH),
    "create-cluster-set": (".pf-c-button.pf-m-primary", By.CSS_SELECTOR),
    "cluster-set-name": (
        "input[placeholder='Enter cluster set name']",
        By.CSS_SELECTOR,
    ),
    "click-create": ("button[type='submit']", By.CSS_SELECTOR),
    "click-manage-resource-assignments": (
        "//button[normalize-space()='Manage resource assignments']",
        By.XPATH,
    ),
    "select-all-assignments": ("input[aria-label='Select all']", By.CSS_SELECTOR),
    "click-local-cluster": (
        "//*[@data-ouia-component-type='PF4/TableRow']//td[2]//*[text()='local-cluster']",
        By.XPATH,
    ),
    "search-cluster": ("//input[@placeholder='Search']", By.XPATH),
    "select-first-checkbox": ("input[name='checkrow0']", By.CSS_SELECTOR),
    "clear-search": ("//*[name()='path' and contains(@d,'M242.72 25')]", By.XPATH),
    "review-btn": (".pf-c-button.pf-m-primary", By.CSS_SELECTOR),
    "confirm-btn": ("button[type='submit']", By.CSS_SELECTOR),
    "cluster-set-status": ("//span[@class='pf-c-modal-box__title-text']", By.XPATH),
    "submariner-tab": ("//a[normalize-space()='Submariner add-ons']", By.XPATH),
    "install-submariner-btn": (
        "//button[normalize-space()='Install Submariner add-ons']",
        By.XPATH,
    ),
    "target-clusters": ("input[placeholder='Select clusters']", By.CSS_SELECTOR),
    "cluster-name-selection": ("//button[normalize-space()='{}']", By.XPATH),
    "next-btn": (".pf-c-button.pf-m-primary", By.CSS_SELECTOR),
    "nat-t-checkbox": ("input[type='checkbox']", By.CSS_SELECTOR),
    "gateway-count-btn": ("//button[@aria-label='Plus']", By.XPATH),
    "install-btn": (".pf-c-button.pf-m-primary.pf-m-progress", By.CSS_SELECTOR),
    "connection-status-1": (
        "(//button[@type='button'][normalize-space()='Healthy'])[1]",
        By.XPATH,
    ),
    "connection-status-2": (
        "(//button[@type='button'][normalize-space()='Healthy'])[3]",
        By.XPATH,
    ),
    "agent-status-1": (
        "(//button[@type='button'][normalize-space()='Healthy'])[2]",
        By.XPATH,
    ),
    "agent-status-2": (
        "(//button[@type='button'][normalize-space()='Healthy'])[4]",
        By.XPATH,
    ),
    "node-label-1": (
        "(//button[@type='button'][normalize-space()='Nodes labeled'])[1]",
        By.XPATH,
    ),
    "node-label-2": (
        "(//button[@type='button'][normalize-space()='Nodes labeled'])[2]",
        By.XPATH,
    ),
    "cluster-set-selection": ("//a[normalize-space()='{}']", By.XPATH),
    "cc_create_cluster": ("createCluster", By.ID),
    "cc_provider_vmware_vsphere": ("//*[@id='vmware-vsphere']", By.XPATH),
    "cc_cluster_name": ("//input[@id='eman']", By.XPATH),
    "cc_base_dns_domain": ("//input[@id='baseDomain']", By.XPATH),
    "cc_openshift_release_image": ("//input[@id='imageSet']", By.XPATH),
    "cc_vsphere_network_name": ("//input[@id='networkType']", By.XPATH),
    "cc_api_vip": ("//input[@id='apiVIP']", By.XPATH),
    "cc_ingress_vip": ("//input[@id='ingressVIP']", By.XPATH),
    "cc_next_page_button": ("//button[normalize-space()='Next']", By.XPATH),
    "cc_create_button": ("//button[normalize-space()='Create']", By.XPATH),
    "cc_cluster_details": ("//div[contains(text(),'Cluster details')]", By.XPATH),
    "cc_node_pools": ("//div[contains(text(),'Node pools')]", By.XPATH),
    "cc_networks": ("//div[contains(text(),'Networks')]", By.XPATH),
    "cc_proxy": ("//div[contains(text(),'Proxy')]", By.XPATH),
    "cc_review": ("//button[normalize-space()='Review']", By.XPATH),
    "cc_infrastructure_provider_creds_dropdown": (
        "input[placeholder='Select a credential']",
        By.CSS_SELECTOR,
    ),
    "cc_infrastructure_provider_creds_select_creds": (
        "//button[normalize-space()='{}']",
        By.XPATH,
    ),
    "cc_provider_credentials": ("//div[@id='add-provider-connection']", By.XPATH),
    "cc_provider_creds_vsphere": (
        "//div[@id='vmw']//div[@class='pf-c-tile__header pf-m-stacked']",
        By.XPATH,
    ),
    "cc_provider_creds_vsphere_cred_name": ("//input[@id='credentialsName']", By.XPATH),
    "cc_provider_creds_vsphere_cred_namespace": (
        "//input[@id='namespaceName-input-toggle-select-typeahead']",
        By.XPATH,
    ),
    "cc_provider_creds_vsphere_base_dns": ("//input[@id='baseDomain']", By.XPATH),
    "cc_provider_creds_vsphere_vcenter_server": ("//input[@id='vCenter']", By.XPATH),
    "cc_provider_creds_vsphere_username": ("//input[@id='username']", By.XPATH),
    "cc_provider_creds_vsphere_password": ("//input[@id='password']", By.XPATH),
    "cc_provider_creds_vsphere_rootca": ("//textarea[@id='cacertificate']", By.XPATH),
    "cc_provider_creds_vsphere_clustername": ("//input[@id='cluster']", By.XPATH),
    "cc_provider_creds_vsphere_dc": ("//input[@id='datacenter']", By.XPATH),
    "cc_provider_creds_vsphere_datastore": (
        "//input[@id='defaultDatastore']",
        By.XPATH,
    ),
    "cc_provider_creds_vsphere_pullsecret": ("//textarea[@id='pullSecret']", By.XPATH),
    "cc_provider_creds_vsphere_ssh_privkey": (
        "//textarea[@id='ssh-privatekey']",
        By.XPATH,
    ),
    "cc_provider_creds_vsphere_ssh_pubkey": (
        "//textarea[@id='ssh-publickey']",
        By.XPATH,
    ),
    "cc_provider_creds_vsphere_add_button": (
        "//button[normalize-space()='Add']",
        By.XPATH,
    ),
    "cc_cluster_status_page_download_config": (
        "//button[@id='download-configuration']",
        By.XPATH,
    ),
    "cc_cluster_status_page_download_install_config": (
        "//a[normalize-space()='install-config']",
        By.XPATH,
    ),
    "cc_cluster_status_page_status_failed": (
        "//button[normalize-space()='Failed']",
        By.XPATH,
    ),
}

add_capacity = {
    "ocs_operator": (
        'a[data-test-operator-row="OpenShift Container Storage"]',
        By.CSS_SELECTOR,
    ),
    "odf_operator": (
        'a[data-test-operator-row="OpenShift Data Foundation"]',
        By.CSS_SELECTOR,
    ),
    "storage_cluster_tab": (
        'a[data-test-id="horizontal-link-Storage Cluster"]',
        By.CSS_SELECTOR,
    ),
    "storage_system_tab": (
        'a[data-test-id="horizontal-link-Storage System"]',
        By.CSS_SELECTOR,
    ),
    "kebab_storage_cluster": ('button[data-test-id="kebab-button"', By.CSS_SELECTOR),
    "add_capacity_button": ('button[data-test-action="Add Capacity"]', By.CSS_SELECTOR),
    "select_sc_add_capacity": (
        'button[data-test="add-cap-sc-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "thin_sc": ('a[id="thin-link"]', By.CSS_SELECTOR),
    "gp2_sc": ('a[id="gp2-link"]', By.CSS_SELECTOR),
    "gp2-csi_sc": ('a[id="gp2-csi-link"]', By.CSS_SELECTOR),
    "gp3-csi_sc": ('a[id="gp3-csi-link"]', By.CSS_SELECTOR),
    "localblock_sc": ('a[id="localblock-link"]', By.CSS_SELECTOR),
    "managed-premium_sc": ('a[id="managed-premium-link"]', By.CSS_SELECTOR),
    "confirm_add_capacity": ('button[data-test="confirm-action"', By.CSS_SELECTOR),
    "filter_pods": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
}

block_pool = {
    "create_block_pool": ("Create BlockPool", By.LINK_TEXT),
    "new_pool_name": (
        'input[data-test="new-pool-name-textbox"]',
        By.CSS_SELECTOR,
    ),
    "first_select_replica": ('button[data-test="replica-dropdown"]', By.CSS_SELECTOR),
    "second_select_replica_2": ("//button[text()='2-way Replication']", By.XPATH),
    "second_select_replica_3": ("//button[text()='3-way Replication']", By.XPATH),
    "conpression_checkbox": (
        'input[data-test="compression-checkbox"]',
        By.CSS_SELECTOR,
    ),
    "pool_confirm_create": ('button[data-test-id="confirm-action"]', By.CSS_SELECTOR),
    "actions_inside_pool": ('button[aria-label="Actions"]', By.CSS_SELECTOR),
    "edit_pool_inside_pool": (
        'button[data-test-action="Edit BlockPool"]',
        By.CSS_SELECTOR,
    ),
    "delete_pool_inside_pool": (
        'button[data-test-action="Delete BlockPool"]',
        By.CSS_SELECTOR,
    ),
    "confirm_delete_inside_pool": ("//button[text()='Delete']", By.XPATH),
    "replica_dropdown_edit": ('button[data-test="replica-dropdown"]', By.CSS_SELECTOR),
    "compression_checkbox_edit": (
        'input[data-test="compression-checkbox"]',
        By.CSS_SELECTOR,
    ),
    "save_pool_edit": ('button[data-test-id="confirm-action"]', By.CSS_SELECTOR),
    "pool_state_inside_pool": ('span[data-test="status-text"]', By.CSS_SELECTOR),
}

storageclass = {
    "create_storageclass_button": ("Create StorageClass", By.LINK_TEXT),
    "input_storageclass_name": ('input[id="storage-class-name"]', By.CSS_SELECTOR),
    "provisioner_dropdown": (
        'button[data-test="storage-class-provisioner-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "rbd_provisioner": ("openshift-storage.rbd.csi.ceph.com", By.LINK_TEXT),
    "pool_dropdown": ('button[id="pool-dropdown-id"]', By.CSS_SELECTOR),
    "save_storageclass": ('button[id="save-changes"]', By.CSS_SELECTOR),
    "action_inside_storageclass": (
        'button[data-test-id="actions-menu-button"]',
        By.CSS_SELECTOR,
    ),
    "delete_inside_storageclass": (
        'button[data-test-action="Delete StorageClass"]',
        By.CSS_SELECTOR,
    ),
    "confirm_delete_inside_storageclass": ("//button[text()='Delete']", By.XPATH),
}

validation = {
    "object_service_button": ("//button[text()='Object Service']", By.XPATH),
    "data_resiliency_button": ("//button[text()='Data Resiliency']", By.XPATH),
    "search_ocs_installed": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
    "ocs_operator_installed": (
        'a[data-test-operator-row="OpenShift Container Storage"]',
        By.CSS_SELECTOR,
    ),
    "osc_subscription_tab": (
        'a[data-test-id="horizontal-link-olm~Subscription"]',
        By.CSS_SELECTOR,
    ),
    "osc_all_instances_tab": (
        'a[data-test-id="horizontal-link-olm~All instances"]',
        By.CSS_SELECTOR,
    ),
    "osc_storage_cluster_tab": (
        'a[data-test-id="horizontal-link-Storage Cluster"]',
        By.CSS_SELECTOR,
    ),
    "osc_backing_store_tab": (
        'a[data-test-id="horizontal-link-Backing Store"]',
        By.CSS_SELECTOR,
    ),
    "osc_bucket_class_tab": (
        'a[data-test-id="horizontal-link-Bucket Class"]',
        By.CSS_SELECTOR,
    ),
    "capacity_breakdown_options": (
        'button[class="pf-c-select__toggle"]',
        By.CSS_SELECTOR,
    ),
    "capacity_breakdown_projects": ("//button[text()='Projects']", By.XPATH),
    "capacity_breakdown_pods": ("//button[text()='Pods']", By.XPATH),
}

validation_4_7 = {
    "object_service_tab": (
        'a[data-test-id="horizontal-link-Object Service"]',
        By.CSS_SELECTOR,
    ),
    "persistent_storage_tab": (
        'a[data-test-id="horizontal-link-Persistent Storage"]',
        By.CSS_SELECTOR,
    ),
}

validation_4_8 = {
    "object_service_tab": (
        'a[data-test-id="horizontal-link-Object"]',
        By.CSS_SELECTOR,
    ),
    "persistent_storage_tab": (
        'a[data-test-id="horizontal-link-Block and File"]',
        By.CSS_SELECTOR,
    ),
}

validation_4_9 = {
    "storage_systems": (
        "a[data-test-id='horizontal-link-Storage Systems']",
        By.CSS_SELECTOR,
    ),
    "ocs-storagecluster-storagesystem-status": (
        "//*[text()= 'Ready']",
        By.XPATH,
    ),
    "ocs-storagecluster-storagesystem": (
        "a[href='/odf/system/ocs.openshift.io~v1~storagecluster/ocs-storagecluster/overview']",
        By.CSS_SELECTOR,
    ),
    "overview": (
        "a[data-test-id='horizontal-link-Overview']",
        By.CSS_SELECTOR,
    ),
    "blockandfile": (
        "a[data-test-id='horizontal-link-Block and File']",
        By.CSS_SELECTOR,
    ),
    "object": ("a[data-test-id='horizontal-link-Object']", By.CSS_SELECTOR),
    "blockpools": ("a[data-test-id='horizontal-link-BlockPools']", By.CSS_SELECTOR),
    "ocs-storagecluster-cephblockpool-status": (
        "//*[text()= 'Ready']",
        By.XPATH,
    ),
    "ocs-storagecluster-cephblockpool": (
        ".co-resource-item__resource-name[data-test='ocs-storagecluster-cephblockpool']",
        By.CSS_SELECTOR,
    ),
    "odf-health-icon-color": (
        "//*[@data-test='OpenShift Data Foundation-health-item-icon']//*[@aria-labelledby='icon-title-403']",
        By.XPATH,
    ),
    "odf-capacityCardLink": (".odf-capacityCardLink--ellipsis", By.CSS_SELECTOR),
    "odf-performanceCardLink": (
        "td[class='pf-u-w-10 performanceCard--verticalAlign'] a",
        By.CSS_SELECTOR,
    ),
    "storagesystems": (".pf-c-breadcrumb__link", By.CSS_SELECTOR),
    "console_plugin_option": (
        ".pf-c-button.pf-m-link.pf-m-inline[data-test='edit-console-plugin']",
        By.CSS_SELECTOR,
    ),
    "save_console_plugin_settings": ("#confirm-action", By.CSS_SELECTOR),
    "warning-alert": ("div[aria-label='Warning Alert']", By.CSS_SELECTOR),
    "refresh-web-console": (
        "//button[normalize-space()='Refresh web console']",
        By.XPATH,
    ),
    "odf-operator": ("//h1[normalize-space()='OpenShift Data Foundation']", By.XPATH),
    "project-dropdown": (".pf-c-menu-toggle__text", By.CSS_SELECTOR),
    "project-search-bar": ("input[placeholder='Select project...']", By.CSS_SELECTOR),
    "plugin-available": (".pf-c-button.pf-m-link.pf-m-inline", By.CSS_SELECTOR),
    "storage-system-on-installed-operators": (
        "a[title='storagesystems.odf.openshift.io']",
        By.CSS_SELECTOR,
    ),
    "show-default-projects": (".pf-c-switch__toggle", By.CSS_SELECTOR),
    "ocs-storagecluster-storgesystem": (
        ".co-resource-item__resource-name[data-test-operand-link='ocs-storagecluster-storagesystem']",
        By.CSS_SELECTOR,
    ),
    "resources-tab": ("a[data-test-id='horizontal-link-Resources']", By.CSS_SELECTOR),
    "system-capacity": ("//h2[normalize-space()='System Capacity']", By.XPATH),
    "ocs-storagecluster": ("//a[normalize-space()='ocs-storagecluster']", By.XPATH),
}

locators = {
    "4.10": {
        "login": login,
        "page": page_nav,
        "generic": generic_locators,
        "deployment": {
            **deployment,
            **deployment_4_7,
            **deployment_4_9,
            **deployment_4_10,
        },
        "add_capacity": add_capacity,
        "validation": {**validation, **validation_4_8, **validation_4_9},
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8, **pvc_4_9},
    },
    "4.9": {
        "login": login,
        "page": page_nav,
        "deployment": {**deployment, **deployment_4_7, **deployment_4_9},
        "generic": generic_locators,
        "validation": {**validation, **validation_4_8, **validation_4_9},
        "acm_page": {**acm_page_nav, **acm_configuration},
        "add_capacity": add_capacity,
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8, **pvc_4_9},
    },
    "4.8": {
        "login": login,
        "page": page_nav,
        "deployment": {**deployment, **deployment_4_7},
        "generic": generic_locators,
        "ocs_operator": ocs_operator_locators,
        "obc": obc,
        "bucketclass": bucketclass,
        "mcg_stores": mcg_stores,
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8},
        "validation": {**validation, **validation_4_8},
        "add_capacity": add_capacity,
        "block_pool": block_pool,
        "storageclass": storageclass,
    },
    "4.7": {
        "login": login,
        "page": page_nav,
        "deployment": {**deployment, **deployment_4_7},
        "pvc": {**pvc, **pvc_4_7},
        "add_capacity": add_capacity,
        "validation": {**validation, **validation_4_7},
    },
    "4.6": {
        "login": login,
        "page": page_nav,
        "deployment": {**deployment, **deployment_4_6},
        "pvc": pvc,
    },
}
