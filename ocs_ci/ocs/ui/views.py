from selenium.webdriver.common.by import By

osd_sizes = ("512", "2048", "4096")

OCS_OPERATOR = "OpenShift Container Storage"
ODF_OPERATOR = "OpenShift Data Foundation"

login = {
    "ocp_page": "Overview · Red Hat OpenShift Container Platform",
    "username": ("inputUsername", By.ID),
    "password": ("inputPassword", By.ID),
    "click_login": ("//button[text()='Log in']", By.XPATH),
    "flexy_kubeadmin": ('a[title="Log in with kube:admin"]', By.CSS_SELECTOR),
}

deployment = {
    "click_install_ocs": ('a[data-test-id="operator-install-btn"]', By.CSS_SELECTOR),
    "choose_ocs_version": (
        'a[data-test="ocs-operator-ocs-catalogsource-openshift-marketplace"]',
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
    "enable_default_porjects": ('span[id="no-label-switch-on-off"]', By.CSS_SELECTOR),
    "choose_openshift-storage_project": (
        "//span[text()='openshift-storage']",
        By.XPATH,
    ),
    "choose_all_projects": ("//span[text()='All Projects']", By.XPATH),
    "click_odf_operator": (
        'a[data-test="odf-operator-ocs-catalogsource-openshift-marketplace"]',
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

storage_class = {
    "create-sc": (
        '#yaml-create',
        By.CSS_SELECTOR,
    ),
    "sc-name": (
        "#storage-class-name",
        By.CSS_SELECTOR,
    ),
    "sc-description": ("#storage-class-description", By.CSS_SELECTOR),
    "reclaim-policy": ("#storage-class-reclaim-policy", By.CSS_SELECTOR),
    "reclaim-policy-delete": ("//button[@id='Delete-link']", By.XPATH),
    "provisioner": ("#storage-class-provisioner", By.CSS_SELECTOR),
    "rbd-provisioner": ("//a[normalize-space()='openshift-storage.rbd.csi.ceph.com']", By.XPATH),
    "storage-pool": ("#pool-dropdown-id", By.CSS_SELECTOR),
    "ceph-block-pool": ("//div[@class='pf-c-dropdown__menu-item-main']", By.XPATH),
    "encryption": ("#storage-class-encryption", By.CSS_SELECTOR),
    "connections-details": (".pf-c-button.pf-m-link[data-test='edit-kms-link']", By.CSS_SELECTOR),
    "service-name": ("#kms-service-name", By.CSS_SELECTOR),
    "kms-address": ("#kms-address", By.CSS_SELECTOR),
    "kms-port": ("#kms-address-port", By.CSS_SELECTOR),
    "save-btn": (".pf-c-button.pf-m-secondary[data-test='save-action']", By.CSS_SELECTOR),
    "advanced-settings": (".pf-c-button.pf-m-link.ocs-storage-class-encryption__form-body", By.CSS_SELECTOR
                          ),
    "backend-path": ("#kms-service-backend-path", By.CSS_SELECTOR),
    "tls-server-name": ("#kms-service-tls", By.CSS_SELECTOR),
    "vault-enterprise-namespace": ("#kms-service-namespace", By.CSS_SELECTOR),
    "browse-ca-certificate": ("#kms-service-ca-cert-browse-button", By.CSS_SELECTOR),
    "browse-client-certificate":("#kms-service-cert-browse-button", By.CSS_SELECTOR),
    "browse-client-private-key":("#kms-service-key-browse-button", By.CSS_SELECTOR),
    "pvc-expansion-check": ("input[class='create-storage-class-form__checkbox']", By.CSS_SELECTOR),
    "save-advanced-settings": ("#confirm-action", By.CSS_SELECTOR),
    "create": ("#save-changes", By.CSS_SELECTOR),
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
}

add_capacity = {
    "ocs_operator": (
        'a[data-test-operator-row="OpenShift Container Storage"]',
        By.CSS_SELECTOR,
    ),
    "storage_cluster_tab": (
        'a[data-test-id="horizontal-link-Storage Cluster"]',
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

locators = {
    "4.9": {
        "login": login,
        "page": page_nav,
        "deployment": {**deployment, **deployment_4_7, **deployment_4_9},
        "generic": generic_locators,
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
        "storage_class": storage_class
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
