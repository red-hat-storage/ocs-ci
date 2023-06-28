from selenium.webdriver.common.by import By
from ocs_ci.framework import config
from ocs_ci.ocs import constants


osd_sizes = ("512", "2048", "4096")

OCS_OPERATOR = "OpenShift Container Storage"
ODF_OPERATOR = "OpenShift Data Foundation"

login = {
    "pre_login_page_title": "Log In",
    "login_page_title": "Log in · Red Hat OpenShift Container Platform",
    "ocp_page": "Overview · Red Hat OpenShift Container Platform",
    "username": ("inputUsername", By.ID),
    "password": ("inputPassword", By.ID),
    "click_login": ("//button[text()='Log in']", By.XPATH),
    "kubeadmin_login_approval": ('a[title="Log in with kube:admin"]', By.CSS_SELECTOR),
    "proceed_to_login_btn": ("button[type='submit']", By.CSS_SELECTOR),
    "username_my_htpasswd": (
        'a[title="Log in with my_htpasswd_provider"]',
        By.CSS_SELECTOR,
    ),
    "skip_tour": (By.CSS_SELECTOR, 'button[data-test="tour-step-footer-secondary"]'),
}
azure_managed = ""
if (
    config.ENV_DATA["platform"] == constants.AZURE_PLATFORM
    and config.ENV_DATA["deployment_type"] == "managed"
):
    azure_managed = "Azure "
login_4_11 = {
    "ocp_page": f"Overview · {azure_managed}Red Hat OpenShift",
    "login_page_title": "Log in · Red Hat OpenShift",
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
    "thin-csi_sc": ("//span[text()='(default) | csi.vsphere.vmware.com']", By.XPATH),
    "gp2_sc": ('a[id="gp2-link"]', By.CSS_SELECTOR),
    "standard_sc": ('a[id="standard-link"]', By.CSS_SELECTOR),
    "standard_csi_sc": ('a[id="standard-csi-link"]', By.CSS_SELECTOR),
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
    "choose_local_storage_version_non_ga": (
        'a[data-test="local-storage-operator-optional-operators-openshift-marketplace"]',
        By.CSS_SELECTOR,
    ),
    "enable_in_transit_encryption": (
        'input[data-test="in-transit-encryption-checkbox"]',
        By.CSS_SELECTOR,
    ),
    "enable_nfs": ('input[id="enable-nfs"]', By.CSS_SELECTOR),
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

deployment_4_11 = {
    "osd_size_dropdown": ("//div[@data-test-id='dropdown-button']", By.XPATH),
    "thin_sc": ("thin-link", By.ID),
    "gp2_sc": ("gp2-link", By.ID),
    "gp2-csi_sc": ("gp2-csi-link", By.ID),
    "gp3-csi_sc": ("gp3-csi-link", By.ID),
    "managed-csi_sc": ("managed-csi-link", By.ID),
    "standard_sc": ("standard-link", By.ID),
    "512": ('button[data-test-dropdown-menu="0.5 TiB"]', By.CSS_SELECTOR),
    "2048": ('button[data-test-dropdown-menu="2 TiB"]', By.CSS_SELECTOR),
    "4096": ('button[data-test-dropdown-menu="4 TiB"]', By.CSS_SELECTOR),
}

deployment_4_12 = {
    "standard_csi_sc": ("standard-csi-link", By.ID),
}

generic_locators = {
    "project_selector": (
        'button[class="pf-c-dropdown__toggle pf-m-plain"],'
        'button[class="pf-c-menu-toggle co-namespace-dropdown__menu-toggle"]',
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
    "storage_class": ("//span[contains(text(), '{}')]", By.XPATH),
    "second_dropdown_option": (
        '//a[@data-test="dropdown-menu-item-link"]/../../li[2]',
        By.XPATH,
    ),
    "actions": (
        '//span[@class="pf-c-dropdown__toggle-text" and text()="Actions"]/..',
        By.XPATH,
    ),
    "three_dots": ('//button[@aria-label="Actions"]', By.XPATH),
    "confirm_action": (
        'button[id="confirm-action"],button[data-test="delete-action"]',
        By.CSS_SELECTOR,
    ),
    "submit_form": ('button[type="submit"]', By.CSS_SELECTOR),
    "ocs_operator": ('//h1[text()="OpenShift Container Storage"]', By.XPATH),
    "kebab_button": ('button[data-test-id="kebab-button"', By.CSS_SELECTOR),
    "resource_status": ('span[data-test="status-text"]', By.CSS_SELECTOR),
    "check_first_row_checkbox": ('input[name="checkrow0"]', By.CSS_SELECTOR),
    "remove_search_filter": ('button[aria-label="close"]', By.CSS_SELECTOR),
    "delete_resource_kebab_button": ('//*[contains(text(), "Delete")]', By.XPATH),
    "text_input_popup_rules": ("//*[@class='pf-c-helper-text__item-text']", By.XPATH),
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
    "create_project": ('//*[@id="yaml-create"]', By.XPATH),
    "project_name": ('input[id="input-name"]', By.CSS_SELECTOR),
    "save_project": ('button[data-test="confirm-action"]', By.CSS_SELECTOR),
    "Developer_dropdown": (
        'button[data-test-id="perspective-switcher-toggle"]',
        By.CSS_SELECTOR,
    ),
    "select_administrator": (
        "//a[@class='pf-c-dropdown__menu-item']//h2[@class='pf-c-title pf-m-md'][normalize-space()='Administrator']",
        By.XPATH,
    ),
    "obc_menu_name": ("//a[normalize-space()='Object Bucket Claims']", By.XPATH),
    "storageclass_dropdown": ("sc-dropdown", By.ID),
    "storageclass_text_field": ("//input[@id='search-bar']", By.XPATH),
    "bucketclass_dropdown": ("bc-dropdown", By.ID),
    "bucketclass_text_field": (
        'input[placeholder="Select BucketClass"],input[class="pf-c-form-control pf-m-search"]',
        By.CSS_SELECTOR,
    ),
    "resource_name": (
        '//td[@id="name"]//a[@class="co-resource-item__resource-name"]',
        By.XPATH,
    ),
    "default_bucketclass": ("noobaa-default-bucket-class-link", By.ID),
    "obc_name": ("obc-name", By.ID),
    "first_obc_link": ('a[class="co-resource-item__resource-name"]', By.CSS_SELECTOR),
    "delete_resource": (
        'li[id="Delete"] a[role="menuitem"]',
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
    "search-project": ("input[placeholder='Select project...']", By.CSS_SELECTOR),
    "test-project-link": ("//a[normalize-space()='{}']", By.XPATH),
    "expand_pvc": ("button[data-test-action='Expand PVC']", By.CSS_SELECTOR),
    "resize-value": ("//input[@name='requestSizeValue']", By.XPATH),
    "expand-btn": ("#confirm-action", By.CSS_SELECTOR),
    "pvc-status": (
        "dd[data-test-id='pvc-status'] span[data-test='status-text']",
        By.CSS_SELECTOR,
    ),
    "expected-capacity": (
        "//dd[contains(text(),'{}') and @data-test='pvc-requested-capacity']",
        By.XPATH,
    ),
    "new-capacity": (
        "//dd[contains(text(),'{}') and @data-test-id='pvc-capacity']",
        By.XPATH,
    ),
}

pvc_4_6 = {
    "pvc_create_button": ("#yaml-create", By.CSS_SELECTOR),
    "pvc_storage_class_selector": (
        "#storageclass-dropdown",
        By.CSS_SELECTOR,
    ),
    "ocs-storagecluster-ceph-rbd": (
        "a[id='ocs-storagecluster-ceph-rbd-link'] span[class='co-resource-item']",
        By.CSS_SELECTOR,
    ),
    "ocs-storagecluster-cephfs": (
        "a[id='ocs-storagecluster-cephfs-link'] span[class='co-resource-item']",
        By.CSS_SELECTOR,
    ),
    "pvc_name": ("#pvc-name", By.CSS_SELECTOR),
    "ReadWriteOnce": (
        "input[value='ReadWriteOnce']",
        By.CSS_SELECTOR,
    ),
    "ReadWriteMany": (
        "input[value='ReadWriteMany']",
        By.CSS_SELECTOR,
    ),
    "pvc_size": ("#request-size-input", By.CSS_SELECTOR),
    "pvc_create": ("#save-changes", By.CSS_SELECTOR),
    "pvc-status": (
        "dd[data-test-id='pvc-status'] span[data-test='status-text']",
        By.CSS_SELECTOR,
    ),
    "pvc_delete": (
        "button[data-test-action='Delete Persistent Volume Claim']",
        By.CSS_SELECTOR,
    ),
    "clone_name_input": ("//input[@value='{}']", By.XPATH),
}

pvc_4_7 = {
    "test-pvc-fs": ('a[data-test-id="test-pvc-fs"]', By.CSS_SELECTOR),
    "test-pvc-rbd": ("a[title='test-pvc-rbd']", By.CSS_SELECTOR),
    "Block": ("input[value='Block']", By.CSS_SELECTOR),
    "Filesystem": ("input[value='Filesystem']", By.CSS_SELECTOR),
    "search-project": ("input[placeholder='Select Project...']", By.CSS_SELECTOR),
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
    "test-storage-class": (
        "a[id='test-storage-class-link'] span[class='co-resource-item__resource-name']",
        By.CSS_SELECTOR,
    ),
    "pvc_storage_class": ("//*[text()='{}']", By.XPATH),
    "test-pvc-for-sc": ("a[title='test-pvc-for-sc']", By.CSS_SELECTOR),
}

pvc_4_9 = {
    "pvc_project_selector": (".pf-c-menu-toggle__text", By.CSS_SELECTOR),
    "test-project-link": ("//span[contains(text(),'{}')]", By.XPATH),
    "search-project": ("input[placeholder='Select project...']", By.CSS_SELECTOR),
}
pvc_4_12 = {
    "resize-value": ("//input[@data-test='pvc-expand-size-input']", By.XPATH),
}
page_nav = {
    "page_navigator_sidebar": ("page-sidebar", By.ID),
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
    "show-default-projects": (".pf-c-switch__toggle", By.CSS_SELECTOR),
}

page_nav_4_6 = {
    "persistentvolumeclaims_page": ("Persistent Volume Claims", By.LINK_TEXT),
}

page_nav_4_10 = {
    "odf_tab_new": ("Data Foundation", By.LINK_TEXT),
}

acm_page_nav = {
    "Home": ("//button[text()='Home']", By.XPATH),
    "Welcome_page": ("Welcome", By.LINK_TEXT),
    "Overview_page": ("Overview", By.LINK_TEXT),
    "Infrastructure": (
        "//button[normalize-space()='Infrastructure' and @class='pf-c-nav__link']",
        By.XPATH,
    ),
    "Clusters_page": ("Clusters", By.LINK_TEXT),
    "Bare_metal_assets_page": ("Bare metal assets", By.LINK_TEXT),
    "Automation_page": ("Automation", By.LINK_TEXT),
    "Infrastructure_environments_page": ("Infrastructure environments", By.LINK_TEXT),
    "Applications": ("Applications", By.LINK_TEXT),
    "Governance": ("Governance", By.LINK_TEXT),
    "Credentials": ("Credentials", By.LINK_TEXT),
    "Import_cluster": ("//*[text()='Import cluster']", By.XPATH),
    "Import_cluster_enter_name": ("clusterName", By.ID),
    "Import_mode": ('button[class="pf-c-select__toggle"]', By.CSS_SELECTOR),
    "choose_kubeconfig": ("//button[text()='Kubeconfig']", By.XPATH),
    "Kubeconfig_text": ("kubeConfigEntry", By.ID),
    "Submit_import": ("//button[text()='Import']", By.XPATH),
    "Acm_import_endswith_url": "import",
    "modal_dialog_close_button": ("//button[@aria-label='Close']", By.XPATH),
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
    "check-globalnet": ("//div[normalize-space()='True']", By.XPATH),
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
    "cc_create_cluster": ("//button[@id='createCluster']", By.XPATH),
    "cc_create_cluster_index_xpath": (
        "(//button[normalize-space()='Create cluster'])[1]",
        By.XPATH,
    ),
    "cc_provider_vmware_vsphere": (
        "//div[contains(text(),'VMware vSphere')]",
        By.XPATH,
    ),
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
    "cc_provider_creds_default_namespace": (
        "//button[normalize-space()='default']",
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
    "cc_cluster_status_page_download_config_dropdown": (
        "//button[@id='download-configuration']",
        By.XPATH,
    ),
    "cc_cluster_status_page_download_install_config": (
        "//a[normalize-space()='install-config']",
        By.XPATH,
    ),
    "cc_cluster_status_page_status": (
        "//button[normalize-space()={}]",
        By.XPATH,
    ),
    "cc_cluster_status_page_status_failed": (
        "//button[normalize-space()='Failed']",
        By.XPATH,
    ),
    "cc_cluster_status_page_status_creating": (
        "//button[normalize-space()='Creating']",
        By.XPATH,
    ),
    "cc_cluster_status_page_status_ready": (
        "//button[normalize-space()='Ready']",
        By.XPATH,
    ),
    "cc_cluster_status_page_download_config_kubeconfig": (
        "//a[normalize-space()='kubeconfig']",
        By.XPATH,
    ),
    "cc_table_entry": ("//a[normalize-space()='{}']", By.XPATH),
    "cc_cluster_details_page": ("//div[text()='Details']", By.XPATH),
    "cc_cluster_status_text": ("//span[text()='Status']", By.XPATH),
    "cc_details_toggle_icon": (
        "//span[@class='pf-c-card__header-toggle-icon']",
        By.XPATH,
    ),
    "cc_deployment_yaml_toggle_button": (
        "//span[@class='pf-c-switch__toggle']",
        By.XPATH,
    ),
    "cc_yaml_editor": ("//div[@class='yamlEditorContainer']", By.CSS_SELECTOR),
    "cc_install_config_tab": ("//a[normalize-space()='install-config']", By.XPATH),
    # Action button name format: '<cluster-name>-actions'
    "cc_delete_cluster_action_dropdown": ("//button[@id='{}']", By.XPATH),
    "cc_destroy_cluster": ("//a[normalize-space()='Destroy cluster']", By.XPATH),
    "cc_destroy_cluster_confirm_textbox": ("//input[@id='confirm']", By.XPATH),
    "cc_destroy_button": ("//button[normalize-space()='Destroy']", By.XPATH),
    "cc_cluster_destroying": ("//button[normalize-space()='Destroying']", By.XPATH),
    # Destroy in progress text = '<cluster-name> is being destroyed'
    "cc_cluster_being_destroyed_heading": (
        "//h4[normalize-space()='{} is being destroyed']",
        By.XPATH,
    ),
    "cc_destroy_cluster_back_to_clusters_button": (
        "//button[normalize-space()='Back to clusters']",
        By.XPATH,
    ),
}

acm_ui_specific = {
    "acm_2_5": {
        "cc_create_cluster_endswith_url": "create",
    },
    "acm_2_4": {"cc_create_cluster_endswith_url": "create-cluster"},
}

acm_configuration_4_11 = {
    "install-submariner-btn": ("install-submariner", By.ID),
    "nat-t-checkbox": ("natt-enable", By.ID),
}

acm_configuration_4_12 = {
    "click-local-cluster": ("//a[text()='local-cluster']", By.XPATH),
    "all-clusters": ("//a[normalize-space()='All Clusters']", By.XPATH),
    "local-cluster": ("//h2[text()='local-cluster']", By.XPATH),
    "cluster_status_check": ('//button[normalize-space()="{}"]', By.XPATH),
    "cluster_name": ("//a[normalize-space()='{}']", By.XPATH),
    "clusters-page": ("a[class='pf-c-breadcrumb__link']", By.CSS_SELECTOR),
    "nodes-tab": ("//a[normalize-space()='Nodes']", By.XPATH),
    "data-services": ("//button[normalize-space()='Data Services']", By.XPATH),
    "data-policies": ("//a[normalize-space()='Data policies']", By.XPATH),
    "replication-policy": ("//td[@id='replicationPolicy']", By.XPATH),
    "drpolicy-status": ("//*[text()='Validated']", By.XPATH),
    "workload-name": ('//*[text()="{}"]', By.XPATH),
    "search-bar": ("//input[@class='pf-c-text-input-group__text-input']", By.XPATH),
    "kebab-action": (
        "//button[@class='pf-c-dropdown__toggle pf-m-plain']",
        By.XPATH,
    ),
    "failover-app": ("//button[normalize-space()='Failover application']", By.XPATH),
    "relocate-app": ("//button[normalize-space()='Relocate application']", By.XPATH),
    "policy-dropdown": ("#drPolicy-selection", By.CSS_SELECTOR),
    "select-policy": ('//*[text()="{}"]', By.XPATH),
    "target-cluster-dropdown": (
        "//button[@data-test='target-cluster-dropdown-toggle']",
        By.XPATH,
    ),
    "failover-preferred-cluster-name": ("//button[text()='{}']", By.XPATH),
    "operation-readiness": ("//*[contains(text(), 'Ready')]", By.XPATH),
    "subscription-dropdown": (".pf-c-select__toggle.pf-m-typeahead", By.CSS_SELECTOR),
    "peer-ready": ("//i[normalize-space()='Peer ready']", By.XPATH),
    "initiate-action": ("#modal-intiate-action", By.CSS_SELECTOR),
    "close-action-modal": ("//button[normalize-space()='Close']", By.XPATH),
    "close-action-modal-page": ("//*[text()='Close']", By.XPATH),
    "title-alert-after-action": ("//h4[@class='pf-c-alert__title']", By.XPATH),
    "clear-filters": (
        "(//button[@type='button'][normalize-space()='Clear all filters'])[2]"
    ),
    "data-policy-hyperlink": (
        "1 policy",
        By.LINK_TEXT,
    ),
    "view-more-details": ("//button[@data-test='status-modal-link']", By.XPATH),
    "action-status-failover": ("//*[text()='Failed']", By.XPATH),
    "action-status-relocate": ('//*[text()="Relocated"]', By.XPATH),
    "create-cluster-set": ("//button[@id='createClusterSet']", By.XPATH),
    "review-btn": ("//button[@id='save']", By.XPATH),
    "next-btn": ("//button[@class='pf-c-button pf-m-primary']", By.XPATH),
}

acm_configuration_4_13 = {
    **acm_configuration_4_12,
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
    "standard_sc": ('a[id="standard-link"]', By.CSS_SELECTOR),
    "localblock_sc": ('a[id="localblock-link"]', By.CSS_SELECTOR),
    "managed-premium_sc": ('a[id="managed-premium-link"]', By.CSS_SELECTOR),
    "confirm_add_capacity": ('button[data-test="confirm-action"]', By.CSS_SELECTOR),
    "filter_pods": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
}

add_capacity_4_11 = {
    "thin_sc": ("thin-link", By.ID),
    "gp2_sc": ("gp2-link", By.ID),
    "gp2-csi_sc": ("gp2-csi-link", By.ID),
    "gp3-csi_sc": ("gp3-csi-link", By.ID),
    "managed-csi_sc": ("managed-csi-link", By.ID),
    "standard_sc": ("standard-link", By.ID),
    "localblock_sc": ("localblock-link", By.ID),
}

add_capacity_4_12 = {
    "add_capacity_button": ("//span[text()='Add Capacity']", By.XPATH),
    "confirm_add_capacity": ('button[data-test-id="confirm-action"]', By.CSS_SELECTOR),
}

block_pool_4_12 = {
    "actions_inside_pool": (
        "//span[text()='Actions']/..",
        By.XPATH,
    ),
    "delete_pool_inside_pool": (
        "//a[text()='Delete BlockPool']",
        By.XPATH,
    ),
}
block_pool_4_13 = {
    "second_select_replica_2": ("//div[text()='2-way Replication']/..", By.XPATH),
    "pool_name": (
        "//dt[normalize-space()='Pool name']/following-sibling::dd[1]",
        By.XPATH,
    ),
    "block_pool_volume_type": (
        "//dt[normalize-space()='Volume type']/following-sibling::dd[1]",
        By.XPATH,
    ),
    "block_pool_replica": (
        "//dt[normalize-space()='Replicas']/following-sibling::dd[1]",
        By.XPATH,
    ),
    "block_pool_used_capacity": (
        "//div[normalize-space()='Used']/following-sibling::div",
        By.XPATH,
    ),
    "blockpool_avail_capacity": (
        "//div[normalize-space()='Available']/following-sibling::div",
        By.XPATH,
    ),
    "blockpool_compression_status": (
        "//dt[normalize-space()='Compression status']/following-sibling::dd[1]",
        By.XPATH,
    ),
    "blockpool_compression_eligibility": (
        "//div[normalize-space()='Compression eligibility']/following-sibling::div",
        By.XPATH,
    ),
    "blockpool_compression_ratio": (
        "//div[normalize-space()='Compression ratio']/following-sibling::div",
        By.XPATH,
    ),
    "blockpool_compression_savings": (
        "//div[normalize-space()='Compression savings']/following-sibling::div",
        By.XPATH,
    ),
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
    "create-sc": (
        "#yaml-create",
        By.CSS_SELECTOR,
    ),
    "sc-name": (
        "#storage-class-name",
        By.CSS_SELECTOR,
    ),
    "sc-description": ("#storage-class-description", By.CSS_SELECTOR),
    "reclaim-policy": ("#storage-class-reclaim-policy", By.CSS_SELECTOR),
    "reclaim-policy-delete": ("//button[@id='Delete-link']", By.XPATH),
    "reclaim-policy-retain": ("#Retain-link", By.CSS_SELECTOR),
    "provisioner": ("#storage-class-provisioner", By.CSS_SELECTOR),
    "rbd-provisioner": (
        "//a[normalize-space()='openshift-storage.rbd.csi.ceph.com']",
        By.XPATH,
    ),
    "cephfs-provisioner": (
        "//a[normalize-space()='openshift-storage.cephfs.csi.ceph.com']",
        By.XPATH,
    ),
    "storage-pool": ("#pool-dropdown-id", By.CSS_SELECTOR),
    "ceph-block-pool": ("//div[@class='pf-c-dropdown__menu-item-main']", By.XPATH),
    "encryption": ("#storage-class-encryption", By.CSS_SELECTOR),
    "connections-details": (
        ".pf-c-button.pf-m-link[data-test='edit-kms-link']",
        By.CSS_SELECTOR,
    ),
    "service-name": ("#kms-service-name", By.CSS_SELECTOR),
    "kms-address": ("#kms-address", By.CSS_SELECTOR),
    "kms-port": ("#kms-address-port", By.CSS_SELECTOR),
    "save-btn": (
        ".pf-c-button.pf-m-secondary[data-test='save-action']",
        By.CSS_SELECTOR,
    ),
    "advanced-settings": (
        ".pf-c-button.pf-m-link.ocs-storage-class-encryption__form-body",
        By.CSS_SELECTOR,
    ),
    "backend-path": ("#kms-service-backend-path", By.CSS_SELECTOR),
    "tls-server-name": ("#kms-service-tls", By.CSS_SELECTOR),
    "vault-enterprise-namespace": ("//input[@id='kms-service-namespace']", By.XPATH),
    "browse-ca-certificate": ("(//input[@type='file'])[1]", By.XPATH),
    "browse-client-certificate": ("(//input[@type='file'])[2]", By.XPATH),
    "browse-client-private-key": ("(//input[@type='file'])[3]", By.XPATH),
    "pvc-expansion-check": (
        "input[class='create-storage-class-form__checkbox']",
        By.CSS_SELECTOR,
    ),
    "save-advanced-settings": ("#confirm-action", By.CSS_SELECTOR),
    "save-service-details": (
        ".pf-c-button.pf-m-secondary[data-test='save-action']",
        By.CSS_SELECTOR,
    ),
    "create": ("#save-changes", By.CSS_SELECTOR),
    "sc-dropdown": ("button[data-test-id='dropdown-button']", By.CSS_SELECTOR),
    "name-from-dropdown": ("//button[@id='NAME-link']", By.XPATH),
    "sc-search": ("input[placeholder='Search by name...']", By.CSS_SELECTOR),
    "select-sc": ("//a[normalize-space()='{}']", By.XPATH),
    "sc-actions": ("button[aria-label='Actions']", By.CSS_SELECTOR),
    "delete-storage-class": (
        "button[data-test-action='Delete StorageClass']",
        By.CSS_SELECTOR,
    ),
    "approve-storage-class-deletion": ("#confirm-action", By.CSS_SELECTOR),
    "backing_store_type": (
        "//*[text()='{}']/preceding-sibling::input[@name='backing-storage-radio-group']",
        By.XPATH,
    ),
    "button_with_txt": ("//button[text()=('{}')]", By.XPATH),
}

storageclass_4_9 = {
    "volume_binding_mode": (
        "#storage-class-volume-binding-mode",
        By.CSS_SELECTOR,
    ),
    "wait_for_first_consumer": (
        "#WaitForFirstConsumer-link",
        By.CSS_SELECTOR,
    ),
    "immediate": ("#Immediate-link", By.CSS_SELECTOR),
    "new_kms": ("#create-new-kms-connection", By.CSS_SELECTOR),
    "toggle_switch": ("no-label-switch-on-on", By.ID),
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
    "namespacestore_page": (
        'a[data-test-id="horizontal-link-Namespace Store"]',
        By.CSS_SELECTOR,
    ),
    "capacity_breakdown_options": (
        'button[class="pf-c-select__toggle"]',
        By.CSS_SELECTOR,
    ),
    "capacity_breakdown_projects": ("//button[text()='Projects']", By.XPATH),
    "capacity_breakdown_pods": ("//button[text()='Pods']", By.XPATH),
    "backingstore_name": ("input[placeholder='my-backingstore']", By.CSS_SELECTOR),
    "namespacestore_name": ("input[placeholder='my-namespacestore']", By.CSS_SELECTOR),
    "blockpool_name": ("input[placeholder='my-block-pool']", By.CSS_SELECTOR),
    "input_value_validator_icon": (".pf-c-icon", By.CSS_SELECTOR),
    "text_input_field_error_improvements": (
        "input[data-ouia-component-id='OUIA-Generated-TextInputBase-1']",
        By.CSS_SELECTOR,
    ),
    "blockpool_status": ("//span[@data-test='status-text']", By.XPATH),
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
    "ocs-operator": ("//h1[normalize-space()='OpenShift Container Storage']", By.XPATH),
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
    "ocs-external-storagecluster-storagesystem": (
        "a[href='/odf/system/ocs.openshift.io~v1~storagecluster/ocs-external-storagecluster/overview']",
        By.CSS_SELECTOR,
    ),
    "overview": (
        "a[data-test-id='horizontal-link-Overview']",
        By.CSS_SELECTOR,
    ),
    "odf-overview": ("a[data-test-id='horizontal-link-Overview']", By.CSS_SELECTOR),
    "1_storage_system": ("//button[normalize-space()='1 Storage System']", By.XPATH),
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
    "odf-capacityCardLink": ("//a[@class='odf-capacityCardLink--ellipsis']", By.XPATH),
    "storagesystem-details-page": (
        "//li[normalize-space()='StorageSystem details']",
        By.XPATH,
    ),
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
    "storagesystem-status-card": (
        ".pf-c-button.pf-m-link.pf-m-inline.co-dashboard-card__button-link.co-status-card__popup",
        By.CSS_SELECTOR,
    ),
    "block-and-file-health-message": ("div[class='text-muted']", By.CSS_SELECTOR),
    "storage-system-status-card-hyperlink": (
        "//div[@class='odf-status-popup__row']//a[contains(text(),'ocs-storagecluster-storagesystem')]",
        By.XPATH,
    ),
    "storage-system-external-status-card-hyperlink": (
        "(//a[contains(text(),'ocs-external-storagecluster-storagesystem')])[3]",
        By.XPATH,
    ),
    "storagesystem-details": (
        "//li[normalize-space()='StorageSystem details']",
        By.XPATH,
    ),
    "storagesystem-details-compress-state": (
        "#compressionStatus",
        By.CSS_SELECTOR,
    ),
    "storagecluster-blockpool-details-compress-status": (
        "article[data-test-id='compression-details-card'] dd[class='co-overview-details-card__item-value']",
        By.CSS_SELECTOR,
    ),
    "performance-card": ("//h2[normalize-space()='Performance']", By.XPATH),
    "backingstore": ("//a[normalize-space()='Backing Store']", By.XPATH),
    "backingstore-link": (
        "//a[normalize-space()='noobaa-default-backing-store']",
        By.XPATH,
    ),
    "backingstore-status": ("span[data-test='status-text']", By.CSS_SELECTOR),
    "backingstorage-breadcrumb": (
        ".pf-c-breadcrumb__link[data-test-id='breadcrumb-link-1']",
        By.CSS_SELECTOR,
    ),
    "bucketclass": ("a[data-test-id='horizontal-link-Bucket Class']", By.CSS_SELECTOR),
    "bucketclass-link": (
        "//a[normalize-space()='noobaa-default-bucket-class']",
        By.XPATH,
    ),
    "bucketclass-status": ("//span[@data-test='status-text']", By.XPATH),
    "bucketclass-breadcrumb": (
        ".pf-c-breadcrumb__link[data-test-id='breadcrumb-link-1']",
        By.CSS_SELECTOR,
    ),
    "namespace-store": ("//a[normalize-space()='Namespace Store']", By.XPATH),
    "developer_dropdown": (
        'button[data-test-id="perspective-switcher-toggle"]',
        By.CSS_SELECTOR,
    ),
    "select_administrator": (
        "//a[@class='pf-c-dropdown__menu-item']//h2[@class='pf-c-title pf-m-md'][normalize-space()='Administrator']",
        By.XPATH,
    ),
}

validation_4_10 = {
    "system-capacity": ("//div[contains(text(),'System Capacity')]", By.XPATH),
    "ocs-storagecluster-storagesystem": (
        "a[href='/odf/system/ocs.openshift.io~v1~storagecluster/ocs-storagecluster-storagesystem/overview']",
        By.CSS_SELECTOR,
    ),
    "ocs-external-storagecluster-storagesystem": (
        "a[href='/odf/system/ocs.openshift.io~v1~storagecluster/ocs-external-storagecluster-storagesystem/overview']",
        By.CSS_SELECTOR,
    ),
    "performance-card": ("//div[contains(text(),'Performance')]", By.XPATH),
    "storagesystem-status-card": (
        ".pf-c-button.pf-m-link.pf-m-inline.co-status-card__popup",
        By.CSS_SELECTOR,
    ),
    "storage-system-health-card-hyperlink": (
        "//div[@class='odf-storageSystemPopup__item--margin']//a[contains(text(),'ocs-storagecluster-storagesystem')]",
        By.XPATH,
    ),
    "developer_dropdown": (
        'button[data-test-id="perspective-switcher-toggle"]',
        By.CSS_SELECTOR,
    ),
    "select_administrator": (
        "//a[@class='pf-c-dropdown__menu-item']//h2[@class='pf-c-title pf-m-md'][normalize-space()='Administrator']",
        By.XPATH,
    ),
}

validation_4_11 = {
    "overview": ("//span[normalize-space()='Overview']", By.XPATH),
    "overview_odf_4_10": ("//a[@data-test-id='horizontal-link-Overview']", By.XPATH),
    "odf-overview": ("//a[@data-test-id='horizontal-link-Overview']", By.XPATH),
    "object": ("//span[normalize-space()='Object']", By.XPATH),
    "object-odf-4-10": ("//a[normalize-space()='Object']", By.XPATH),
    "blockandfile": ("//span[normalize-space()='Block and File']", By.XPATH),
    "blockandfile-odf-4-10": ("//a[normalize-space()='Block and File']", By.XPATH),
    "blockpools": ("//span[normalize-space()='BlockPools']", By.XPATH),
    "blockpools-odf-4-10": ("//a[normalize-space()='BlockPools']", By.XPATH),
    "system-capacity": ("//div[contains(text(),'System Capacity')]", By.XPATH),
    "backingstorage-breadcrumb": ("//a[normalize-space()='BackingStores']", By.XPATH),
    "backingstorage-breadcrumb-odf-4-10": (
        "//a[normalize-space()='noobaa.io~v1alpha1~BackingStore']",
        By.XPATH,
    ),
    "bucketclass-breadcrumb": ("//a[normalize-space()='BucketClasses']", By.XPATH),
    "bucketclass-breadcrumb-odf-4-10": (
        "//a[normalize-space()='noobaa.io~v1alpha1~BucketClass']",
        By.XPATH,
    ),
    "developer_dropdown": (
        'button[data-test-id="perspective-switcher-toggle"]',
        By.CSS_SELECTOR,
    ),
    "select_administrator": (
        "//a[@class='pf-c-dropdown__menu-item']//h2[@class='pf-c-title pf-m-md'][normalize-space()='Administrator']",
        By.XPATH,
    ),
}


locators = {
    "4.13": {
        "login": {**login, **login_4_11},
        "page": {**page_nav, **page_nav_4_10},
        "generic": generic_locators,
        "add_capacity": {**add_capacity, **add_capacity_4_11, **add_capacity_4_12},
        "deployment": {
            **deployment,
            **deployment_4_7,
            **deployment_4_9,
            **deployment_4_10,
            **deployment_4_11,
            **deployment_4_12,
        },
        "validation": {
            **validation,
            **validation_4_8,
            **validation_4_9,
            **validation_4_10,
            **validation_4_11,
        },
        "obc": obc,
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8, **pvc_4_9, **pvc_4_12},
        "acm_page": {
            **acm_page_nav,
            **acm_configuration,
            **acm_configuration_4_11,
            **acm_configuration_4_12,
            **acm_configuration_4_13,
        },
        "block_pool": {**block_pool, **block_pool_4_12, **block_pool_4_13},
        "storageclass": {**storageclass, **storageclass_4_9},
        "bucketclass": bucketclass,
    },
    "4.12": {
        "login": {**login, **login_4_11},
        "page": {**page_nav, **page_nav_4_10},
        "generic": generic_locators,
        "add_capacity": {**add_capacity, **add_capacity_4_11, **add_capacity_4_12},
        "deployment": {
            **deployment,
            **deployment_4_7,
            **deployment_4_9,
            **deployment_4_10,
            **deployment_4_11,
            **deployment_4_12,
        },
        "validation": {
            **validation,
            **validation_4_8,
            **validation_4_9,
            **validation_4_10,
            **validation_4_11,
        },
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8, **pvc_4_9, **pvc_4_12},
        "acm_page": {
            **acm_page_nav,
            **acm_configuration,
            **acm_configuration_4_11,
            **acm_configuration_4_12,
        },
        "obc": obc,
        "block_pool": {**block_pool, **block_pool_4_12},
    },
    "4.11": {
        "login": {**login, **login_4_11},
        "page": {**page_nav, **page_nav_4_10},
        "generic": generic_locators,
        "deployment": {
            **deployment,
            **deployment_4_7,
            **deployment_4_9,
            **deployment_4_10,
            **deployment_4_11,
        },
        "validation": {
            **validation,
            **validation_4_8,
            **validation_4_9,
            **validation_4_10,
            **validation_4_11,
        },
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8, **pvc_4_9},
        "acm_page": {
            **acm_page_nav,
            **acm_configuration,
            **acm_configuration_4_11,
        },
        "add_capacity": {**add_capacity, **add_capacity_4_11},
        "obc": obc,
    },
    "4.10": {
        "login": login,
        "page": {**page_nav, **page_nav_4_10},
        "generic": generic_locators,
        "deployment": {
            **deployment,
            **deployment_4_7,
            **deployment_4_9,
            **deployment_4_10,
        },
        "add_capacity": add_capacity,
        "validation": {
            **validation,
            **validation_4_8,
            **validation_4_9,
            **validation_4_10,
        },
        "pvc": {**pvc, **pvc_4_7, **pvc_4_8, **pvc_4_9},
        "acm_page": {**acm_page_nav, **acm_configuration},
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
        "storageclass": {**storageclass, **storageclass_4_9},
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
        "page": {**page_nav, **page_nav_4_6},
        "deployment": {**deployment, **deployment_4_6},
        "pvc": {**pvc, **pvc_4_6},
        "validation": validation,
    },
}
