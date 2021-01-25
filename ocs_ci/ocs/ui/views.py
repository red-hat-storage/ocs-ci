from selenium.webdriver.common.by import By

login = {
    "ocp_page": "Overview · Red Hat OpenShift Container Platform",
    "username": ("inputUsername", By.ID),
    "password": ("inputPassword", By.ID),
    "click_login": ("/html/body/div/div/main/div/form/div[4]/button", By.XPATH),
}

deployment_4_7 = {
    "click_install_ocs": ('a[data-test-id="operator-install-btn"]', By.CSS_SELECTOR),
    "choose_ocs_version": (
        '//*[@id="content-scrollable"]/div[1]/div/div[2]/div/div/div[2]'
        "/div[2]/div[1]/div/div/div/div[1]/a/div[1]",
        By.XPATH,
    ),
    "search_operators": ('input[placeholder="Filter by keyword..."]', By.CSS_SELECTOR),
    "operators_tab": ("//button[normalize-space(text())='Operators']", By.XPATH),
    "operatorhub_tab": ("OperatorHub", By.LINK_TEXT),
    "installed_operators_tab": ("Installed Operators", By.LINK_TEXT),
    "storage_cluster_tab": (
        'a[data-test-id="horizontal-link-Storage Cluster"]',
        By.CSS_SELECTOR,
    ),
    "create_storage_cluster": ('button[data-test="item-create"]', By.CSS_SELECTOR),
    "internal_mode": ('input[data-test="Internal-radio-input"]', By.CSS_SELECTOR),
    "storage_class_dropdown": (
        'button[data-test="storage-class-dropdown"]',
        By.CSS_SELECTOR,
    ),
    "thin_sc": ('//*[@id="thin-link"]/span/span[3]/div', By.XPATH),
    "osd_size_dropdown": ('button[data-test-id="dropdown-button"]', By.CSS_SELECTOR),
    "0.5T": ('button[data-test-dropdown-menu="512Gi"]', By.CSS_SELECTOR),
    "2T": ('button[data-test-dropdown-menu="2Ti"]', By.CSS_SELECTOR),
    "4T": ('button[data-test-dropdown-menu="4Ti"]', By.CSS_SELECTOR),
    "all_nodes": (
        '//*[@id="content-scrollable"]/div[3]/div[2]/div/div/div/div/div/form/'
        "div[5]/div[2]/div[2]/div/div[2]/div/div/div/div/table/thead/tr/td/input",
        By.XPATH,
    ),
    "next_capacity": (
        '//*[@id="content-scrollable"]/div[3]/div[2]/div/div/footer/button[1]',
        By.XPATH,
    ),
    "enable_encryption": ('input[data-test="encryption-checkbox"]', By.CSS_SELECTOR),
    "wide_encryption": ('//*[@id="cluster-wide-encryption"]', By.XPATH),
    "class_encryption": ('//*[@id="storage-class-encryption"]', By.XPATH),
    "advanced_encryption": ('//*[@id="advanced-encryption"]', By.XPATH),
    "kms_service_name": ('//*[@id="kms-service-name"]', By.XPATH),
    "kms_address": ('//*[@id="kms-address"]', By.XPATH),
    "kms_address_port": ('//*[@id="kms-address-port"]', By.XPATH),
    "kms_token": ('//*[@id="kms-token"]', By.XPATH),
    "next_on_configure": (
        '//*[@id="content-scrollable"]/div[3]/div[2]/div/div/footer/button[1]',
        By.XPATH,
    ),
    "create_on_review": (
        '//*[@id="content-scrollable"]/div[3]/div[2]/div/div/footer/button[1]',
        By.XPATH,
    ),
}

pvc = {
    "storage_tab": ("//button[normalize-space(text())='Storage']", By.XPATH),
    "pvc_page": ("Persistent Volume Claims", By.LINK_TEXT),
    "pvc_project_selector": (
        'button[class="pf-c-dropdown__toggle pf-m-plain"]',
        By.CSS_SELECTOR,
    ),
    "select_openshift-storage_project": (
        'a[id="openshift-storage-link"]',
        By.CSS_SELECTOR,
    ),
    "pvc_create_button": ("//*[@id='yaml-create']", By.XPATH),
    "pvc_storage_class_selector": ("//*[@id='storageclass-dropdown']", By.XPATH),
    "ocs-storagecluster-ceph-rbd": (
        "//*[@id='ocs-storagecluster-ceph-rbd-link']/span",
        By.XPATH,
    ),
    "ocs-storagecluster-cephfs": (
        "//*[@id='ocs-storagecluster-cephfs-link']/span",
        By.XPATH,
    ),
    "pvc_name": ("//*[@id='pvc-name']", By.XPATH),
    "ReadWriteOnce": (
        "//*[@id='content-scrollable']/div/form/div[1]/div[3]/label[1]/input",
        By.XPATH,
    ),
    "ReadWriteMany": (
        "//*[@id='content-scrollable']/div/form/div[1]/div[3]/label[2]/input",
        By.XPATH,
    ),
    "ReadOnlyMany": (
        "//*[@id='content-scrollable']/div/form/div[1]/div[3]/label[3]/input",
        By.XPATH,
    ),
    "pvc_size": ("//*[@id='request-size-input']", By.XPATH),
    "pvc_create": ("//*[@id='save-changes']", By.XPATH),
    "pvc_actions": ('button[data-test-id="actions-menu-button"]', By.CSS_SELECTOR),
    "pvc_delete": (
        'button[data-test-action="Delete Persistent Volume Claim"]',
        By.CSS_SELECTOR,
    ),
    "confirm_pvc_deletion": ('button[data-test="confirm-action"]', By.CSS_SELECTOR),
    "pvc_test": ('a[data-test-id="test-pvc-fs"]', By.CSS_SELECTOR),
    "search_pvc": ('input[data-test-id="item-filter"]', By.CSS_SELECTOR),
}

locators = {
    "4.7": {
        "login": login,
        "deployment": deployment_4_7,
        "pvc": pvc,
    },
    "4.6": {
        "login": login,
        "deployment": deployment_4_7,
        "pvc": pvc,
    },
}
