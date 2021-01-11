login = {
    "Login Button": 'button[class="pf-c-button pf-m-primary pf-m-block"]',
    "OCP Login Page": "Login - Red Hat OpenShift Container Platform",
    "OCP Page": "Overview · Red Hat OpenShift Container Platform",
}

deployment = {
    "Operators Tab": "//button[normalize-space(text())='Operators']",
    "OperatorHub Tab": "OperatorHub",
    "Installed Operators Tab": "Installed Operators",
    "Search Operators": 'input[placeholder="Filter by keyword..."]',
    "Choose OCS": '//*[@id="content-scrollable"]/div[1]/div/div[2]/div/div/'
    "div[2]/div[2]/div[1]/div/div/div/div[1]/a/div[1]",
    "Click Install OCS": 'a[data-test-id="operator-install-btn"]',
    "OCS Installed": 'a[data-test-operator-row="OpenShift Container Storage"]',
    "Storage Cluster Tab": 'a[data-test-id="horizontal-link-Storage Cluster"]',
    "Create Storage Cluster": '[data-test="yaml-create"]',
    "Internal": "/html/body/div[2]/div/div/div/div/div[1]/main/div/div/div/"
    "section/div/div[3]/div[2]/div/div/label[1]/input",
    "Storage Class Dropdown": 'button[id="ceph-sc-dropdown"]',
    "thin": 'a[id="thin-link"]',
    "OSD Size Dropdown": 'button[id="ocs-service-capacity-dropdown"]',
    "0.5TiB": '//*[@id="512Gi-link"]/span',
    "2TiB": '//*[@id="2Ti-link"]/span',
    "4TiB": '//*[@id="4Ti-link"]/span',
    "Enable Encryption": '//*[@id="content-scrollable"]/div[3]/div/form/div[3]/div/div/label/span[1]',
    "Create Storage Cluster Page": '//*[@id="content-scrollable"]/div[3]/div/form/div[5]/div/div/div/button[1]',
}

pvc = {
    "Storage Tab": "//button[normalize-space(text())='Storage']",
    "PVC Page": "Persistent Volume Claims",
    "PVC Project Selector": 'button[class="pf-c-dropdown__toggle pf-m-plain"]',
    "PVC Select Project openshift-storage": 'a[id="openshift-storage-link"]',
    "PVC Create Button": "//*[@id='yaml-create']",
    "PVC Storage Class Selector": "//*[@id='storageclass-dropdown']",
    "ocs-storagecluster-ceph-rbd": "//*[@id='ocs-storagecluster-ceph-rbd-link']/span",
    "ocs-storagecluster-cephfs": "//*[@id='ocs-storagecluster-cephfs-link']/span",
    "PVC Name": "//*[@id='pvc-name']",
    "ReadWriteOnce": "//*[@id='content-scrollable']/div/form/div[1]/div[3]/label[1]/input",
    "ReadWriteMany": "//*[@id='content-scrollable']/div/form/div[1]/div[3]/label[2]/input",
    "ReadOnlyMany": "//*[@id='content-scrollable']/div/form/div[1]/div[3]/label[3]/input",
    "PVC Size": "//*[@id='request-size-input']",
    "PVC Create": "//*[@id='save-changes']",
    "PVC Actions": 'button[data-test-id="actions-menu-button"]',
    "PVC Delete": 'button[data-test-action="Delete Persistent Volume Claim"]',
    "Confirm PVC Deletion": 'button[data-test="confirm-action"]',
    "PVC Test": 'a[data-test-id="test-pvc-fs"]',
}
