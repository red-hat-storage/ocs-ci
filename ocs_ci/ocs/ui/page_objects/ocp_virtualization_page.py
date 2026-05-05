"""
OpenShift Virtualization console flow: Fedora template VM via UI.

"""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from typing import List, Sequence

from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.helpers import helpers
from ocs_ci.ocs.ocp import get_ocp_url
from ocs_ci.ocs.ui.base_ui import close_browser, login_ui
from ocs_ci.ocs.ui.page_objects.vm_deployment_base_page import BasePage, Locator

logger = logging.getLogger(__name__)


@dataclass
class FedoraUITemplateDeployConfig:
    """User-controlled parameters for the Fedora-from-template wizard."""

    namespace: str
    storage_class: str
    vm_name: str
    registry_image: str = "quay.io/containerdisks/fedora:latest"
    cloud_user_password: str = "fedora"
    cpus: int = 12
    memory_gib: str = "16Gi"
    customize_timeout: float = 180.0
    customize_reload_interval: float = 45.0


class OCPVirtualizationPage(BasePage):
    """
    Page object for Administrator -> Virtualization -> VirtualMachines wizard.
    """

    # --- Navigation ---
    NAV_VIRTUALIZATION: Sequence[Locator] = (
        ("//a[@data-test-id='virtualization-nav-item']", By.XPATH),
        (
            "//a[contains(@href,'kubevirt.io') and contains(@class,'pf-c-nav__link')]",
            By.XPATH,
        ),
        ("//a[contains(@href,'virtualization')]", By.XPATH),
    )
    NAV_VMS: Sequence[Locator] = (
        ("//a[@data-test-id='virtualmachines-nav-item']", By.XPATH),
        ("//a[@data-test='nav' and normalize-space()='Virtual machines']", By.XPATH),
        ("//a[contains(@href,'VirtualMachine')]", By.XPATH),
    )

    # --- List / project ---
    PROJECT_FILTER: Sequence[Locator] = (
        ("input[data-test='project-dropdown-input']", By.CSS_SELECTOR),
        ("input[data-test-id='namespace-bar-search-input']", By.CSS_SELECTOR),
        ("input[placeholder='Select project']", By.CSS_SELECTOR),
    )

    # --- Create flow ---
    BTN_CREATE: Sequence[Locator] = (
        ("button[data-test-id='item-create']", By.CSS_SELECTOR),
        ("button[data-test='item-create']", By.CSS_SELECTOR),
        (
            "//button[contains(@class,'pf-c-button')][normalize-space()='Create']",
            By.XPATH,
        ),
    )
    MENU_FROM_TEMPLATE: Sequence[Locator] = (
        (
            "//span[normalize-space()='From template']/ancestor::*[@role='menuitem']",
            By.XPATH,
        ),
        ("//button[normalize-space()='From template']", By.XPATH),
        ("//*[@role='menuitem']//*[normalize-space()='From template']", By.XPATH),
    )
    TILE_FEDORA: Sequence[Locator] = (
        ("//a[contains(@href,'fedora')]", By.XPATH),
        ("//*[contains(@data-test-id,'fedora')]", By.XPATH),
        (
            "//div[contains(@class,'catalog-tile')][.//span[contains(.,'Fedora')]]",
            By.XPATH,
        ),
        ("//span[normalize-space()='Fedora VM']/ancestor::a", By.XPATH),
    )
    BTN_CUSTOMIZE: Sequence[Locator] = (
        ("//button[contains(.,'Customize virtual machine')]", By.XPATH),
        ("//a[contains(.,'Customize virtual machine')]", By.XPATH),
    )

    # --- Disk / source ---
    DISK_SOURCE_TOGGLE: Sequence[Locator] = (
        ("//button[contains(@aria-label,'Disk source')]", By.XPATH),
        ("//button[contains(@aria-label,'disk source')]", By.XPATH),
    )
    OPTION_REGISTRY: Sequence[Locator] = (
        ("//button[normalize-space()='Registry']", By.XPATH),
        ("//*[@role='option' and contains(.,'Registry')]", By.XPATH),
    )
    REGISTRY_IMAGE: Sequence[Locator] = (
        ("input[aria-label*='Registry']", By.CSS_SELECTOR),
        ("input[placeholder*='registry']", By.CSS_SELECTOR),
        ("input[placeholder*='Registry']", By.CSS_SELECTOR),
        ("input[name='url']", By.CSS_SELECTOR),
    )

    # --- Tabs ---
    TAB_DISKS: Sequence[Locator] = (
        ("button[data-test-id='horizontal-link-Disks']", By.CSS_SELECTOR),
        ("//button[contains(.,'Disks')]", By.XPATH),
    )
    TAB_YAML: Sequence[Locator] = (
        ("button[data-test='yaml-tab-link']", By.CSS_SELECTOR),
        ("button[data-test-id='yaml-tab']", By.CSS_SELECTOR),
        ("//button[contains(.,'YAML')]", By.XPATH),
    )

    BTN_EDIT_ROOTDISK: Sequence[Locator] = (
        (
            "//tr[.//*[contains(translate(normalize-space(.)"
            ",'ROOTDISK','rootdisk'),'rootdisk')]]"
            "//button[@aria-label='Edit']",
            By.XPATH,
        ),
        (
            "//tr[.//td[contains(.,'rootdisk')]]//button[contains(@aria-label,'Edit')]",
            By.XPATH,
        ),
    )
    SC_TOGGLE: Sequence[Locator] = (
        ("button[data-test='storage-class-dropdown']", By.CSS_SELECTOR),
        ("//button[contains(@aria-label,'Storage class')]", By.XPATH),
        ("//label[contains(.,'Storage class')]/following::button[1]", By.XPATH),
    )
    SC_OPTION_TEMPLATE: Sequence[Locator] = (
        (
            "//button[contains(@class,'pf-c-select__menu-item')]"
            "[contains(.,'{sc}')]",
            By.XPATH,
        ),
        ("//*[@role='option' and contains(.,'{sc}')]", By.XPATH),
    )

    # --- CPU / Memory ---
    CPU_INPUT: Sequence[Locator] = (
        ("input[name='cpus']", By.CSS_SELECTOR),
        ("input#cpus", By.CSS_SELECTOR),
        ("input[data-test-id='vm-cpu-input']", By.CSS_SELECTOR),
    )
    MEMORY_INPUT: Sequence[Locator] = (
        ("input[name='memory']", By.CSS_SELECTOR),
        ("input#memory", By.CSS_SELECTOR),
        ("input[data-test-id='vm-memory-input']", By.CSS_SELECTOR),
    )

    BTN_CREATE_VM: Sequence[Locator] = (
        ("//button[contains(.,'Create Virtual Machine')]", By.XPATH),
        ("button[data-test-id='create-vm-wizard-submit']", By.CSS_SELECTOR),
        ("//button[@type='submit'][contains(.,'Create')]", By.XPATH),
    )

    MONACO_TEXTAREA: Sequence[Locator] = (
        ("div.monaco-editor textarea", By.CSS_SELECTOR),
        ("textarea[data-test='yaml-textarea']", By.CSS_SELECTOR),
    )

    def navigate_to_virtual_machines(self) -> None:
        logger.info("Navigating to Virtualization -> VirtualMachines")
        self.click_first_matching(self.NAV_VIRTUALIZATION, timeout=90)
        self.click_first_matching(self.NAV_VMS, timeout=90)
        self.page_has_loaded(retries=15, sleep_time=1)

    def select_namespace(self, namespace: str) -> None:
        logger.info("Selecting namespace %s", namespace)
        self.do_click(self.page_nav["drop_down_projects"], timeout=60)
        self.click_first_matching(self.PROJECT_FILTER, timeout=30)
        flt = self.wait_any_visible(self.PROJECT_FILTER, timeout=30)
        flt.send_keys(Keys.CONTROL + "a")
        flt.send_keys(Keys.DELETE)
        flt.send_keys(namespace)
        ns_locators: List[Locator] = [
            (
                f"//button[contains(@class,'pf-c-menu__item')][contains(.,'{namespace}')]",
                By.XPATH,
            ),
            (f"//span[normalize-space()='{namespace}']/ancestor::button[1]", By.XPATH),
            (
                f"//span[contains(@class,'pf-c-menu__item-text')][normalize-space()='{namespace}']",
                By.XPATH,
            ),
        ]
        self.click_first_matching(ns_locators, timeout=60)
        self.page_has_loaded(retries=10, sleep_time=1)

    def _disk_or_customize_visible(self) -> bool:
        markers = self.BTN_CUSTOMIZE + self.TAB_DISKS + self.REGISTRY_IMAGE
        return any(self.element_visible(loc, timeout=1) for loc in markers)

    def open_fedora_template_wizard(self, cfg: FedoraUITemplateDeployConfig) -> None:
        self.navigate_to_virtual_machines()
        self.select_namespace(cfg.namespace)
        self.click_first_matching(self.BTN_CREATE, timeout=60)
        self.click_first_matching(self.MENU_FROM_TEMPLATE, timeout=60)
        self.click_first_matching(self.TILE_FEDORA, timeout=120)
        logger.info("Waiting for template wizard (disk / customize controls)")
        self.reload_until(
            lambda _d: self._disk_or_customize_visible(),
            total_timeout=cfg.customize_timeout,
            reload_interval=cfg.customize_reload_interval,
        )

    def configure_registry_disk(self, image: str) -> None:
        logger.info("Setting disk source to Registry with image %s", image)
        self.click_first_matching(self.DISK_SOURCE_TOGGLE, timeout=60)
        self.click_first_matching(self.OPTION_REGISTRY, timeout=60)
        sent = False
        for loc in self.REGISTRY_IMAGE:
            if self.element_visible(loc, timeout=5):
                self.send_keys_when_visible(loc, image, timeout=60)
                sent = True
                break
        assert sent, "Registry URL / container image input not found on wizard"

    def click_customize_virtual_machine(
        self, cfg: FedoraUITemplateDeployConfig
    ) -> None:
        logger.info("Opening Customize virtual machine (with reload fallback)")
        self.reload_until(
            lambda _d: self.element_visible(self.BTN_CUSTOMIZE[0], timeout=1)
            or self.element_visible(self.BTN_CUSTOMIZE[1], timeout=1),
            total_timeout=cfg.customize_timeout,
            reload_interval=cfg.customize_reload_interval,
        )
        self.click_first_matching(self.BTN_CUSTOMIZE, timeout=120)
        self.page_has_loaded(retries=15, sleep_time=1)

    def open_disks_and_set_rootdisk_storage_class(self, storage_class: str) -> None:
        logger.info("Updating rootdisk storage class to %s", storage_class)
        self.click_first_matching(self.TAB_DISKS, timeout=60)
        self.click_first_matching(self.BTN_EDIT_ROOTDISK, timeout=60)
        self.click_first_matching(self.SC_TOGGLE, timeout=60)
        sc_opts: List[Locator] = [
            (tpl[0].format(sc=storage_class), tpl[1]) for tpl in self.SC_OPTION_TEMPLATE
        ]
        self.click_first_matching(sc_opts, timeout=120)

    def _read_yaml_from_editor(self) -> str:
        self.click_first_matching(self.TAB_YAML, timeout=60)
        for loc in self.MONACO_TEXTAREA:
            try:
                el = self.wait(10).until(
                    ec.presence_of_element_located((loc[1], loc[0]))
                )
                val = el.get_attribute("value")
                if val:
                    return val
            except TimeoutException:
                continue
        js = """
        try {
          const m = monaco && monaco.editor && monaco.editor.getModels()[0];
          return m ? m.getValue() : '';
        } catch (e) { return ''; }
        """
        out = self.driver.execute_script(js)
        return out or ""

    def yaml_set_password_cloudinit(self, password: str) -> None:
        logger.info("Opening YAML tab and aligning cloud-init password")
        current = self._read_yaml_from_editor()
        monaco = self.wait_any_visible(self.MONACO_TEXTAREA, timeout=30)
        updated, n = re.subn(
            r"(password:\s*)([^\s#]+)",
            rf"\g<1>{password}",
            current,
            count=1,
            flags=re.IGNORECASE,
        )
        if n == 0:
            updated, n = re.subn(
                r"(chpasswd:\s*\|\s*\n\s*list:\s*\|\s*\n\s*[^\n:]+:)([^\n]+)",
                rf"\g<1> {password}",
                current,
                count=1,
            )
        if n == 0:
            raise AssertionError(
                "Could not locate a password field pattern in YAML; "
                "update ocp_virtualization_page.yaml_set_password_cloudinit patterns"
            )
        try:
            ok = self.driver.execute_script(
                """
                try {
                  const m = monaco && monaco.editor && monaco.editor.getModels()[0];
                  if (m) { m.setValue(arguments[0]); return true; }
                } catch (e) {}
                return false;
                """,
                updated,
            )
            if not ok:
                raise RuntimeError("monaco setValue not applied")
        except Exception:
            monaco.click()
            monaco.send_keys(Keys.CONTROL + "a")
            monaco.send_keys(Keys.DELETE)
            monaco.send_keys(updated)

    def set_cpu_and_memory(self, cpus: int, memory_gib: str) -> None:
        logger.info("Setting CPU=%s memory=%s", cpus, memory_gib)
        self.send_keys_when_visible(self.CPU_INPUT[0], str(cpus), timeout=60)
        for loc in self.CPU_INPUT[1:]:
            if self.element_visible(loc, timeout=2):
                self.send_keys_when_visible(loc, str(cpus), timeout=60)
                break
        self.send_keys_when_visible(self.MEMORY_INPUT[0], memory_gib, timeout=60)
        for loc in self.MEMORY_INPUT[1:]:
            if self.element_visible(loc, timeout=2):
                self.send_keys_when_visible(loc, memory_gib, timeout=60)
                break

    def set_vm_name(self, vm_name: str) -> None:
        name_locs: List[Locator] = [
            ("input[name='name']", By.CSS_SELECTOR),
            ("input#name", By.CSS_SELECTOR),
            ("input[data-test='vm-name-input']", By.CSS_SELECTOR),
        ]
        self.send_keys_when_visible(name_locs[0], vm_name, timeout=60)
        for loc in name_locs[1:]:
            if self.element_visible(loc, timeout=2):
                self.send_keys_when_visible(loc, vm_name, timeout=60)
                break

    def submit_create_virtual_machine(self, vm_name: str, namespace: str) -> None:
        logger.info("Submitting Create Virtual Machine")
        self.click_first_matching(self.BTN_CREATE_VM, timeout=120)
        WebDriverWait(self.driver, 600).until(
            ec.any_of(
                ec.url_contains(f"/ns/{namespace}/"),
                ec.url_contains(vm_name),
                ec.presence_of_element_located(
                    (
                        By.XPATH,
                        "//button[normalize-space()='Running']|//span[normalize-space()='Running']",
                    )
                ),
            )
        )

    def deploy_fedora_vm_from_template(self, cfg: FedoraUITemplateDeployConfig) -> None:
        """
        End-to-end UI workflow with explicit validation checkpoints.
        """
        assert cfg.namespace, "namespace is required"
        assert cfg.storage_class, "storage_class is required"
        assert cfg.vm_name, "vm_name is required"

        self.open_fedora_template_wizard(cfg)
        assert (
            self._disk_or_customize_visible()
        ), "Template wizard did not expose disk/customize controls"

        self.configure_registry_disk(cfg.registry_image)
        self.click_customize_virtual_machine(cfg)

        self.set_vm_name(cfg.vm_name)
        self.open_disks_and_set_rootdisk_storage_class(cfg.storage_class)
        self.yaml_set_password_cloudinit(cfg.cloud_user_password)
        self.set_cpu_and_memory(cfg.cpus, cfg.memory_gib)
        self.submit_create_virtual_machine(cfg.vm_name, cfg.namespace)


def _parse_args(argv: Sequence[str] | None = None):
    p = argparse.ArgumentParser(
        description="Create project (CLI), login to console, deploy Fedora VM from template (UI)."
    )
    p.add_argument(
        "--storage-class",
        required=True,
        help="StorageClass name for rootdisk (any ODF / DR / third-party SC).",
    )
    p.add_argument("--vm-name", default="fedora-ui-vm", help="VirtualMachine name")
    p.add_argument(
        "--namespace",
        default=None,
        help="Target namespace (default: create unique project via CLI)",
    )
    p.add_argument("--registry-image", default="quay.io/containerdisks/fedora:latest")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    ns = args.namespace
    if not ns:
        proj = helpers.create_project()
        ns = proj.namespace
        logger.info("Created project %s", ns)
    cfg = FedoraUITemplateDeployConfig(
        namespace=ns,
        storage_class=args.storage_class,
        vm_name=args.vm_name,
        registry_image=args.registry_image,
    )
    try:
        login_ui()
        page = OCPVirtualizationPage()
        page.driver.get(get_ocp_url())
        page.page_has_loaded(retries=15, sleep_time=1)
        page.deploy_fedora_vm_from_template(cfg)
        logger.info("UI workflow finished for VM %s in %s", cfg.vm_name, cfg.namespace)
    finally:
        close_browser()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
