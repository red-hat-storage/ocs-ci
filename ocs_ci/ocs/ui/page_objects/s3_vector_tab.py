import logging
import time

from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab

logger = logging.getLogger(__name__)


class S3VectorTab(BucketsTab):
    """
    Page object for S3 Vector tab operations in the ODF console.
    Handles bucket creation, vector index management, and index detail validation.
    """

    PAGE_LOAD_WAIT = 2
    INDEX_CREATE_WAIT = 3
    _METRIC_LOCATORS = {
        "cosine": "distance_metric_cosine",
        "euclidean": "distance_metric_euclidean",
    }

    def create_vector_bucket_via_obc(
        self, bucket_name: str, storageclass: str, bucketclass_name: str
    ) -> str:
        """
        Create a vector bucket via OBC from the S3 Vector tab.

        Args:
            bucket_name (str): Name for the new OBC/bucket.
            storageclass (str): Storage class name (e.g. openshift-storage.noobaa.io).
            bucketclass_name (str): Vector bucketclass name created by vector_bucket_factory.

        Returns:
            str: The bucket name created.
        """
        logger.info(f"Creating vector bucket '{bucket_name}' via OBC")

        logger.info("Clicking Create bucket button")
        self.do_click(self.bucket_tab["create_bucket_button"], enable_screenshot=True)

        logger.info("Selecting 'Create via Object Bucket Claim'")
        self.do_click(
            self.bucket_tab["create_bucket_button_obc"], enable_screenshot=True
        )

        logger.info(f"Entering bucket name: {bucket_name}")
        self.do_send_keys(self.bucket_tab["obc_bucket_name_input"], bucket_name)

        logger.info(f"Selecting storage class: {storageclass}")
        try:
            self.do_click(self.bucket_tab["storage_class_dropdown"])
            self.do_click(self.bucket_tab["storage_class_noobaa_option"])
        except (NoSuchElementException, TimeoutException):
            logger.warning(
                "Primary storage class dropdown not found, trying OBC locator"
            )
            self.do_click(self.obc_loc["storageclass_dropdown"])
            self.do_send_keys(self.obc_loc["storageclass_text_field"], storageclass)
            self.do_click(self.generic_locators["first_dropdown_option"])

        logger.info(f"Selecting bucketclass: {bucketclass_name}")
        self._select_bucketclass(bucketclass_name)

        logger.info("Submitting OBC creation form")
        time.sleep(1)
        self.do_click(self.bucket_tab["submit_button_obc"], enable_screenshot=True)
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Vector bucket '{bucket_name}' created via OBC")
        return bucket_name

    def create_vector_bucket_via_s3api(
        self, bucket_name: str, namespace_store_name: str
    ) -> str:
        """
        Create a vector bucket directly via the S3 API option from the S3 Vector tab.

        Unlike OBC creation, this does not create an OBC resource. The bucket
        name entered is the NooBaa internal bucket name. An NSFS namespacestore
        must exist on the cluster before calling this method.

        Args:
            bucket_name (str): Name for the new bucket.
            namespace_store_name (str): Name of the NSFS NamespaceStore to back
                the vector bucket (required by the S3 API creation form).

        Returns:
            str: The bucket name created.
        """
        logger.info(f"Creating vector bucket '{bucket_name}' via S3 API")

        logger.info("Clicking Create bucket button")
        self.do_click(self.bucket_tab["create_bucket_button"], enable_screenshot=True)

        logger.info("Selecting 'Create via S3 API'")
        self.do_click(
            self.bucket_tab["create_bucket_button_s3"], enable_screenshot=True
        )

        logger.info(f"Entering bucket name: {bucket_name}")
        self.do_send_keys(self.bucket_tab["s3_bucket_name_input"], bucket_name)

        logger.info(f"Selecting Filesystem NamespaceStore: {namespace_store_name}")
        self.do_click(
            self.bucket_tab["s3_namespace_store_dropdown_toggle"],
            enable_screenshot=True,
        )
        self.do_click(
            format_locator(
                self.bucket_tab["s3_namespace_store_item"], namespace_store_name
            ),
            enable_screenshot=True,
        )

        logger.info("Submitting S3 API bucket creation form")
        time.sleep(1)
        self.do_click(self.bucket_tab["submit_button_s3"], enable_screenshot=True)
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Vector bucket '{bucket_name}' created via S3 API")
        return bucket_name

    def _select_bucketclass(self, bucketclass_name: str):
        """
        Select a bucket class from the dropdown in the OBC creation form.

        Args:
            bucketclass_name (str): Name of the bucket class to select.
        """
        try:
            self.do_click(self.s3_vector_loc["obc_bucketclass_dropdown"])
        except (NoSuchElementException, TimeoutException):
            try:
                self.do_click(self.obc_loc["bucketclass_dropdown"])
            except (NoSuchElementException, TimeoutException):
                raise TimeoutException(
                    f"Bucket class dropdown not found for '{bucketclass_name}'"
                )

        try:
            self.do_send_keys(
                self.s3_vector_loc["obc_bucketclass_input"], bucketclass_name
            )
        except (NoSuchElementException, TimeoutException):
            try:
                self.do_send_keys(
                    self.obc_loc["bucketclass_text_field"], bucketclass_name
                )
            except (NoSuchElementException, TimeoutException):
                raise TimeoutException(
                    f"Bucket class search input not found for '{bucketclass_name}'"
                )

        self.do_click(
            format_locator(self.s3_vector_loc["obc_bucketclass_item"], bucketclass_name)
        )
        logger.info(f"Selected bucketclass: {bucketclass_name}")

    def navigate_to_vector_bucket(self, displayed_name: str):
        """
        Click on a vector bucket name to open its detail page.

        Args:
            displayed_name (str): The NooBaa internal bucket name as shown in the
                S3 Vector tab. For OBC-created buckets this is the value of
                ``OBC(obc_name).bucket_name``; for S3 API buckets it is the name
                entered in the creation form.
        """
        logger.info(f"Navigating to vector bucket: {displayed_name}")
        self.do_click(
            format_locator(
                self.s3_vector_loc["vector_bucket_link_by_name"], displayed_name
            ),
            enable_screenshot=True,
        )
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Opened vector bucket detail page for: {displayed_name}")

    def create_vector_index(
        self,
        index_name: str,
        dimension: int,
        distance_metric: str = "cosine",
        data_type: str = "float32",
    ):
        """
        Create a vector index on the currently open vector bucket detail page.

        Args:
            index_name (str): Name for the new vector index.
            dimension (int): Vector dimension (e.g. 3).
            distance_metric (str): 'cosine' or 'euclidean'. Defaults to 'cosine'.
            data_type (str): Vector data type. Defaults to 'float32'.
        """
        logger.info(
            f"Creating vector index '{index_name}' (dim={dimension}, "
            f"metric={distance_metric}, type={data_type})"
        )

        logger.info("Clicking Create vector index button")
        self.do_click(
            self.s3_vector_loc["create_vector_index_button"], enable_screenshot=True
        )
        self.page_has_loaded(sleep_time=1)

        logger.info(f"Entering index name: {index_name}")
        self.do_send_keys(self.s3_vector_loc["index_name_input"], index_name)

        logger.info(f"Entering dimension: {dimension}")
        dim_locator = self.s3_vector_loc["dimension_input"]
        dim_el = WebDriverWait(self.driver, 30).until(
            ec.visibility_of_element_located((dim_locator[1], dim_locator[0]))
        )
        # The dimension field defaults to 90 (React state). CTRL+A does not
        # reliably select all on <input type="number">, so use the native value
        # setter to set the value and trigger React's onChange handler directly.
        self.driver.execute_script(
            "var s = Object.getOwnPropertyDescriptor("
            "    window.HTMLInputElement.prototype, 'value').set;"
            "s.call(arguments[0], arguments[1]);"
            "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
            dim_el,
            str(dimension),
        )
        logger.info(f"Dimension set to {dimension}")

        logger.info(f"Selecting data type: {data_type}")
        self._select_data_type(data_type)

        logger.info(f"Selecting distance metric: {distance_metric}")
        self._select_distance_metric(distance_metric)

        logger.info("Submitting index creation form")
        self.do_click(
            self.s3_vector_loc["create_index_submit_button"], enable_screenshot=True
        )
        time.sleep(self.INDEX_CREATE_WAIT)
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Vector index '{index_name}' created")

    def _select_distance_metric(self, distance_metric: str):
        """
        Select the distance metric radio button on the index creation form.
        The live UI renders distance metric as radio inputs, not a dropdown.

        Args:
            distance_metric (str): 'cosine' or 'euclidean'.
        """
        locator_key = self._METRIC_LOCATORS.get(distance_metric.lower())
        if locator_key is None:
            raise ValueError(
                f"Unknown distance metric: '{distance_metric}'. "
                f"Supported: {sorted(self._METRIC_LOCATORS)}"
            )
        self.do_click(self.s3_vector_loc[locator_key])
        logger.info(f"Selected distance metric radio button: {distance_metric}")

    def _select_data_type(self, data_type: str):
        """
        Select the data type. In the live UI data type is pre-selected (float32 only),
        so this is a no-op unless a radio/dropdown element is actually present.

        Uses WebDriverWait with a 1-second timeout to avoid mutating the global
        implicit-wait setting on the driver (which would affect concurrent tests).

        Args:
            data_type (str): Data type. Only 'float32' is currently supported.

        Raises:
            ValueError: If an unsupported data_type is requested.
        """
        if data_type != "float32":
            raise ValueError(
                f"Unsupported data_type: '{data_type}'. Only 'float32' is supported."
            )
        locator = self.s3_vector_loc["data_type_float32"]
        try:
            elements = WebDriverWait(self.driver, 1).until(
                ec.presence_of_all_elements_located((locator[1], locator[0]))
            )
        except TimeoutException:
            elements = []

        if elements:
            try:
                elements[0].click()
                logger.info(f"Selected data type: {data_type}")
            except WebDriverException:
                logger.info(
                    f"Data type '{data_type}' appears pre-selected; no action needed."
                )
        else:
            logger.info(
                f"Data type '{data_type}' appears pre-selected; no action needed."
            )

    def navigate_to_index(self, index_name: str):
        """
        Click on a vector index name to open its detail page.

        Args:
            index_name (str): Name of the vector index.
        """
        logger.info(f"Navigating to vector index: {index_name}")
        self.do_click(
            format_locator(self.s3_vector_loc["index_link_by_name"], index_name),
            enable_screenshot=True,
        )
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Opened index detail page for: {index_name}")

    def get_index_detail_text(self, detail_key: str) -> str:
        """
        Get text of a detail field on the index detail page.

        Args:
            detail_key (str): One of 'index_detail_name', 'index_detail_dimension',
                              'index_detail_data_type', 'index_detail_distance_metric',
                              'index_detail_vector_bucket'.

        Returns:
            str: Text value of the detail field (stripped).
        """
        locator = self.s3_vector_loc[detail_key]
        text = self.get_element_text(locator).strip()
        logger.info(f"{detail_key}: {text}")
        return text

    def verify_index_details(
        self,
        index_name: str,
        dimension: int,
        distance_metric: str,
        data_type: str = "float32",
        vector_bucket_name: str = None,
    ) -> dict:
        """
        Verify details shown on the vector index detail page.

        Args:
            index_name (str): Expected index name.
            dimension (int): Expected dimension.
            distance_metric (str): Expected distance metric ('cosine' or 'euclidean').
            data_type (str): Expected data type. Defaults to 'float32'.
            vector_bucket_name (str, optional): Expected vector bucket name.

        Returns:
            dict: Actual values found on the detail page, keyed by field name.

        Raises:
            AssertionError: If any detail does not match expectation.
        """
        actual = {}

        detail_checks = [
            ("index_detail_name", "name", index_name),
            ("index_detail_dimension", "dimension", str(dimension)),
            ("index_detail_data_type", "data_type", data_type),
            ("index_detail_distance_metric", "distance_metric", distance_metric),
        ]

        for locator_key, field, expected in detail_checks:
            value = self.get_index_detail_text(locator_key)
            actual[field] = value
            assert value.strip().lower() == expected.lower(), (
                f"Index detail '{field}' mismatch: "
                f"expected '{expected}', got '{value}'"
            )
            logger.info(f"Verified {field}: '{value}'")

        if vector_bucket_name:
            try:
                bucket_value = self.get_index_detail_text("index_detail_vector_bucket")
                actual["vector_bucket"] = bucket_value
                assert bucket_value.strip() == vector_bucket_name, (
                    f"Vector bucket name mismatch: "
                    f"expected '{vector_bucket_name}', got '{bucket_value}'"
                )
                logger.info(
                    f"Verified vector_bucket: '{bucket_value}' contains '{vector_bucket_name}'"
                )
            except (NoSuchElementException, TimeoutException):
                logger.warning("Could not verify vector bucket name on detail page")

        return actual

    def get_vector_bucket_names_from_tab(self) -> list:
        """
        Get list of vector bucket names shown in the S3 Vector tab.

        Returns:
            list: List of vector bucket name strings.
        """
        try:
            elements = self.get_elements(self.s3_vector_loc["vector_bucket_list_items"])
            names = [el.text.strip() for el in elements if el.text.strip()]
            logger.info(f"Found {len(names)} vector buckets in S3 Vector tab")
            return names
        except (NoSuchElementException, TimeoutException):
            logger.warning("Could not find vector bucket list items")
            return []

    def get_index_names_from_bucket(self) -> list:
        """
        Get list of index names shown on the current vector bucket detail page.

        Returns:
            list: List of index name strings.
        """
        try:
            elements = self.get_elements(self.s3_vector_loc["index_list_items"])
            names = [el.text.strip() for el in elements if el.text.strip()]
            logger.info(f"Found {len(names)} indices on bucket detail page")
            return names
        except (NoSuchElementException, TimeoutException):
            logger.warning("Could not find index list items")
            return []

    def delete_index(self, index_name: str):
        """
        Delete a single vector index by name via the row kebab menu.

        Clicks the kebab button on the index row, selects "Delete index",
        types the index name into the confirmation input, and confirms.

        Args:
            index_name (str): Name of the index to delete.
        """
        logger.info(f"Deleting vector index via UI: {index_name}")
        self.do_click(
            format_locator(self.s3_vector_loc["row_kebab_by_name"], index_name),
            enable_screenshot=True,
        )
        self.do_click(self.s3_vector_loc["delete_index_option"], enable_screenshot=True)
        self.do_send_keys(self.s3_vector_loc["delete_index_name_input"], index_name)
        self.do_click(
            self.s3_vector_loc["delete_index_confirm_button"], enable_screenshot=True
        )
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Deleted vector index: {index_name}")

    def delete_all_indices(self, index_names):
        """
        Delete every index in *index_names* one by one via the row kebab menu.

        The S3 Vector UI has no bulk-select or bulk-delete — each index must be
        deleted individually. The caller is expected to pass the full list of
        index names that are currently present in the bucket.

        Args:
            index_names (list[str]): Names of the indices to delete.
        """
        logger.info(f"Deleting {len(index_names)} indices individually: {index_names}")
        for idx_name in index_names:
            self.delete_index(idx_name)

    def delete_vector_bucket_from_list(self, bucket_name: str):
        """
        Delete a vector bucket from the S3 Vector tab bucket list via the row
        kebab menu.

        Clicks the kebab button on the bucket row, selects "Delete bucket",
        types the bucket name into the confirmation input, and confirms.

        Args:
            bucket_name (str): Name of the vector bucket to delete.
        """
        logger.info(f"Deleting vector bucket via UI: {bucket_name}")
        self.do_click(
            format_locator(self.s3_vector_loc["row_kebab_by_name"], bucket_name),
            enable_screenshot=True,
        )
        self.do_click(
            self.s3_vector_loc["delete_vector_bucket_option"], enable_screenshot=True
        )
        self.do_send_keys(
            self.s3_vector_loc["delete_vector_bucket_name_input"], bucket_name
        )
        self.do_click(
            self.s3_vector_loc["delete_vector_bucket_confirm_button"],
            enable_screenshot=True,
        )
        self.page_has_loaded(sleep_time=self.PAGE_LOAD_WAIT)
        logger.info(f"Deleted vector bucket: {bucket_name}")
