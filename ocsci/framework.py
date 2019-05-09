import importlib
import logging
import os
import time
import traceback
from pprint import pformat

from ocs.exceptions import UnknownTestStatusException
from utility.polarion import post_to_polarion
from utility.utils import create_unique_test_name, configure_logger, timestamp
from .enums import TestStatus

log = logging.getLogger(__name__)


class TestCase(object):
    """
    Generic test case object to contain configuration details from suite.
    Handles test execution as well as pre and post execution steps
    for Report Portal and Polarion.

    Attributes
        name (str): name of test case
        desc (str): description of test case
        file (str): test module file
        polarion_id (str): polarion test case ID
        abort_on_fail (bool): abort entire run after test failure or not
        suite_name (str): name of test suite test belongs to
        unique_name (str): unique version of test case name
        log_link (str): URL to log file location
        status (TestStatus): Status of test case
        start (Float): start timestamp of test case
        end (Float): end timestamp of test case
        duration (str): duration of test case execution
        test_mod (str): imported python test module
        test_kwargs (dict): arbitrary keyword args passed to test module
        rp_service (ReportPortalServiceAsync): report portal service object
        post_results (bool): post to Polarion after test execution or not
    """
    def __init__(
        self,
        specs,
        suite_name,
        run_dir,
        test_kwargs,
        rp_service,
        post_results=False
    ):
        """
        Initializes the TestCase class with information about the test.

        Args:
            specs (dict): test case specifics pulled from suite file
            suite_name (str): name of the test suite
            run_dir (str): directory where logs are stored
            test_kwargs (dict): arbitrary keyword args passed to test module
            rp_service (ReportPortalServiceAsync): report portal service object
            post_results (bool): post to Polarion after test execution or not
        """

        self.name = specs.get('name')
        self.desc = specs.get('desc')
        self.file = specs.get('module')
        self.polarion_id = specs.get('polarion-id')
        self.abort_on_fail = specs.get('abort-on-fail', False)
        self.suite_name = suite_name
        self.unique_name = create_unique_test_name(self.name)
        self.log_link = configure_logger(self.unique_name, run_dir)
        self.duration = '0s'
        self.status = TestStatus.NOT_EXECUTED
        self.start = time.time()
        self.end = None
        mod_file_name = os.path.splitext(self.file)[0]
        self.test_mod = importlib.import_module(mod_file_name)
        self.test_kwargs = test_kwargs
        self.rp_service = rp_service
        self.post_results = post_results

    def _setup(self):
        """
        Setup method for the TestCase. Any operations that happen before
        the actual test case execution happen here.
        """
        if self.rp_service:
            log.info(
                f"Creating report portal test item for {self.unique_name}"
            )
            self.rp_service.start_test_item(
                name=self.unique_name,
                description=self.desc,
                start_time=timestamp(),
                item_type="STEP"
            )
            self.rp_service.log(
                time=timestamp(),
                message=f"Logfile location: {self.log_link}",
                level="INFO"
            )
            self.rp_service.log(
                time=timestamp(),
                message=f"Polarion ID: {self.polarion_id}",
                level="INFO"
            )

    @staticmethod
    def rc_to_status(rc):
        """
        Transform int unix return code to TestStatus

        Args:
            rc (int): Return code

        Returns:
            Enum: one of Test status from TestStatus Enum. If return code
                differ from what we have defined in TestStatus it returns
                TestStatus.FAILED
        """
        if rc in TestStatus._value2member_map_:
            return TestStatus._value2member_map_[rc]
        return TestStatus.FAILED

    def execute(self):
        """
        Actual test case execution phase.
        Calls the run() function in the appropriate test case module and
        sets values for pertinent attributes upon completion.
        """
        log.info(f"Executing test case: {self.unique_name}")
        try:
            self._setup()
            test_status = self.test_mod.run(**self.test_kwargs)
            if isinstance(test_status, int):
                test_status = self.rc_to_status(test_status)
            if not isinstance(test_status, TestStatus):
                raise UnknownTestStatusException(
                    f"This is unknown Test Status: {test_status}"
                )
            self.status = test_status
        except Exception:
            log.error(traceback.format_exc())
            self.status = TestStatus.FAILED
        finally:
            self._teardown()

    def _teardown(self):
        """
        Teardown method for the TestCase.
        Any final operations that happen after the actual test case
        executes happen here.
        """
        if self.rp_service:
            log.info(
                f"Finishing report portal test item for {self.unique_name}"
            )
            self.rp_service.finish_test_item(
                end_time=timestamp(),
                status=self.status.name
            )
        if self.post_results:
            post_to_polarion(self.__dict__)
            # TODO: update post_to_polarion to look for correct keys
        self.end = time.time()
        self.duration = self.end - self.start
        log.debug(f"Test case info:\n{pformat(self.__dict__)}")
