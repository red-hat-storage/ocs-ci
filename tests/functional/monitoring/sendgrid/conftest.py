import logging

import pytest

from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@pytest.fixture
def notification_emails_required():
    """
    Return emails that are set as notification emails for ODF Managed Service
    add-on. If there are no emails set then xfail the test.

    Returns:
        list: list of notification emails

    """
    emails = [
        config.REPORTING.get(parameter)
        for parameter in [
            "notification_email_0",
            "notification_email_1",
            "notification_email_2",
        ]
    ]
    if not any(emails):
        pytest.xfail("No notification emails were set in ODF Managed Service add-on")
    return emails
