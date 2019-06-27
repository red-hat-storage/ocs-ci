import pytest
import logging
from py.xml import html
from utility.utils import email_reports
from ocsci import config as ocsci_config


@pytest.mark.optionalhook
def pytest_html_results_table_header(cells):
    """
    Add Description header to the table
    """
    cells.insert(2, html.th('Description'))


@pytest.mark.optionalhook
def pytest_html_results_table_row(report, cells):
    """
    Add content to the column Description
    """
    cells.insert(2, html.td(report.description))


@pytest.mark.hookwrapper
def pytest_runtest_makereport(item, call):
    """
    Add extra column( Log File) and link the log file location
    """
    pytest_html = item.config.pluginmanager.getplugin('html')
    outcome = yield
    report = outcome.get_result()
    report.description = str(item.function.__doc__)
    extra = getattr(report, 'extra', [])

    if report.when == 'call':
        log_file = logging.getLogger().handlers[1].baseFilename
        extra.append(pytest_html.extras.url(log_file, name='Log File'))
        report.extra = extra


def pytest_sessionfinish(session, exitstatus):
    """
    send email report
    """
    if ocsci_config.RUN['cli_params']['email']:
        email_reports()
