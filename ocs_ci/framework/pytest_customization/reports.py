import os
import pytest
import logging
from py.xml import html
from ocs_ci.utility.utils import email_reports
from ocs_ci.framework import config as ocsci_config


@pytest.mark.optionalhook
def pytest_html_results_table_header(cells):
    """
    Add Description header to the table
    """
    cells.insert(2, html.th("Description"))


@pytest.mark.optionalhook
def pytest_html_results_table_row(report, cells):
    """
    Add content to the column Description
    """
    try:
        cells.insert(2, html.td(report.description))
    except AttributeError:
        cells.insert(2, html.td("--- no description ---"))
    # if logs_url is defined, replace local path Log File links to the logs_url
    if ocsci_config.RUN.get("logs_url"):
        for tag in cells[4][0]:
            if (
                hasattr(tag, "xmlname")
                and tag.xmlname == "a"
                and hasattr(tag.attr, "href")
            ):
                tag.attr.href = tag.attr.href.replace(
                    os.path.expanduser(ocsci_config.RUN.get("log_dir")),
                    ocsci_config.RUN.get("logs_url"),
                )


@pytest.mark.hookwrapper
def pytest_runtest_makereport(item, call):
    """
    Add extra column( Log File) and link the log file location
    """
    from pytest_reportportal import RPLogHandler

    pytest_html = item.config.pluginmanager.getplugin("html")
    outcome = yield
    report = outcome.get_result()
    report.description = str(item.function.__doc__)
    extra = getattr(report, "extra", [])

    if report.when == "call":
        if isinstance(logging.getLogger().handlers[1], RPLogHandler):
            log_file = logging.getLogger().handlers[2].baseFilename
        else:
            log_file = logging.getLogger().handlers[1].baseFilename
        extra.append(pytest_html.extras.url(log_file, name="Log File"))
        report.extra = extra
        item.session.results[item] = report
    if report.skipped:
        item.session.results[item] = report
    if report.when in ("setup", "teardown") and report.failed:
        item.session.results[item] = report


def pytest_sessionstart(session):
    """
    Prepare results dict
    """
    session.results = dict()


def pytest_sessionfinish(session, exitstatus):
    """
    send email report
    """
    if ocsci_config.RUN["cli_params"].get("email"):
        email_reports(session)
