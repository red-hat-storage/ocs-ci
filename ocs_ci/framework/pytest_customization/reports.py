import os
import pytest
import logging
from py.xml import html
from ocs_ci.utility.utils import email_reports, save_reports
from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework import globalVariables as globalVariables


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
    pytest_html = item.config.pluginmanager.getplugin("html")
    outcome = yield
    report = outcome.get_result()
    report.description = str(item.function.__doc__)
    extra = getattr(report, "extra", [])

    if report.when == "call":
        log_file = ""
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                log_file = handler.baseFilename
                break
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
    save session's report files and send email report
    """
    if ocsci_config.REPORTING.get("save_mem_report"):
        save_reports()
    if ocsci_config.RUN["cli_params"].get("email"):
        email_reports(session)

    # creating report of test cases with total time in ascending order
    data = globalVariables.TIMEREPORT_DICT
    sorted_data = dict(sorted(data.items(), key=lambda item: item[1]["total"]))
    with open("time_report.txt", "a") as f:
        f.write("testName\tsetup\tcall\tteardown\ttotal\n")
        for test, values in sorted_data.items():
            row = (
                f"{test}\t{values.get('setup', 'NA')}\t"
                f"{values.get('call', 'NA')}\t"
                f"{values.get('teardown', 'NA')}\t"
                f"{values.get('total', 'NA'):.2f}\n"
            )
            f.write(row)


def pytest_report_teststatus(report, config):
    """
    This function checks the status of the test at which stage it is at an calculates
    the time take by each stage to complete it.
    There are three stages:
    setup : when the test case is setup
    call : when the test case is run
    teardown: when the teardown of the test case happens.
    """
    globalVariables.TIMEREPORT_DICT[
        report.nodeid
    ] = globalVariables.TIMEREPORT_DICT.get(report.nodeid, {})

    if report.when == "setup":
        print(
            f"duration reported by {report.nodeid} immediately after test execution: {round(report.duration, 2)}"
        )
        globalVariables.TIMEREPORT_DICT[report.nodeid]["setup"] = round(
            report.duration, 2
        )
        globalVariables.TIMEREPORT_DICT[report.nodeid]["total"] = round(
            report.duration, 2
        )

    if report.when == "call":
        print(
            f"duration reported by {report.nodeid} immediately after test execution: {round(report.duration, 2)}"
        )
        globalVariables.TIMEREPORT_DICT[report.nodeid]["call"] = round(
            report.duration, 2
        )
        globalVariables.TIMEREPORT_DICT[report.nodeid]["total"] += round(
            report.duration, 2
        )

    if report.when == "teardown":
        print(
            f"duration reported by {report.nodeid} immediately after test execution: {round(report.duration, 2)}"
        )
        globalVariables.TIMEREPORT_DICT[report.nodeid]["teardown"] = round(
            report.duration, 2
        )
        globalVariables.TIMEREPORT_DICT[report.nodeid]["total"] += round(
            report.duration, 2
        )
