import os
import pytest
import logging
from py.xml import html
from ocs_ci.utility.utils import (
    dump_config_to_file,
    email_reports,
    save_reports,
    ocsci_log_path,
)
from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework import GlobalVariables as GV

logger = logging.getLogger(__name__)


log = logging.getLogger(__name__)


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
    import csv

    if ocsci_config.REPORTING.get("save_mem_report"):
        save_reports()
    if ocsci_config.RUN["cli_params"].get("email"):
        email_reports(session)

    # creating report of test cases with total time in ascending order
    data = GV.TIMEREPORT_DICT
    sorted_data = dict(
        sorted(data.items(), key=lambda item: item[1].get("total"), reverse=True)
    )
    try:
        time_report_file = os.path.join(
            ocsci_log_path(), "session_test_time_report_file.csv"
        )
        with open(time_report_file, "a") as fil:
            c = csv.writer(fil)
            c.writerow(["testName", "setup", "call", "teardown", "total"])
            for test, values in sorted_data.items():
                row = [
                    test,
                    values.get("setup", "NA"),
                    values.get("call", "NA"),
                    values.get("teardown", "NA"),
                    values.get("total", "NA"),
                ]
                c.writerow(row)
        logger.info(f"Test Time report saved to '{time_report_file}'")
    except Exception as e:
        logger.warning(
            f"Failed to save Test Time report to logs directory with exception. {e}"
        )

    for i in range(ocsci_config.nclusters):
        ocsci_config.switch_ctx(i)
        if not (
            ocsci_config.RUN["cli_params"].get("--help")
            or ocsci_config.RUN["cli_params"].get("collectonly")
        ):
            config_file = os.path.expanduser(
                os.path.join(
                    ocsci_config.RUN["log_dir"],
                    f"run-{ocsci_config.RUN['run_id']}-cl{i}-config-end.yaml",
                )
            )
            dump_config_to_file(config_file)
            log.info(f"Dump of the consolidated config is located here: {config_file}")


def pytest_report_teststatus(report, config):
    """
    This function checks the status of the test at which stage it is at an calculates
    the time take by each stage to complete it.
    There are three stages:
    setup : when the test case is setup
    call : when the test case is run
    teardown: when the teardown of the test case happens.
    """
    GV.TIMEREPORT_DICT[report.nodeid] = GV.TIMEREPORT_DICT.get(report.nodeid, {})

    if report.when == "setup":
        setup_duration = round(report.duration, 2)
        logger.info(
            f"duration reported by {report.nodeid} immediately after test execution: {setup_duration}"
        )
        GV.TIMEREPORT_DICT[report.nodeid]["setup"] = setup_duration
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = setup_duration

    if "total" not in GV.TIMEREPORT_DICT[report.nodeid]:
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = 0

    if report.when == "call":
        call_duration = round(report.duration, 2)
        logger.info(
            f"duration reported by {report.nodeid} immediately after test execution: {call_duration}"
        )
        GV.TIMEREPORT_DICT[report.nodeid]["call"] = call_duration
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = round(
            GV.TIMEREPORT_DICT[report.nodeid]["total"] + call_duration, 2
        )

    if report.when == "teardown":
        teardown_duration = round(report.duration, 2)
        logger.info(
            f"duration reported by {report.nodeid} immediately after test execution: {teardown_duration}"
        )
        GV.TIMEREPORT_DICT[report.nodeid]["teardown"] = teardown_duration
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = round(
            GV.TIMEREPORT_DICT[report.nodeid]["total"] + teardown_duration, 2
        )
