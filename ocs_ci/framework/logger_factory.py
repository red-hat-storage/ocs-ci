import logging
from ocs_ci.framework import config

current_factory = logging.getLogRecordFactory()


def record_factory(*args, **kwargs):
    """
    Record factory setup function
    Return:
        logging.record: Reference obj to the record of logger
    """
    record = current_factory(*args, **kwargs)
    # Customize the log format for cluster context:
    record.clusterctx = f"- C[{config.cur_index}]" if config.nclusters > 1 else ""

    return record


def set_log_record_factory():
    """
    Custom attribute additions to logging are addressed in this function
    Override the logging format with a new log record factory
    Return:
        None
    """
    logging.setLogRecordFactory(record_factory)
