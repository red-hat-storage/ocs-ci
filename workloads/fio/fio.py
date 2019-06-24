import logging


# Log file will be on pod
log = logging.getLogger(__name__)


def setup(**kwargs):
    """
    setup fio workload

    Args:
        **kwargs (dict): fio setup configuration

    Returns:
        bool: True if setup succeeds else False
    """
    io_pod = kwargs['pod']
    # For first cut doing simple fio install
    # TODO: we can use fio container image
    cmd = "yum -y install fio"
    return io_pod.exec_cmd_on_pod(cmd)


def run(**kwargs):
    """
    Run fio with params from kwargs

    Args:
        kwargs (dict): params for fio

    Result:
        result of command
    """
    io_pod = kwargs.pop('pod')
    st_type = kwargs.pop('type')
    path = kwargs.pop('path')

    fio_cmd = "fio"
    args = ""
    for k, v in kwargs.items():
        if k == 'filename':
            if st_type == 'fs':
                args = args + f" --{k}={path}/{v}"
        args = args + f" --{k}={v}"
    fio_cmd = fio_cmd + args

    return io_pod.exec_cmd_on_pod(fio_cmd)
