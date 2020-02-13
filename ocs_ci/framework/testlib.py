from ocs_ci.framework.pytest_customization.marks import *  # noqa: F403
from functools import wraps


@pytest.mark.usefixtures(  # noqa: F405
    'run_io_in_background', 'environment_checker'
)
class BaseTest:
    """
    Base test class for our testing.
    If some functionality/property needs to be implemented in all test classes
    here is the place to put your code.
    """
    pass


@e2e  # noqa: F405
class E2ETest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@manage  # noqa: F405
class ManageTest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@ecosystem  # noqa: F405
class EcosystemTest(BaseTest):
    """
    Base class for E2E team
    """
    pass


def skipif_ocs_version(expr_list):
    """
    This is a decorator to skip the test if condition evaluates to
    true based on expression

    Args:
        expr_list (list): condition for which we need to check,
            eg: ['<4.3', '>4.2'], ['<=4.3', '>=4.2']

    Return:
        decorated function
    """
    skip_this = True
    for expr in expr_list:
        comparision_str = config.ENV_DATA['ocs_version'] + expr
        skip_this = skip_this and eval(comparision_str)
    # skip_this will be either True or False after eval

    def wrapper(func):
        @wraps(func)
        @pytest.mark.skipif(
            skip_this, reason=f'Condition not satisfied {expr_list}'
        )
        def wrapped(*args, **kwargs):
            func(*args, **kwargs)
        return wrapped
    return wrapper
