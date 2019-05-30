# Following config variables will be loaded by ocscilib pytest plugin.
# See the conf/ocsci/default_config.yaml for default values.
# The data are combined from multiple sources and then exposed to the following
# variables.
#
# You can see the logic in ./pytest_customization/plugins/ocscilib.py.

# Those have lowest priority and are overwritten and filled with data loaded
# during the config phase.
RUN = dict(cli_params={})  # this will be filled with CLI parameters data
DEPLOYMENT = {}  # deployment related data
REPORTING = {}  # reporting related data
ENV_DATA = {}  # environment related data

CONFIG = {
    'RUN': RUN,
    'DEPLOYMENT': DEPLOYMENT,
    'REPORTING': REPORTING,
    'ENV_DATA': ENV_DATA,
}
