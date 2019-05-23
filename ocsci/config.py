# Following config variables will be loaded by ocscilib pytest plugin.
# See the conf/ocsci/default_config.yaml for default values.
# The data are combined from multiple sources and then exposed to the following
# variables.
#
# You can see the logic in pytest_customization/plugins/ocscilib.py.

RUN = {}  # all runtime related data
DEPLOYMENT = {}  # deployment related data
REPORTING = {}  # reporting related data
ENV_DATA = {}  # environment related data
