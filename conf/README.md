# Config directory

In this directory we store all the configuration for cluster and OCSCI config
files.

During the execution we are loading different config files passed by
--ocsci-conf parameter which we merge together. The last one passed config file
overwrite previous file.

Each of config files can contain different sections (DEFAULTS, ENV_DATA, RUN, etc).

For more information please read the rest of the documentation.

## OCS CI Config

We moved most of the OCSCI framework related config under
[ocsci folder](https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocsci/).

You can pass those config files by `--ocsci-conf` parameter.

## Custom config

If you would like to overwrite cluster default data you can create file
similar to
[this example](https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocs_basic_install.yml).
Example shown overwrites below ENV data:

* `platform` - currently we support only AWS, but in future we will cover
    more.
* `worker_replicas` - number of replicas of worker nodes.
* `master_replicas` - number of replicas of master nodes.

TODO: We have to document all possible parameters!

### Sections in our configs

All of the below sections, will be available from the ocsci config dataclass.

#### RUN

Framework RUN related config parameters. If the parameter is for the complete
run it belongs here.

#### DEPLOYMENT

Deployment related parameters. Only deployment related params not used
anywhere else.

#### REPORTING

Reporting related config. (Do not store secret data in the repository!).

#### ENV_DATA

Environment specific data. This section is meant to be overwritten by own
cluster config file, but can be overwritten also here (But cluster config has
higher priority).

## Example of accessing config/default data

```python
from ocsci import config
from ocs import defaults

# From you code you can access those data like

Taking data from ENV_DATA will always use right cluster_namespace passed via
`--ocsci-conf` config file or default one defined in `default_config.yaml`.
function_which_taking_namespace(namespace=config.ENV_DATA['cluster_namespace'])

# Defaults data you can access like in this example:
print(f"Printing some default data like API version: {defaults.API_VERSION}")
```

## Priority of loading configs:

Lower number == higher priority

1) **CLI args** - sometime we can pass some variables by CLI parameters, in
    this case those arguments should overwrite everything and have the highest
    priority.
2) **cluster config file** - yaml file passed by `--cluster-conf` parameter
3) **ocsci config file** - ocsci related config passed by `--ocsci-conf`
    parameter.
4) **default configuration** - default values and the lowest priority. You can
    see [default config here](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/conf/default_config.yaml).
