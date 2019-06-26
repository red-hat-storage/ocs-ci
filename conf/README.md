# Config directory

In this directory we store all the configuration for cluster and OCSCI config
files.

During the execution we are loading different config files with different
sections (DEFAULTS, ENV_DATA, RUN, etc).
We are using [jinja2](http://jinja.pocoo.org/docs/2.10/templates/#variables)
template's variables which you can reuse among different configs from
DEFAULT and different sections.

This gives us the ability to easily reuse variables from context of the
yaml file itself.

For more information please read the rest of the documentation.


## OCS CI Config

## defaults file
* [defaults.py](../ocs/defaults.py) - this is main defaults file which is
  a python file, All default values should reside here.

We moved most of the OCSCI framework related config under
[ocsci folder](./ocsci/). * In the future we will have more config files in
this folder which you can reuse.:


## Cluster config

If you would like to overwrite cluster default data you can create file
similar to [this example](./examples/ocs_basic_install.yml). Example shown
overwrites below ENV data:

* `platform` - currently we support only AWS, but in future we will cover
    more.
* `worker_replicas` - number of replicas of worker.
* `rook_image` - which rook image can be used.
* and many more - look at the deployment templates under
    [../templates/ocs-deployment/](../templates/ocs-deployment/) and you can
    see the variables in templates. In the deploment we are using ENV_DATA
    section as data for rendering the templates which allow us to add new
    values just in config and we don't have to also update the code on all
    places to take specific variable.

If you define cluster config you have to put all the data under `ENV_DATA`
section like in the mentioned example. Actually you can add another section as
well but in cluster config file we recommend to define just `ENV_DATA` section
or one more additional section which is `DEFAULTS` for propagation those
defaults among all configs as well.

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

# Taking data from ENV_DATA will always use right cluster_namespace passed via
#`--ocsci-conf` or `--cluster-conf` config files.

# Config data should not be accessed at module level and instead should be
# used within function definitions (as in example below) or fixtures.
function_which_taking_namespace(namespace=None):
    namespace = namespace if namespace else config.ENV_DATA[
        'cluster_namespace'
    ]

# Defaults data you can access like in this example:
print(f"Printing some default data like API version: {defaults.SOME_VAR}")
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
    see [default config here](ocsci/default_config.yaml).
