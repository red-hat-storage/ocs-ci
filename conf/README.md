# Config directory

In this directory we store all the configuration for clusters and OCSCI config
files.

During the execution, via
[ocsci pytest plugin](../pytest_customization/ocscilib.py), we are loading all
the configs described below, combining them togehter and using
[jinja2](http://jinja.pocoo.org/docs/2.10/templates/#variables)
templates rendering variables which you can reuse among different configs from
DEFAULT and different sections.

This gives us the ability to easily reuse variables from context of the
yaml file itself.

Cause you can overwrite data just in specific section like for ENV_DATA
(see below) and if you don't overwrite data in DEFAULT section, someone can
still access old default value from DEFAULT section directly. See example of
[this cluster config file](./examples/ocs_basic_install.yml) for more details.

## OCS CI Config

We moved all the OCSCI framework related config under
[ocsci folder](./ocsci/). You can see we have defined here:

* [default_config.yaml](ocsci/default_config.yaml) - this is the default
    config file for OCS CI which is loaded automatically (don't pass it to
    --ocsci-conf). It contains multiple sections which you can overwrite in
    some custom config file you create or in feature we will have more config
    files defined under `ocsci` folder as well which you can reuse.

## Cluster config

If you would like to overwrite cluster default data you can create file
similar to [this example](./examples/ocs_basic_install.yml). You can see we
can overwrite default data like:

* `platform` - currently we support only AWS, but in future we will cover
    more.
* `worker_replicas` - number of replicas of worker.
* `rook_image` - which rook image can be used.
* and many more - look at the deployment templates under
    [../templates/ocs-deployment/](../templates/ocs-deployment/) and you can
    see the variables in templates. In the deploment we are using ENV_DATA
    section as data for rendering the templates whic allow us to add new
    values just in config and we don't have to also update the code on all
    places to take specific variable.

If you define cluster config you have to put all the data under `ENV_DATA`
section like in the mentioned example. Actually you can add another section as
well but in cluster config file we recommend to define just `ENV_DATA` section
or one more additional section which is `DEFAULTS` for propagation those
defaults among all configs as well.

### Sections in our configs

All of the sections below, except for `DEFAULTS` one, will be available from
[ocsci/config.py](../ocsci/config.py) module. `DEFAULTS` section is exposed to
[ocs/defaults.py](../ocs/defaults.py) module as described below.

#### DEFAULTS

Default values which are used among different configs and sections.

All **capital letters** defined variables are exposed to
[../ocs/defaults.py](../ocs/defaults.py) during the start of the execution.
So you can access those default data from `ocs/defaults.py` module directly.

In this section please put mostly just data which will be used among the other
configuration sections mentioned bellow. **If data are mostly default data
which are NOT going/suppose to be overwritten by config files. Or if the data
are NOT going to be used in any config section at all please put those data
directly to [../ocs/defaults.py](../ocs/defaults.py) module**

#### RUN

Framework RUN related config parameters.

#### DEPLOYMENT

Deployment related parameters. In deplyment we are using some config from this
section but if its ENV specific we are using also `ENV_DATA` section to be
able to reuse in templates.

#### REPORTING

Reporting related config. (Do not store secret data in the repository!).

#### ENV_DATA

This section is meant to be overwritten by own cluster config file, but can be
overwritten also here (But cluster config has higher priority).

## Example of accessing config/default data

```python
from ocsci.config import RUN, ENV_DATA
from ocs import defaults

# From you code you can access those data like

# Taking data from ENV_DATA will alsways using actuall cluster_namespace
# passed via config or default one defined in default_config.yaml.
funciton_which_taking_namespace(namespace=ENV_DATA['cluster_namespace'])

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
    see [default config here](ocsci/default_config.yaml).
5) **ocsci/config.py** - module data are with lowest priority and can be
    overwritten with data mentioned above.
