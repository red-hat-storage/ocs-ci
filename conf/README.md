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

We moved all the OCSCI framework related config under
[ocsci folder](./ocsci/). We have defined currently this file:

* [default_config.yaml](ocsci/default_config.yaml) - this is the default
    config file for OCS CI which is loaded automatically (don't pass it to
    --ocsci-conf). It contains multiple sections which user can overwrite in
    custom file and provide as an option to `--oscci-conf`.
* In the future we will have more config files in this folder which you can
    reuse.

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

All of the below sections, except for `DEFAULTS`, will be available from
[ocsci/config.py](../ocsci/config.py) module. `DEFAULTS` section is exposed to
[ocs/defaults.py](../ocs/defaults.py) module as described below.

#### DEFAULTS

Default values which are used among different configs and sections.

All variables defined in this section are exposed to
[../ocs/defaults.py](../ocs/defaults.py) module during the start of the
execution. So you can access those default data from `ocs/defaults.py` module
directly from your tests or modules.

#### RUN

Framework RUN related config parameters. If the paremeter is for whole run
it belongs here.

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
    see [default config here](ocsci/default_config.yaml).
5) **ocsci/config.py** - module data are with lowest priority and can be
    overwritten with data mentioned above.
