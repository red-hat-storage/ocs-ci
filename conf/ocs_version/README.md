# OCS Version Config Files

These files are intended to be used internally by the `--ocs-version` option
and not by users through the `--ocsci-conf` option. The reasoning for this
has to do with the order in which these configuration arguments are loaded
and this ensures that we execute with the correct configuration.
