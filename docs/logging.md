# Logs from execution

## Log location

Default location for logs is under `/tmp` folder.
User can change it by providing own location via config file passed via `--ocsci-conf`:

```yaml
---
RUN:
    log_dir: /my/custom/log/location
```

In the log_dir defined above, the run-ci will create new folder per execution
named in format of: `ocs-ci-logs-$RUN_TYPE-$JENKINS_BUILD_ID-$RUN_ID`.

Where:

- **RUN_TYPE** - can be deployment-ocp , deployment-odf, upgrade, test
- **JENKINS_BUILD_ID** - Jenkins job build ID - which helps easily identify the jenkins build with the log directory
- **RUN_ID** - Unique identifier of the OCS-CI execution.

Under this folder you will find logs:

- **test-info-level.log**: Which contains all INFO level logs from all of the modules
- **pytest_logger**: all DEBUG level logs split per test case via [pytest-logger](https://pytest-logger.readthedocs.io/en/latest/usage.html#logs-dir-layout) plugin

## Console output

Console output is limited to INFO level only for all loggers which has in their name or starts with:

- **tests.** - should limit logs from tests dir only
- **console.logger** - logger defined in: `ocs_ci/utility/logging.py` - for logs messages which you want to have printed out to console output
- **ocs.deployment** - should limit also logs from deployment modules

If you want to have other logs printed out to console output, you can use `console_logger` from: `ocs_ci/utility/logging.py` module as well.

With this approach, we do not spam console log with unnecessary information.
If user is missing INFO level logs from other modules, it can be found in mentioned *test-info-level.log* file.
For the more debug level logs, user should found them under: **pytest_logger** folder.

At the start of each execution we print out the log path message in format like:

```
Logs from run-ci execution (RUN ID: $RUN_ID) for cluster ${i} will be stored in: $LOGS_DIR"
```

In the case of the Jenkins job execution, the `$LOGS_DIR` will be replaced by config.RUN["info_logs_url"] which points to real URL of the logs instead of local path.
