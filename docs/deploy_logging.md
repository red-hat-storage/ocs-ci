# Deploy openshift-logging on cluster

## Pre-requisites before starting to deploy openshift-logging

### It is recommended to have minimum m5.xlarge setup.

The images for the openshift-logging need to be pulled from the quay,
sync the images with the internal registry and update the cluster with the new images for containers.


> Note: Steps are to be followed till OCP 4.2 is GAed, These steps can be skipped after that

1. Create a quay.io account [quay.io](https://quay.io)
2. Add your quay username in the spreadsheet given below and wait for the invite sent
   from quay.io ocp-repo maintainers [spreadsheet](https://docs.google.com/spreadsheets/d/1OyUtbu9aiAi3rfkappz5gcq5FjUbMQtJG4jZCNqVT20/edit#gid=0)
3. Generate the quay_token by replacing $USER and $PASSWD with quay username and quay account password,
   ensure you run the command from root user

    ```console
    curl -sH "Content-Type: application/json" -XPOST https://quay.io/cnr/api/v1/users/login -d '
    {
        "user": {
            "username": "'"$USER"'",
            "password": "'"$PASSWD"'"
        }
    }' | jq -r '.token'
    ```
4. Save the token as ```quay.token``` under ocs-ci/data/
5. Now install podman in your local using [link](https://github.com/containers/libpod/blob/master/install.md)
6. To give root privileges for your podman run
    ```
    sudo bash -c 'echo 10000 > /proc/sys/user/max_user_namespaces'
    sudo bash -c "echo $(whoami):110000:65536 > /etc/subuid"
    sudo bash -c "echo $(whoami):110000:65536 > /etc/subgid"
    ```

    Navigate to this [link](https://www.scrivano.org/2018/10/12/rootless-podman-from-upstream-on-centos-7/) for more info
7. Then run the script to deploy openshift-logging on your using
    ```console
    run-ci --no-print-logs --cluster-path /home/username/ tests/e2e/logging/test_openshift-logging.py
    ```

The script is currently under review, will soon be merged.

>Note:
>The openshift-logging deployment will soon be moved along with ocs-ci deployment enabling the user to set an option true/false in a ```conf/ocsci/deployment_logging.yaml```