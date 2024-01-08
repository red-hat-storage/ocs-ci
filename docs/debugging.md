.. _debugging:

Debugging
===========

In this document you will find useful information for the debugging.

## SSH to the host

If you would like to ssh to the host and your cluster is running on AWS where
you don't have your nodes exposed to the public network you can ssh to the hosts with the
ssh bastion pod. See this [repository](https://github.com/eparis/ssh-bastion) for
more details.

1) make sure you are logged to OCP or have exported `KUBECONFIG`
1) optional step:

    ```console
    export SSH_BASTION_NAMESPACE=openshift-ssh-bastion
    ```

    > openshift-ssh-bastion is used by default

1) run the following command for deploy bastion pod:

    ```console
    curl https://raw.githubusercontent.com/eparis/ssh-bastion/master/deploy/deploy.sh | bash
    ```

1) the bastion address can be found by running:

    ```console
    oc get service -n openshift-ssh-bastion ssh-bastion -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
    ```

    > The address was also printed out by previous command.

1) find the ip of the node you would like to connect to by running:

    ```console
    oc get node -o wide
    ```

1) connect to the node by:

    ```console
    ssh -i ~/.ssh/openshift-dev.pem -t -o StrictHostKeyChecking=no -o ProxyCommand='ssh -A -i ~/.ssh/openshift-dev.pem -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -W %h:%p core@$(oc get service --all-namespaces -l run=ssh-bastion -o jsonpath="{.items[0].status.loadBalancer.ingress[0].hostname}")' core@10.0.130.69 "sudo -i"
    ```

    > in case you didn’t use the same location of openshift-dev.pem key or you used different one during OCP deployment you have to change the path to the other one.

## OC debug

if you don't need to connect via ssh you can use `oc debug` command.

```console
oc debug node/NODE_NAME
```

This will create temporary pod on the specified node and you have to run
following command after the previous mentioned one.

```console
chroot /host
```

Now you can start running commands on the node.


## Enable debug log level for rook-ceph-operator

To enable DEBUG log level for rook-ceph-operator you can pass:

`--ocsci-conf conf/ocsci/set_debug_rook_log_level.yaml`

This will update the config map to change the log level.
This happens after subscription to the OCS/ODF and before creating the storage
cluster CR.
