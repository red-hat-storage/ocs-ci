# How to use alternative mirrors/repositories for OCP installer/client download

By default, we pull the OCP installer and client from `https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/`.
Using a combination of the OCP version and OS type we construct the full path to the
location of the binary we want to download.

If you have a reason to pull installer or client images from another repository or mirror,
you can do so by changing the `ocp_url_template` which is used to construct the full path.
This can be especially useful if the default location does not support the arch type of
the system the binary is running on.

For example, if we want to use `https://mirror.openshift.com` as our mirror,
so we might pull `s390x` binaries we could use the following config file and pass it to
ocs-ci through the `--ocsci-conf` CLI argument.

```yaml
DEPLOYMENT:
  ocp_url_template: "https://mirror.openshift.com/pub/openshift-v4/s390x/clients/ocp/{version}/{file_name}-{os_type}-{version}.tar.gz"
```

Note that this is in fact a template used to construct the full location of the installer or client.

Some clarification around the formatting done:
- `{version}` - OCP version
- `{os_type}` - Operating System of the system running ocs-ci
- `{file_name}` - "client" or "install" depending on which binary we are trying to download
