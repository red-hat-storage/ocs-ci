.. _enable-huge-pages:

ENABLE HUGE PAGES
=================

If you need to enable Huge Pages please use this config file:

`conf/ocsci/enable_huge_pages.yaml`

If you pass this file via --ocsci-conf, it will apply file below on your cluster
in order to enable Huge Pages on worker nodes:

`ocs_ci/templates/ocp-deployment/huge_pages.yaml`
