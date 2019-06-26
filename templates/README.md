# Templates

In this folder you can find our templates used in the tests and deployment.

If you are creating some new template from some source like
[rook examples](https://github.com/rook/rook/tree/master/cluster/examples/kubernetes/ceph)
please create the patch file for this template in the same path with the same
name + add `.patch` suffix. You can see one example of patch file
[here](ocs-deployment/cluster.yaml.patch) which was created by
[scripts/create-patch.sh](../scripts/create-patch.sh) script.

It will provide us the way to keep track of changes of our template
files till we will find some better solution.

See the the [documentation](../scripts/README.md) for new scripts for creating
and applying patches.

Once we will have all patch files we can just run the script
[scripts/apply-patch.sh](../scripts/apply-patch.sh) and see what was changed
via `git diff` command and you can send PR with latest changes to our
templates. Advantage is also that if there will be some conflict, the script
`apply-patch.sh` will fail and we will need to solve the conflict.

In case of new changes provided, you need to submit the patch for new changes
and you need to create new patch file to update the last one to be able to
watch new changes!

The intention is to have some job which will run maybe once a day and will send
the notification in case of new changes, so then we cannot manually watch all
the changes.

If no change is done in the template and no need to  it's worth to at least
the mention the source URL of the template or link to documentation in the
comment in the top of the yaml template file. But we can probably create the
patch file which will be without any change, than we can have the automation
for new changes also for those files.
