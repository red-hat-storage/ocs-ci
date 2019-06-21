# Scripts Folder

This folder is going to hold our helper shell scripts.

## Our Scripts

### create-patch.sh

This script helps you to create the patch file for the template.
See the [content](./create-patch.sh) of the file what it actually does.
It creates the diff unified patch for provided URL and TEMPLATE file.

#### Required Positional Parameters

* **URL** - address of the original file. (First positional parameter)
* **TEMPLATE** - path to template for which to create the patch file. (Second
  positional parameter)

### apply-patch.sh

This script helps you to apply one specific patch or all patch files found in
the `ocs-ci` repository.

> Be careful if you are running this script and you have some local changes which
> you don't have in the git yet. As the script is removing the template file and
> downloading the new one and applying patch you can lose your changes!

If the first positional parameter with PATCH file path is provided then the
patch is applied only on one file.

If no parameter provided it will find all the `*.patch` files in repo and apply
those patches.

See the [content](./apply-patch.sh) of the script what it actually does.
It downloads latest version of template file (defined on first line of patch)
and apply the provided patch to that file.

#### Optional parameter

* **PATCH FILE** - path to template for which to create the patch file.
