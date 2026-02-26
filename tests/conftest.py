    def finalizer():
        for instance in instances:
            try:
                sc_data = instance.ocp.get(resource_name=instance.name)
                if sc_data.get("metadata", {}).get("ownerReferences"):
                    log.info(
                        f"Removing ownerReferences from StorageClass "
                        f"{instance.name} before deletion"
                    )
                    instance.ocp.patch(
                        resource_name=instance.name,
                        params='{"metadata": {"ownerReferences": null}}',
                        format_type="merge",
                    )
            except CommandFailed:
                log.warning(
                    f"Failed to remove ownerReferences from StorageClass "
                    f"{instance.name}, proceeding with deletion anyway"
                )
            instance.delete()
            instance.ocp.wait_for_delete(instance.name, timeout=120)

    request.addfinalizer(finalizer)
    return factory