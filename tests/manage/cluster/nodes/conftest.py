import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.utility import aws


@pytest.fixture()
def aws_obj():
    """
    Initialize AWS instance

    Returns:
        AWS: An instance of AWS class

    """
    aws_obj = aws.AWS()
    return aws_obj


@pytest.fixture()
def instances(request, aws_obj):
    """
    Get cluster instances

    Returns:
        dict: The ID keys and the name values of the instances

    """
    # Get all cluster nodes objects
    nodes = node.get_node_objs()

    # Get the cluster nodes ec2 instances
    ec2_instances = aws.get_instances_ids_and_names(nodes)

    def finalizer():
        """
        Make sure all instances are running
        """
        # Getting the instances that are in status 'stopping' (if there are any), to wait for them to
        # get to status 'stopped' so it will be possible to start them
        stopping_instances = {
            key: val for key, val in ec2_instances.items() if (
                aws_obj.get_instances_status_by_id(key) == constants.INSTANCE_STOPPING
            )
        }

        # Waiting fot the instances that are in status 'stopping'
        # (if there are any) to reach 'stopped'
        if stopping_instances:
            for stopping_instance in stopping_instances:
                instance = aws_obj.get_ec2_instance(stopping_instance.key())
                instance.wait_until_stopped()
        stopped_instances = {
            key: val for key, val in ec2_instances.items() if (
                aws_obj.get_instances_status_by_id(key) == constants.INSTANCE_STOPPED
            )
        }

        # Start the instances
        if stopped_instances:
            aws_obj.start_ec2_instances(instances=stopped_instances, wait=True)

    request.addfinalizer(finalizer)

    return ec2_instances


@pytest.fixture()
def resources(request):
    """
    Delete the resources created during the test

    Returns:
        tuple: empty lists of resources

    """
    # Initialize the resources empty lists, to be filled during the test
    # and deleted on teardown
    projects, secrets, pools, storageclasses, pvcs, pods = ([] for i in range(6))

    def finalizer():
        """
        Delete the resources created during the test
        """
        for resource_type in pods, pvcs, storageclasses, secrets:
            for resource in resource_type:
                resource.delete()
                resource.ocp.wait_for_delete(resource.name)
        if pools:
            # Delete only the RBD pool
            pools[0].delete()
        if projects:
            for project in projects:
                project.delete(resource_name=project.namespace)
                project.wait_for_delete(project.namespace)

    request.addfinalizer(finalizer)

    return projects, secrets, pools, storageclasses, pvcs, pods
