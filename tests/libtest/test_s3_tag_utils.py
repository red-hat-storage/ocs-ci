from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.ocs.bucket_utils import (
    delete_object_tags,
    get_object_to_tags_dict,
    tag_objects,
    write_random_test_objects_to_bucket,
)


@libtest
def test_s3_tags_util_funcs(
    mcg_obj, bucket_factory, awscli_pod_session, test_directory_setup
):
    """
    Test bucket_utils.py::tag_objects, get_object_to_tags_dict, delete_object_tags

    """
    bucket = bucket_factory()[0].name
    tagA = {"keyA": "valueA"}
    tagB = {"keyB": "valueB"}

    objects = write_random_test_objects_to_bucket(
        io_pod=awscli_pod_session,
        bucket_to_write=bucket,
        file_dir=test_directory_setup.origin_dir,
        amount=10,
        mcg_obj=mcg_obj,
    )
    # NOTE: the first object is left untagged to test an edge-case of
    # get_object_to_tags_dict and delete_object_tags
    tag_objects(
        awscli_pod_session,
        mcg_obj,
        bucket=bucket,
        object_keys=objects[1:],
        tags=[tagA, tagB],
    )

    obj_to_tags_output = get_object_to_tags_dict(
        awscli_pod_session,
        mcg_obj,
        bucket=bucket,
        object_keys=objects,
    )
    for obj, tags in obj_to_tags_output.items():
        if obj == objects[0]:
            continue
        assert tagA in tags
        assert tagB in tags

    delete_object_tags(
        awscli_pod_session,
        mcg_obj,
        bucket=bucket,
        object_keys=objects,
    )
    obj_to_tags_dict = get_object_to_tags_dict(
        awscli_pod_session,
        mcg_obj,
        bucket=bucket,
        object_keys=objects,
    )
    for tags_dict in obj_to_tags_dict.values():
        assert len(tags_dict) == 0
