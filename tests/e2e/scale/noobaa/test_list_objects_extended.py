import pytest
import random
import logging

from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    list_objects_from_bucket,
    s3_list_objects_v2,
)

logger = logging.getLogger(__name__)


def generate_random_unicode_prefix(number_of_chars=3):
    unicode_blocks = [
        (0x0020, 0x007E),  # Basic Latin
        (0x00A0, 0x00FF),  # Latin-1 Supplement
        (0x0100, 0x017F),  # Latin Extended-A
        (0x0180, 0x024F),  # Latin Extended-B
        (0x0250, 0x02AF),  # IPA Extensions
        (0x0391, 0x03A1),
        (0x03A3, 0x03A9),
        (0x03B1, 0x03C1),
        (0x03C3, 0x03C9),  # Greek and Coptic
        (0x0410, 0x042F),
        (0x0430, 0x044F),  # Cyrillic
        (0x0531, 0x0556),  # Armenian
        (0x05D0, 0x05EA),  # Hebrew
        (0x0621, 0x063A),
        (0x0641, 0x064A),  # Arabic
        (0x0905, 0x0939),  # Devanagari
        (0x4E00, 0x9FFF),  # CJK Unified Ideographs (Chinese)
        (0x3040, 0x309F),  # Hiragana (Japanese)
        (0x30A0, 0x30FF),  # Katakana (Japanese)
        (0x3041, 0x3096),  # Additional Japanese kana
        (0x4E00, 0x9FFF),  # CJK Unified Ideographs (Chinese)
        (0x3400, 0x4DBF),  # Extension A (Chinese)
        (0x2000, 0x206F),  # General Punctuation
        (0x2190, 0x21FF),  # Arrows
        (0x2200, 0x22FF),  # Mathematical Operators
        (0x25A0, 0x25FF),  # Geometric Shapes
        (0x2600, 0x26FF),  # Miscellaneous Symbols
    ]
    prefix = []
    for _ in range(number_of_chars):
        start, end = random.choice(unicode_blocks)
        prefix.append(chr(random.randint(start, end)))
    return "".join(prefix)


def get_number_of_objs(io_pod, objects_path):
    return int(io_pod.exec_sh_cmd_on_pod(command=f"ls -ltr {objects_path} | wc -l")) - 1


def make_dirs(io_pod):

    uploaded_dir = "/data/uploaded_dir"
    uploading_dir = "/data/uploading_dir"

    io_pod.exec_sh_cmd_on_pod(
        command=f"mkdir -p {uploading_dir} && mkdir -p {uploaded_dir}"
    )
    return uploading_dir, uploaded_dir


def setup_dir_struct_and_upload_objects(
    io_pod, s3_obj, bucket_name, objects_path, h_level, v_level, pref_len=5
):
    if v_level == 0:
        v_level = 1
    if h_level == 0:
        h_level = 1

    uploading_dir, uploaded_dir = make_dirs(io_pod)

    all_prefixes = []
    for _ in range(v_level * h_level):
        all_prefixes.append(generate_random_unicode_prefix(pref_len))

    num_of_objs = get_number_of_objs(io_pod, objects_path)
    logger.info(f"Total number of objects: {num_of_objs}")

    objs_per_pref = num_of_objs // (v_level * h_level)
    rem = num_of_objs % (v_level * h_level)

    for i in range(v_level):
        pref_str = ""
        for j in range(h_level):
            pref = generate_random_unicode_prefix(pref_len)
            pref_str += pref + "/"
            logger.info(
                f"uploading {objs_per_pref} onto {bucket_name} with prefix {pref_str}"
            )
            if i == v_level - 1 and j == h_level - 1:
                n_obj = objs_per_pref + rem
            else:
                n_obj = objs_per_pref

            objs = io_pod.exec_sh_cmd_on_pod(
                command=f"ls -ltr {objects_path}| tail -n +2 | head -{n_obj} | awk '{{print $9}}'"
            ).split()
            logger.info(objs)
            for obj in objs:
                io_pod.exec_sh_cmd_on_pod(
                    command=f"mv {objects_path}/{obj} {uploading_dir}/"
                )
            sync_object_directory(
                io_pod,
                f"{uploading_dir}/",
                f"s3://{bucket_name}/{pref_str}",
                s3_obj=s3_obj,
            )
            io_pod.exec_sh_cmd_on_pod(command=f"mv {uploading_dir}/* {uploaded_dir}")


class TestListObjectsExtended:
    @pytest.mark.parametrize(
        argnames=["bucketclass", "h_level", "v_level"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                5,
                2,
            ),
        ],
        ids=[
            "AWS-Data",
        ],
    )
    def test_list_small_small(
        self,
        scale_cli_v2_pod,
        bucket_factory,
        mcg_obj_session,
        bucketclass,
        h_level,
        v_level,
    ):

        bucket = bucket_factory(bucketclass=bucketclass, amount=1)[0]
        logger.info(f"bucket created: {bucket.name}")

        setup_dir_struct_and_upload_objects(
            scale_cli_v2_pod,
            mcg_obj_session,
            bucket.name,
            "/data/small_small",
            h_level,
            v_level,
        )

        # List all the objects recursively
        ls_objs = list_objects_from_bucket(
            scale_cli_v2_pod, bucket.name, recursive=True, s3_obj=mcg_obj_session
        )
        assert len(ls_objs) == 100, "Not all the objects are listed"

        # List objects specific to some random prefix,delimiter combination

        # List all the objects in a directory
        sync_object_directory(
            scale_cli_v2_pod, "/data/small_small/", bucket.name, s3_obj=mcg_obj_session
        )
        s3_list_objects_v2(mcg_obj_session, bucket.name)
