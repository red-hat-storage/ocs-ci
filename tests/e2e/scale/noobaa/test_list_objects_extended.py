import random


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


#
# def setup_dir_struct_and_upload_objects(
#     io_pod, objects_path, h_level, v_level, pref_len
# ):
#     if v_level == 0:
#         v_level = 1
#     if h_level == 0:
#         h_level = 1
#     all_prefixes = []
#     for _ in range(v_level * h_level):
#         all_prefixes.append(generate_random_unicode_prefix(pref_len))
#
#     num_of_objs = 100
#     objs_per_pref = num_of_objs // (v_level * h_level)
#     rem = num_of_objs % (v_level * h_level)
#
#     for _ in range(num_of_objs):
#         pass
#
#
# class TestListObjectsExtended:
#     @pytest.mark.parametrize(
#         argnames=["bucketclass", "h_level", "v_level"],
#         argvalues=[
#             pytest.param(
#                 {
#                     "interface": "OC",
#                     "backingstore_dict": {"aws": [(1, "eu-central-1")]},
#                 },
#                 5,
#                 1,
#             ),
#         ],
#         ids=[
#             "AWS-Data",
#         ],
#     )
#     def test_list_small_small(self, bucket_factory, bucketclass, h_level, v_level):
#
#         bucket = bucket_factory(bucketclass=bucketclass, amount=1)
