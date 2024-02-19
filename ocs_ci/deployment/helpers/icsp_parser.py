import json
import logging

"""
This module contains the ImageContentSourcePolicy parser
Main task is to parse the ImageContentSourcePolicy cr in format, suitable for
passing 'mirrors to sources' file to 'hcp' or 'hypershift' binaries while creating
hosted cluster.
"""

logger = logging.getLogger(__name__)


class ImageContentSourcePolicy:
    def __init__(self, apiVersion, items, kind=None, metadata=None):
        self.apiVersion = apiVersion
        self.items = [ImageContentItem(**item) for item in items]
        self.kind = kind
        self.metadata = metadata


class ImageContentItem:
    def __init__(self, apiVersion, kind, metadata, spec):
        self.apiVersion = apiVersion
        self.kind = kind
        self.metadata = ImageContentMetadata(**metadata)
        self.spec = ImageContentSpec(**spec)


class ImageContentMetadata:
    def __init__(
        self, annotations, creationTimestamp, generation, name, resourceVersion, uid
    ):
        self.annotations = annotations
        self.creationTimestamp = creationTimestamp
        self.generation = generation
        self.name = name
        self.resourceVersion = resourceVersion
        self.uid = uid


class ImageContentSpec:
    def __init__(self, repositoryDigestMirrors):
        self.repositoryDigestMirrors = [
            RepositoryDigestMirror(**mirror) for mirror in repositoryDigestMirrors
        ]


class RepositoryDigestMirror:
    def __init__(self, mirrors, source):
        self.mirrors = mirrors
        self.source = source


def parse_image_content_source_policy(image_content_source_policy):
    """
    Parse the image content source policy
    :param image_content_source_policy: image content source policy
    :return: ImageContentSourcePolicy object
    """
    return ImageContentSourcePolicy(**image_content_source_policy)


def write_mirrors_to_file(file_path, mirrors_to_source_list):
    """
    Write the mirrors to a file
    :param file_path: file path to write the mirrors to
    :param mirrors_to_source_list: mirrors and their sources to write to the file, list of mirrors and their sources
    """
    result = ""
    for mirrors_source in mirrors_to_source_list:
        result += "- mirrors:\n"
        for mirror in mirrors_source.mirrors:
            result += f"  - {mirror}\n"
        result += f"  source: {mirrors_source.source}\n"
    with open(file_path, "w") as f:
        f.write(result)


def parse_ICSP_json_to_mirrors_file(icsp_json_str, file_path):
    """
    Parse the ImageContentSourcePolicy object to a file
    :param icsp_json_str: ImageContentSourcePolicy CR in json string format
    :param file_path: file path to write the mirrors to
    """
    icsp_json = json.loads(icsp_json_str)
    icsp_obj = parse_image_content_source_policy(icsp_json)
    logger.info("ImageContentSourcePolicy object parsed")
    write_mirrors_to_file(file_path, icsp_obj.items[0].spec.repositoryDigestMirrors)
    logger.info(f"Mirrors were written to file {file_path}")
