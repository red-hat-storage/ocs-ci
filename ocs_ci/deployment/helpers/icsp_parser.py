import logging

"""
This module contains the ImageContentSourcePolicy parser
Main task is to parse the ImageContentSourcePolicy cr in format, suitable for
passing 'mirrors to sources' file to 'hcp' or 'hypershift' binaries while creating
hosted cluster.
"""

logger = logging.getLogger(__name__)


class ImageContentSourcePolicy:
    """
    ImageContentSourcePolicy object to parse the ImageContentSourcePolicy CR in dict format to a file
    Main purpose is to use parse_ICSP_json_to_mirrors_file and create a file with mirrors and their sources in format
    required for the 'hcp' or 'hypershift' binaries
    """

    def __init__(self, apiVersion=None, items=None, kind=None, metadata=None, **kwargs):
        self.apiVersion = apiVersion if apiVersion is not None else ""
        self.items = [ImageContentItem(**item) for item in items]
        self.kind = kind if kind is not None else ""
        self.metadata = metadata if metadata is not None else {}
        # process other arguments if needed
        for key, value in kwargs.items():
            setattr(self, key, value)


class ImageContentItem:
    """
    ImageContentItem object to parse the ImageContentSourcePolicy CR in dict format to a file
    """

    def __init__(self, apiVersion=None, kind=None, metadata=None, spec=None, **kwargs):
        self.apiVersion = apiVersion if apiVersion is not None else ""
        self.kind = kind if kind is not None else ""
        self.metadata = ImageContentMetadata(**metadata)
        self.spec = ImageContentSpec(**spec)
        # process other arguments if needed
        for key, value in kwargs.items():
            setattr(self, key, value)


class ImageContentMetadata:
    """
    ImageContentMetadata object to parse the ImageContentSourcePolicy CR in dict format to a file
    """

    def __init__(
        self,
        annotations=None,
        creationTimestamp=None,
        generation=None,
        name=None,
        resourceVersion=None,
        uid=None,
        **kwargs,
    ):
        self.annotations = annotations if annotations is not None else {}
        self.creationTimestamp = (
            creationTimestamp if creationTimestamp is not None else ""
        )
        self.generation = generation if generation is not None else -1
        self.name = name if name is not None else ""
        self.resourceVersion = resourceVersion if resourceVersion is not None else ""
        self.uid = uid if uid is not None else ""
        # process other arguments if needed
        for key, value in kwargs.items():
            setattr(self, key, value)


class ImageContentSpec:
    """
    ImageContentSpec object to parse the ImageContentSourcePolicy CR in dict format to a file
    """

    def __init__(self, repositoryDigestMirrors, **kwargs):
        self.repositoryDigestMirrors = [
            RepositoryDigestMirror(**mirror) for mirror in repositoryDigestMirrors
        ]
        # process other arguments if needed
        for key, value in kwargs.items():
            setattr(self, key, value)


class RepositoryDigestMirror:
    """
    RepositoryDigestMirror object to parse the ImageContentSourcePolicy CR in dict format to a file
    """

    def __init__(self, mirrors=None, source=None, **kwargs):
        self.mirrors = mirrors if mirrors is not None else []
        self.source = source if source is not None else ""
        # process other arguments if needed
        for key, value in kwargs.items():
            setattr(self, key, value)


def parse_image_content_source_policy(image_content_source_policy):
    """
    Parse the image content source policy
    Args:
        image_content_source_policy: image content source policy
    Returns:
         ImageContentSourcePolicy object
    """
    return ImageContentSourcePolicy(**image_content_source_policy)


def write_mirrors_to_file(file_path, mirrors_to_source_list):
    """
    Write the mirrors to a file
    Args:
        file_path: file path to write the mirrors to
        mirrors_to_source_list: mirrors and their sources to write to the file, list of mirrors and their sources
    """
    result = ""
    for mirrors_source in mirrors_to_source_list:
        result += "- mirrors:\n"
        for mirror in mirrors_source.mirrors:
            result += f"  - {mirror}\n"
        result += f"  source: {mirrors_source.source}\n"
    with open(file_path, "a") as f:
        f.write(result)


def parse_ICSP_json_to_mirrors_file(icsp_json_dict, file_path):
    """
    Parse the ImageContentSourcePolicy object to a file
    Args:
        icsp_json_dict: ImageContentSourcePolicy CR in dict format
        file_path: file path to write the mirrors to
    """
    icsp_obj = parse_image_content_source_policy(icsp_json_dict)
    logger.info("ImageContentSourcePolicy object parsed")

    for item in icsp_obj.items:
        logger.info(f"Processing item {item.metadata.name}")
        write_mirrors_to_file(file_path, item.spec.repositoryDigestMirrors)
    logger.info(f"Mirrors were written to file {file_path}")
