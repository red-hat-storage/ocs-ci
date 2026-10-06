# -*- coding: utf8 -*-

"""
Unit tests for the catalog parsing and z-n version pinning helpers used by
disconnected deployments.
"""

import io
import json

import pytest

from ocs_ci.deployment import disconnected
from ocs_ci.ocs.exceptions import CommandFailed, NotFoundError


def encode_pretty(*documents):
    """
    Encode documents the way `opm render --output=json` does: each one
    pretty-printed over many lines, concatenated with no separator.

    Args:
        *documents: JSON-serializable objects

    Returns:
        bytes: concatenated pretty-printed JSON
    """
    encoder = json.JSONEncoder(indent=4)
    return "\n".join(encoder.encode(document) for document in documents).encode()


def build_catalog_documents(
    package, versions, channel, default_channel=None, other_minor=()
):
    """
    Build the olm.package/olm.bundle/olm.channel documents of a single package.

    Args:
        package (str): OLM package name
        versions (list): versions carried by ``channel``
        channel (str): channel name the versions are listed in
        default_channel (str): package defaultChannel, defaults to ``channel``
        other_minor (list): extra bundle versions not listed in any channel,
            used to emulate the cumulative catalog carrying a prior minor

    Returns:
        list: declarative-config documents
    """
    documents = [
        {
            "schema": "olm.package",
            "name": package,
            "defaultChannel": default_channel or channel,
        }
    ]
    for version in list(versions) + list(other_minor):
        documents.append(
            {
                "schema": "olm.bundle",
                "name": f"{package}.v{version}",
                "package": package,
                # a string array is what broke the original line-based parser:
                # such a line decodes to a bare str, not a dict
                "properties": [{"type": "olm.package", "value": {"version": version}}],
            }
        )
    documents.append(
        {
            "schema": "olm.channel",
            "name": channel,
            "package": package,
            "entries": [
                {"name": f"{package}.v{version}", "skips": [f"{package}.v0.0.1"]}
                for version in versions
            ],
        }
    )
    return documents


@pytest.fixture
def render(monkeypatch):
    """
    Patch `opm render` out: instead of spawning the binary, feed
    _render_catalog the given documents as pretty-printed stdout.

    Returns:
        callable: takes documents, returns the parsed catalog dict
    """

    def _render(documents, packages):
        monkeypatch.setattr(disconnected, "get_opm_tool", lambda *_, **__: None)

        class FakePopen:
            returncode = 0

            def __init__(self, *_, **__):
                self.stdout = io.BytesIO(encode_pretty(*documents))
                self.pid = 0

            def wait(self, *_, **__):
                return 0

        monkeypatch.setattr(disconnected.subprocess, "Popen", FakePopen)
        return disconnected._render_catalog("index:v4.20", packages)

    return _render


class TestIterJsonDocuments:
    """
    `opm render --output=json` emits pretty-printed concatenated JSON, not
    NDJSON, so the parser must not be line based.
    """

    def test_pretty_printed_documents(self):
        documents = [{"schema": "olm.bundle", "name": f"p.v{i}"} for i in range(5)]
        stream = io.BytesIO(encode_pretty(*documents))
        assert list(disconnected._iter_json_documents(stream)) == documents

    def test_string_array_element_does_not_break_parsing(self):
        """
        Regression test: a line holding only a string-array element used to be
        parsed on its own and decoded to a bare str, raising
        "AttributeError: 'str' object has no attribute 'get'".
        """
        document = {"schema": "olm.channel", "skips": ["p.v1", "p.v2"]}
        raw = encode_pretty(document)
        assert b'\n        "p.v1"' in raw, "test input is not pretty-printed"
        assert list(disconnected._iter_json_documents(io.BytesIO(raw))) == [document]

    def test_ndjson_is_still_supported(self):
        documents = [{"a": 1}, {"b": 2}]
        raw = "\n".join(json.dumps(document) for document in documents).encode()
        assert list(disconnected._iter_json_documents(io.BytesIO(raw))) == documents

    @pytest.mark.parametrize("chunk_size", [1, 7, 64, 4096])
    def test_document_spanning_chunks(self, chunk_size):
        document = {"schema": "olm.bundle", "csv": "x" * 20000}
        stream = io.BytesIO(encode_pretty(document))
        parsed = list(disconnected._iter_json_documents(stream, chunk_size=chunk_size))
        assert parsed == [document]

    @pytest.mark.parametrize("chunk_size", [7, 13, 1024])
    def test_multibyte_character_split_across_chunks(self, chunk_size):
        """A fixed-size read may cut a UTF-8 character in half."""
        document = {"schema": "olm.package", "description": "ODF — ünicode ✓" * 50}
        stream = io.BytesIO(encode_pretty(document))
        parsed = list(disconnected._iter_json_documents(stream, chunk_size=chunk_size))
        assert parsed == [document]

    def test_truncated_output_raises(self):
        """
        Never resolve versions from a partial catalog: that yields a plausible
        but wrong pin instead of a visible failure.
        """
        stream = io.BytesIO(b'{"a": 1}{"b":')
        with pytest.raises(CommandFailed, match="Unparsable trailing output"):
            list(disconnected._iter_json_documents(stream))

    def test_oversized_document_raises(self):
        stream = io.BytesIO(b"[" + b"1," * 5000)
        with pytest.raises(CommandFailed, match="larger than"):
            list(
                disconnected._iter_json_documents(
                    stream, chunk_size=64, max_buffer=1024
                )
            )


class TestSortVersions:
    def test_numeric_not_lexicographic(self):
        versions = ["4.20.10-rhodf", "4.20.9-rhodf", "4.20.1-rhodf"]
        assert disconnected._sort_versions(versions) == [
            "4.20.1-rhodf",
            "4.20.9-rhodf",
            "4.20.10-rhodf",
        ]

    def test_build_number_of_dev_builds_is_compared(self):
        """
        Comparing only the x.y.z prefix makes every 4.22.5-* build tie, which
        makes z-n selection non-deterministic on konflux catalogs.
        """
        versions = ["4.22.5-6.konflux", "4.22.5-10.konflux", "4.22.5-2.konflux"]
        assert disconnected._sort_versions(versions) == [
            "4.22.5-2.konflux",
            "4.22.5-6.konflux",
            "4.22.5-10.konflux",
        ]

    def test_duplicates_removed(self):
        assert disconnected._sort_versions(["4.20.1-rhodf"] * 3) == ["4.20.1-rhodf"]


class TestVersionFromBundleName:
    @pytest.mark.parametrize(
        "package,bundle_name,expected",
        [
            ("odf-operator", "odf-operator.v4.20.10-rhodf", "4.20.10-rhodf"),
            (
                "odf-dependencies",
                "odf-dependencies.v4.22.0-65.stable",
                "4.22.0-65.stable",
            ),
            # a bundle of a different package must not be misattributed
            ("odf-operator", "ocs-operator.v4.20.10-rhodf", None),
            ("odf-operator", "odf-operator.", None),
        ],
    )
    def test_extraction(self, package, bundle_name, expected):
        assert disconnected._version_from_bundle_name(package, bundle_name) == expected


class TestRenderCatalog:
    def test_collects_versions_and_channels(self, render):
        documents = build_catalog_documents(
            "odf-operator", ["4.20.1-rhodf", "4.20.2-rhodf"], "stable-4.20"
        )
        catalog = render(documents, ["odf-operator"])
        assert catalog["odf-operator"]["versions"] == {
            "4.20.1-rhodf": {"stable-4.20"},
            "4.20.2-rhodf": {"stable-4.20"},
        }
        assert catalog["odf-operator"]["default_channel"] == "stable-4.20"

    def test_unknown_package_yields_empty_entry(self, render):
        documents = build_catalog_documents(
            "odf-operator", ["4.20.1-rhodf"], "stable-4.20"
        )
        catalog = render(documents, ["odf-operator", "missing-operator"])
        assert catalog["missing-operator"] == {
            "versions": {},
            "default_channel": None,
        }


class TestGetCatalogPackageVersions:
    def test_filters_to_requested_minor(self, render):
        """
        The index is cumulative: a v4.20 catalog also carries 4.19 bundles, and
        z-n must never silently resolve to the prior minor.
        """
        documents = build_catalog_documents(
            "odf-operator",
            ["4.20.1-rhodf", "4.20.2-rhodf"],
            "stable-4.20",
            other_minor=["4.19.8-rhodf"],
        )
        catalog = render(documents, ["odf-operator"])
        versions = disconnected.get_catalog_package_versions(
            "index:v4.20", "odf-operator", "4.20", catalog=catalog
        )
        assert versions == ["4.20.1-rhodf", "4.20.2-rhodf"]

    def test_no_match_raises(self, render):
        documents = build_catalog_documents(
            "odf-operator", ["4.20.1-rhodf"], "stable-4.20"
        )
        catalog = render(documents, ["odf-operator"])
        with pytest.raises(NotFoundError, match="minor version 4.21"):
            disconnected.get_catalog_package_versions(
                "index:v4.20", "odf-operator", "4.21", catalog=catalog
            )


class TestResolveZMinusNVersion:
    @pytest.fixture
    def catalog(self, render):
        documents = build_catalog_documents(
            "odf-operator",
            [f"4.20.{i}-rhodf" for i in range(5)],
            "stable-4.20",
        )
        return render(documents, ["odf-operator"])

    @pytest.mark.parametrize(
        "z_minus_n,expected",
        [(0, "4.20.4-rhodf"), (2, "4.20.2-rhodf"), ("3", "4.20.1-rhodf")],
    )
    def test_resolution(self, catalog, z_minus_n, expected):
        assert (
            disconnected.resolve_z_minus_n_version(
                "index:v4.20", z_minus_n, "4.20", catalog=catalog
            )
            == expected
        )

    def test_falls_back_to_oldest(self, catalog):
        """Newly branched minors may have fewer builds than requested."""
        assert (
            disconnected.resolve_z_minus_n_version(
                "index:v4.20", 99, "4.20", catalog=catalog
            )
            == "4.20.0-rhodf"
        )


class TestResolvePinnedVersions:
    def test_pins_each_package_to_its_own_channel(self, render):
        """
        The disconnected package list is not purely ODF. elasticsearch-operator
        ships in stable-5.7, never in stable-4.20, so reusing the odf-operator
        pin for it would emit constraints no bundle satisfies.
        """
        documents = []
        for package in ("odf-operator", "ocs-operator"):
            documents += build_catalog_documents(
                package,
                ["4.20.1-rhodf", "4.20.2-rhodf", "4.20.3-rhodf"],
                "stable-4.20",
            )
        documents += build_catalog_documents(
            "elasticsearch-operator",
            ["5.7.1", "5.7.2"],
            "stable-5.7",
            default_channel="stable",
        )
        packages = ["odf-operator", "ocs-operator", "elasticsearch-operator"]
        catalog = render(documents, packages + ["odf-operator"])

        pinned, anchor = self._resolve(catalog, packages, z_minus_n=1)

        assert anchor == "4.20.2-rhodf"
        assert pinned == {
            "odf-operator": {
                "to": {"channel": "stable-4.20", "version": "4.20.2-rhodf"}
            },
            "ocs-operator": {
                "to": {"channel": "stable-4.20", "version": "4.20.2-rhodf"}
            },
        }
        # left unpinned rather than pinned to a channel it is not in
        assert "elasticsearch-operator" not in pinned

    def test_matches_across_differing_build_suffixes(self, render):
        documents = build_catalog_documents(
            "odf-operator", ["4.22.0-65.stable"], "stable-4.22"
        ) + build_catalog_documents(
            "cephcsi-operator", ["4.22.0-31.konflux"], "stable-4.22"
        )
        packages = ["odf-operator", "cephcsi-operator"]
        catalog = render(documents, packages)

        pinned, anchor = self._resolve(
            catalog, packages, z_minus_n=0, minor_version="4.22"
        )

        assert anchor == "4.22.0-65.stable"
        assert pinned["cephcsi-operator"]["to"]["version"] == "4.22.0-31.konflux"

    @staticmethod
    def _resolve(catalog, packages, z_minus_n, minor_version="4.20"):
        """
        Drive resolve_pinned_versions against an already-parsed catalog.

        Args:
            catalog (dict): output of _render_catalog
            packages (list): packages to pin
            z_minus_n (int): builds behind latest
            minor_version (str): major.minor to resolve within

        Returns:
            tuple(dict, str): pin map and resolved anchor version
        """
        import unittest.mock as mock

        with mock.patch.object(disconnected, "_render_catalog", return_value=catalog):
            return disconnected.resolve_pinned_versions(
                "index:v4.20", packages, z_minus_n, minor_version
            )


class TestPinnedImageSetConfig:
    """The pin map has to survive into the generated ImageSetConfig."""

    @staticmethod
    def build_packages(packages, pinned_versions):
        """
        Run mirror_index_image_via_oc_mirror far enough to capture the package
        list it writes into the ImageSetConfig.

        Args:
            packages (list): packages to mirror
            pinned_versions (dict): per-package pin map

        Returns:
            list: the "packages" entries of the generated catalog entry
        """
        import unittest.mock as mock

        imageset_config = {"mirror": {"operators": []}}
        with (
            mock.patch.object(disconnected, "get_oc_mirror_tool"),
            mock.patch.object(disconnected, "login_to_mirror_registry"),
            mock.patch.object(
                disconnected.templating, "load_yaml", return_value=imageset_config
            ),
            mock.patch.object(disconnected.templating, "dump_data_to_temp_yaml"),
            mock.patch.object(disconnected.os.path, "exists", return_value=True),
            mock.patch.object(
                disconnected, "exec_cmd", side_effect=CommandFailed("stop here")
            ),
            mock.patch.dict(disconnected.config.ENV_DATA, {"cluster_path": "/tmp"}),
            mock.patch.dict(disconnected.config.RUN, {"run_id": "unit-test"}),
            mock.patch.dict(
                disconnected.config.DEPLOYMENT, {"mirror_registry": "mirror:5000"}
            ),
        ):
            with pytest.raises(CommandFailed):
                # __wrapped__ skips the @retry decorator
                disconnected.mirror_index_image_via_oc_mirror.__wrapped__(
                    "index:v4.20", packages, pinned_versions=pinned_versions
                )
        return imageset_config["mirror"]["operators"][0]["packages"]

    def test_pinned_and_unpinned_packages(self):
        entries = self.build_packages(
            ["odf-operator", "elasticsearch-operator"],
            {
                "odf-operator": {
                    "to": {"channel": "stable-4.20", "version": "4.20.2-rhodf"}
                }
            },
        )
        assert entries == [
            {
                "name": "odf-operator",
                "channels": [
                    {
                        "name": "stable-4.20",
                        "minVersion": "4.20.2-rhodf",
                        "maxVersion": "4.20.2-rhodf",
                    }
                ],
            },
            # no build at the pinned version: mirror every version instead of
            # emitting an unsatisfiable constraint
            {"name": "elasticsearch-operator"},
        ]

    def test_without_pinning_no_constraints_are_emitted(self):
        entries = self.build_packages(["odf-operator", "ocs-operator"], None)
        assert entries == [{"name": "odf-operator"}, {"name": "ocs-operator"}]
