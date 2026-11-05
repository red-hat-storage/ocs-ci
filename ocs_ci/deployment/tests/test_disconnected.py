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

    def test_pinned_upgrade_ystream(self):
        """
        Y-stream upgrade: from_channel != to_channel.
        ISC must contain both channels with exact pins for OLM graph continuity.
        """
        entries = self.build_packages(
            ["odf-operator"],
            {
                "odf-operator": {
                    "from": {
                        "channel": "stable-4.20",
                        "version": "4.20.10-rhodf",
                    },
                    "to": {
                        "channel": "stable-4.21",
                        "version": "4.21.11-rhodf",
                    },
                }
            },
        )
        assert entries == [
            {
                "name": "odf-operator",
                "channels": [
                    {
                        "name": "stable-4.20",
                        "minVersion": "4.20.10-rhodf",
                        "maxVersion": "4.20.10-rhodf",
                    },
                    {
                        "name": "stable-4.21",
                        "minVersion": "4.21.11-rhodf",
                        "maxVersion": "4.21.11-rhodf",
                    },
                ],
            }
        ]

    def test_pinned_upgrade_zstream_non_consecutive(self):
        """
        Z-stream upgrade across non-consecutive builds: from_channel == to_channel.
        ISC must contain single channel entry with [from_version, to_version] range.
        """
        entries = self.build_packages(
            ["odf-operator"],
            {
                "odf-operator": {
                    "from": {
                        "channel": "stable-4.20",
                        "version": "4.20.1-rhodf",
                    },
                    "to": {
                        "channel": "stable-4.20",
                        "version": "4.20.3-rhodf",
                    },
                }
            },
        )
        assert entries == [
            {
                "name": "odf-operator",
                "channels": [
                    {
                        "name": "stable-4.20",
                        "minVersion": "4.20.1-rhodf",
                        "maxVersion": "4.20.3-rhodf",
                    }
                ],
            }
        ]

    def test_pinned_upgrade_new_package_in_target(self):
        """
        Package newly introduced in target minor (only has 'to' pin).
        ISC must contain single channel entry with target pin.
        """
        entries = self.build_packages(
            ["ceph-volsync-plugin-operator"],
            {
                "ceph-volsync-plugin-operator": {
                    "to": {
                        "channel": "stable-4.22",
                        "version": "4.22.0-10.konflux",
                    }
                }
            },
        )
        assert entries == [
            {
                "name": "ceph-volsync-plugin-operator",
                "channels": [
                    {
                        "name": "stable-4.22",
                        "minVersion": "4.22.0-10.konflux",
                        "maxVersion": "4.22.0-10.konflux",
                    }
                ],
            }
        ]


class TestResolvePinnedUpgradeVersions:
    """
    Unit tests for resolve_pinned_upgrade_versions resolving both 'from' and 'to'
    channel and version pins across packages.
    """

    def test_resolve_ystream_upgrade(self, render):
        # Catalog has 4.20 builds and 4.21 builds
        docs_420 = build_catalog_documents(
            "odf-operator",
            ["4.20.10-rhodf", "4.20.11-rhodf"],
            "stable-4.20",
        )
        docs_421 = build_catalog_documents(
            "odf-operator",
            ["4.21.10-rhodf", "4.21.11-rhodf"],
            "stable-4.21",
        )
        docs_ocs = build_catalog_documents(
            "ocs-operator",
            ["4.20.10-rhodf", "4.21.11-rhodf"],
            "stable-4.21",
        )
        # merge all docs
        all_docs = docs_420 + docs_421 + docs_ocs
        catalog = render(all_docs, ["odf-operator", "ocs-operator"])

        import unittest.mock as mock

        with mock.patch.object(disconnected, "_render_catalog", return_value=catalog):
            pinned, from_anchor, to_anchor = (
                disconnected.resolve_pinned_upgrade_versions(
                    index_image="index:v4.21",
                    packages=["odf-operator", "ocs-operator"],
                    from_minor="4.20",
                    target_minor="4.21",
                    from_version="4.20.10-rhodf",
                    to_version="4.21.11-rhodf",
                )
            )

        assert from_anchor == "4.20.10-rhodf"
        assert to_anchor == "4.21.11-rhodf"
        assert pinned["odf-operator"]["from"]["version"] == "4.20.10-rhodf"
        assert pinned["odf-operator"]["from"]["channel"] == "stable-4.20"
        assert pinned["odf-operator"]["to"]["version"] == "4.21.11-rhodf"
        assert pinned["odf-operator"]["to"]["channel"] == "stable-4.21"

    def test_resolve_zstream_upgrade(self, render):
        docs = build_catalog_documents(
            "odf-operator",
            ["4.20.1-rhodf", "4.20.2-rhodf", "4.20.3-rhodf"],
            "stable-4.20",
        )
        catalog = render(docs, ["odf-operator"])

        import unittest.mock as mock

        with mock.patch.object(disconnected, "_render_catalog", return_value=catalog):
            pinned, from_anchor, to_anchor = (
                disconnected.resolve_pinned_upgrade_versions(
                    index_image="index:v4.20",
                    packages=["odf-operator"],
                    from_minor="4.20",
                    target_minor="4.20",
                    from_version="4.20.1-rhodf",
                    to_version="4.20.3-rhodf",
                )
            )

        assert from_anchor == "4.20.1-rhodf"
        assert to_anchor == "4.20.3-rhodf"
        assert pinned["odf-operator"]["from"]["channel"] == "stable-4.20"
        assert pinned["odf-operator"]["to"]["channel"] == "stable-4.20"

    def test_default_z_minus_n_resolves_latest(self, render):
        docs = build_catalog_documents(
            "odf-operator",
            ["4.21.0-rhodf", "4.21.1-rhodf", "4.21.2-rhodf"],
            "stable-4.21",
        )
        catalog = render(docs, ["odf-operator"])

        import unittest.mock as mock

        with mock.patch.object(disconnected, "_render_catalog", return_value=catalog):
            pinned, from_anchor, to_anchor = (
                disconnected.resolve_pinned_upgrade_versions(
                    index_image="index:v4.21",
                    packages=["odf-operator"],
                    from_minor="4.20",
                    target_minor="4.21",
                    from_version="4.20.0-rhodf",
                    # to_version and z_minus_n omitted -> should default to latest target
                )
            )

        assert to_anchor == "4.21.2-rhodf"
        assert pinned["odf-operator"]["to"]["version"] == "4.21.2-rhodf"


class TestPinnedInstallManualApproval:
    """
    When disconnected_z_minus_n is set, prepare_disconnected_ocs_deployment()
    must force subscription_plan_approval=Manual so OLM cannot auto-upgrade
    beyond the pinned version due to a shared proxy serving higher bundles.
    """

    @staticmethod
    def _run_prepare(z_minus_n, pre_existing_approval=None):
        """
        Drive prepare_disconnected_ocs_deployment() just far enough to trigger
        the subscription_plan_approval side-effect, then capture the config.

        Returns:
            str or None: value of config.DEPLOYMENT["subscription_plan_approval"]
                after the call
        """
        import unittest.mock as mock
        from ocs_ci.deployment import disconnected

        deployment_overrides = {
            "disconnected": True,
            "disconnected_z_minus_n": z_minus_n,
        }
        if pre_existing_approval is not None:
            deployment_overrides["subscription_plan_approval"] = pre_existing_approval

        fake_catalog = {
            "odf-operator": {
                "versions": {f"4.20.{i}-rhodf": {"stable-4.20"} for i in range(5)},
                "default_channel": "stable-4.20",
            }
        }

        with (
            mock.patch.dict(
                disconnected.config.DEPLOYMENT,
                deployment_overrides,
                clear=False,
            ),
            mock.patch.dict(
                disconnected.config.ENV_DATA,
                {"ocs_version": "4.20", "cluster_path": "/tmp"},
                clear=False,
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocs_version_from_config",
                return_value=mock.MagicMock(
                    __str__=lambda s: "4.20", __format__=lambda s, f: "4.20"
                ),
            ),
            mock.patch.object(disconnected, "disable_default_sources"),
            mock.patch.object(disconnected, "login_to_mirror_registry"),
            mock.patch.object(
                disconnected, "get_and_apply_idms_from_catalog", return_value=None
            ),
            mock.patch.object(
                disconnected,
                "_render_catalog",
                return_value=fake_catalog,
            ),
            mock.patch.object(
                disconnected,
                "mirror_index_image_via_oc_mirror",
                return_value="mirror:5000/redhat/redhat-operator-index:v4.20",
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocp_running_version",
                return_value=mock.MagicMock(__le__=lambda s, o: False),
            ),
            mock.patch.dict(
                disconnected.config.DEPLOYMENT,
                {
                    "stage_rh_osbs": False,
                    "live_deployment": False,
                    "ocs_registry_image": "quay.io/rhceph-dev/ocs-registry:4.20.4-rhodf",
                    "mirror_registry": "mirror:5000",
                },
                clear=False,
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocp_running_version",
                return_value=mock.MagicMock(__le__=lambda s, o: False),
            ),
        ):
            try:
                disconnected.prepare_disconnected_ocs_deployment(upgrade=False)
            except Exception:
                pass
            return disconnected.config.DEPLOYMENT.get("subscription_plan_approval")

    def test_manual_approval_set_when_z_minus_n_configured(self):
        """z_minus_n set, no prior approval → Manual is forced."""
        result = self._run_prepare(z_minus_n=2)
        assert result == "Manual", (
            f"Expected 'Manual' but got {result!r}. "
            "Pinned installs must block OLM auto-upgrades."
        )

    def test_explicit_automatic_not_overwritten(self):
        """User explicitly set Automatic → their choice is respected."""
        result = self._run_prepare(z_minus_n=2, pre_existing_approval="Automatic")
        assert (
            result == "Automatic"
        ), "An explicitly configured subscription_plan_approval must not be overwritten."

    def test_no_z_minus_n_no_forced_approval(self):
        """No z_minus_n → approval stays unset (existing behaviour)."""
        import unittest.mock as mock
        from ocs_ci.deployment import disconnected

        with (
            mock.patch.dict(
                disconnected.config.DEPLOYMENT,
                {"disconnected": True, "disconnected_z_minus_n": None},
                clear=False,
            ),
            mock.patch.dict(
                disconnected.config.ENV_DATA,
                {"ocs_version": "4.20", "cluster_path": "/tmp"},
                clear=False,
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocs_version_from_config",
                return_value=mock.MagicMock(
                    __str__=lambda s: "4.20", __format__=lambda s, f: "4.20"
                ),
            ),
            mock.patch.object(disconnected, "disable_default_sources"),
            mock.patch.object(disconnected, "login_to_mirror_registry"),
            mock.patch.object(
                disconnected, "get_and_apply_idms_from_catalog", return_value=None
            ),
            mock.patch.object(
                disconnected,
                "mirror_index_image_via_oc_mirror",
                return_value="mirror:5000/redhat/redhat-operator-index:v4.20",
            ),
            mock.patch.dict(
                disconnected.config.DEPLOYMENT,
                {
                    "stage_rh_osbs": False,
                    "live_deployment": False,
                    "ocs_registry_image": "quay.io/rhceph-dev/ocs-registry:4.20.4-rhodf",
                    "mirror_registry": "mirror:5000",
                },
                clear=False,
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocp_running_version",
                return_value=mock.MagicMock(__le__=lambda s, o: False),
            ),
        ):
            try:
                disconnected.prepare_disconnected_ocs_deployment(upgrade=False)
            except Exception:
                pass
            result = disconnected.config.DEPLOYMENT.get("subscription_plan_approval")
        assert result is None, (
            f"Expected no approval to be set but got {result!r}. "
            "Without z_minus_n, approval must remain unset."
        )


class TestPrepareDisconnectedOcsDeploymentUpgrade:
    """
    Unit tests for prepare_disconnected_ocs_deployment when upgrade=True.
    Differentiates PINNED upgrade vs FULL (unpinned) upgrade.
    """

    @staticmethod
    def _run_upgrade_prepare(upgrade_overrides, deployment_overrides=None):
        import unittest.mock as mock
        from ocs_ci.deployment import disconnected

        dep_dict = {
            "disconnected": True,
            "stage_rh_osbs": False,
            "live_deployment": False,
            "mirror_registry": "mirror:5000",
        }
        if deployment_overrides:
            dep_dict.update(deployment_overrides)

        upg_dict = {"upgrade": True}
        if upgrade_overrides:
            upg_dict.update(upgrade_overrides)

        fake_catalog = {
            "odf-operator": {
                "versions": {
                    "4.20.10-rhodf": {"stable-4.20"},
                    "4.21.11-rhodf": {"stable-4.21"},
                },
                "default_channel": "stable-4.21",
            }
        }

        captured_kwargs = {}

        def fake_mirror(index_image, packages, **kwargs):
            captured_kwargs["index_image"] = index_image
            captured_kwargs["packages"] = packages
            captured_kwargs["pinned_versions"] = kwargs.get("pinned_versions")
            return f"mirror:5000/{index_image}"

        with (
            mock.patch.dict(disconnected.config.DEPLOYMENT, dep_dict, clear=False),
            mock.patch.dict(disconnected.config.UPGRADE, upg_dict, clear=False),
            mock.patch.dict(
                disconnected.config.ENV_DATA,
                {"ocs_version": "4.20", "cluster_path": "/tmp"},
                clear=False,
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocs_version_from_config",
                return_value=mock.MagicMock(
                    __str__=lambda s: "4.20", __format__=lambda s, f: "4.20"
                ),
            ),
            mock.patch.object(disconnected, "disable_default_sources"),
            mock.patch.object(disconnected, "login_to_mirror_registry"),
            mock.patch.object(
                disconnected, "get_and_apply_idms_from_catalog", return_value=None
            ),
            mock.patch.object(
                disconnected, "get_installed_odf_versions", return_value={}
            ),
            mock.patch.object(
                disconnected,
                "_render_catalog",
                return_value=fake_catalog,
            ),
            mock.patch.object(
                disconnected,
                "mirror_index_image_via_oc_mirror",
                side_effect=fake_mirror,
            ),
            mock.patch.object(
                disconnected,
                "get_semantic_ocp_running_version",
                return_value=mock.MagicMock(__le__=lambda s, o: False),
            ),
            mock.patch.object(
                disconnected,
                "get_latest_ds_olm_tag",
                return_value="latest-stable-4.21",
            ),
        ):
            try:
                disconnected.prepare_disconnected_ocs_deployment(upgrade=True)
            except Exception:
                pass
            return captured_kwargs, disconnected.config.DEPLOYMENT.get(
                "subscription_plan_approval"
            )

    def test_full_unpinned_upgrade_passes_no_pins(self):
        """Full (unpinned) upgrade: pinned_versions must be None."""
        kwargs, approval = self._run_upgrade_prepare(
            upgrade_overrides={"upgrade_ocs_version": "4.21"}
        )
        assert kwargs.get("pinned_versions") is None
        assert approval is None

    def test_pinned_upgrade_with_explicit_versions(self):
        """Pinned upgrade with explicit from/to versions: pinned_versions populated & Manual approval."""
        kwargs, approval = self._run_upgrade_prepare(
            upgrade_overrides={
                "upgrade_pinned_from_version": "4.20.10-rhodf",
                "upgrade_pinned_from_channel": "stable-4.20",
                "upgrade_pinned_to_version": "4.21.11-rhodf",
                "upgrade_pinned_to_channel": "stable-4.21",
                "upgrade_ocs_version": "4.21",
            }
        )
        pins = kwargs.get("pinned_versions")
        assert pins is not None
        assert "odf-operator" in pins
        assert pins["odf-operator"]["from"]["version"] == "4.20.10-rhodf"
        assert pins["odf-operator"]["to"]["version"] == "4.21.11-rhodf"
        assert approval == "Manual"

    def test_pinned_upgrade_with_z_minus_n(self):
        """Pinned upgrade with upgrade_disconnected_z_minus_n: resolves target pin and Manual approval."""
        kwargs, approval = self._run_upgrade_prepare(
            upgrade_overrides={
                "upgrade_disconnected_z_minus_n": 0,
                "upgrade_ocs_version": "4.21",
                "upgrade_pinned_from_version": "4.20.10-rhodf",
            }
        )
        pins = kwargs.get("pinned_versions")
        assert pins is not None
        assert pins["odf-operator"]["to"]["version"] == "4.21.11-rhodf"
        assert approval == "Manual"
