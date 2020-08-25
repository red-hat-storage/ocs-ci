# -*- coding: utf8 -*-

import json

import pytest

from ocs_ci.framework import config


@pytest.fixture
def clusterdir(request, tmpdir):
    """
    This fixture creates an openshift cluster dir with minimal metadata.json
    file inside so that ocs-ci deployment class can be instantiated.
    """
    # TODO: Do we want to add `cluster_path = null` to ENV_DATA section
    #       of ocs_ci/framework/conf/default_config.yaml file?
    # we assume that cluster_path is not preconfigured
    assert "cluster_path" not in config.ENV_DATA

    def finalizer():
        del config.ENV_DATA['cluster_path']
    request.addfinalizer(finalizer)

    # prepare minimal cluster dir
    metadata_file = tmpdir.join("metadata.json")
    metadata_dict = {"clusterName": "unit-test-cluster"}
    metadata_file.write(json.dumps(metadata_dict))
    config.ENV_DATA['cluster_path'] = tmpdir
    return metadata_dict
