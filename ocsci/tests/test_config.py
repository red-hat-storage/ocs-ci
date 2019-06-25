# -*- coding: utf-8 -*-
from pytest import fixture

import ocsci
import ocsci.main


class TestConfig(object):
    @fixture(autouse=True)
    def reset_config(self):
        ocsci.config.reset()

    def test_defaults(self):
        config_sections = ocsci.config.to_dict().keys()
        for section_name in config_sections:
            section = getattr(ocsci.config, section_name)
            assert section == ocsci.config.get_defaults()[section_name]

    def test_defaults_specific(self):
        assert ocsci.config.ENV_DATA['rook_image'] == 'rook/ceph:master'
        assert ocsci.config.REPORTING['email']['address'] == 'ocs-ci@redhat.com'
        assert ocsci.config.RUN['bin_dir'] == './bin'

    def test_custom_conf(self):
        user_dict = dict(
            REPORTING=dict(email='unit@test.com'),
            RUN=dict(log_dir='/dev/null'),
        )
        ocsci.config.update(user_dict)
        assert ocsci.config.REPORTING['email'] == 'unit@test.com'
        assert ocsci.config.RUN['log_dir'] == '/dev/null'
        default_bin_dir = ocsci.config.get_defaults()['RUN']['bin_dir']
        assert ocsci.config.RUN['bin_dir'] == default_bin_dir

    def test_layered_conf(self):
        orig_client = ocsci.config.get_defaults()['RUN']['client_version']
        assert ocsci.config.RUN['client_version'] == orig_client
        first = dict(RUN=dict(client_version='1'))
        ocsci.config.update(first)
        assert ocsci.config.RUN['client_version'] == '1'
        second = dict(
            RUN=dict(client_version='2'),
            DEPLOYMENT=dict(installer_version='1'),
        )
        ocsci.config.update(second)
        assert ocsci.config.RUN['client_version'] == '2'
        assert ocsci.config.DEPLOYMENT['installer_version'] == '1'


class TestMergeDict:
    def test_merge_dict(self):
        objA = dict(
            a_dict=dict(
                another_list=[],
                another_string="greetings",
            ),
            a_list=[0, 1, 2],
        )
        objB = dict(
            a_dict=dict(
                a_third_string="salutations",
            ),
            a_list=[1, 2, 3],
            a_string='string',
        )
        expected = dict(
            a_dict=dict(
                another_list=[],
                another_string="greetings",
                a_third_string="salutations",
            ),
            a_list=[1, 2, 3],
            a_string='string',
        )
        result = ocsci.merge_dict(objA, objB)
        assert objA is result
        assert result == expected
