# -*- coding: utf-8 -*-
import subprocess

from ocsci.main import update_dict_recursively


class TestEntrypoint(object):
    def test_help(self):
        result = subprocess.check_output(
            ['run-ci', '--help'],
            stderr=subprocess.STDOUT,
        ).decode()
        assert '--ocsci-conf' in result
        assert '--cluster-conf' in result
        assert '--cluster-path' in result
        assert '--cluster-name' in result


class TestUpdateDict(object):
    def test_update_dict(self):
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
            a_string='string',
        )
        expected = dict(
            a_dict=dict(
                another_list=[],
                another_string="greetings",
                a_third_string="salutations",
            ),
            a_list=[0, 1, 2],
            a_string='string',
        )
        result = update_dict_recursively(objA, objB)
        assert objA is result
        assert result == expected
