# -*- coding: utf-8 -*-
import ocs
import run_ocsci


class TestConfig(object):
    def test_defaults_persist(self):
        run_ocsci.init_ocsci_conf()
        assert ocs.defaults.TEST_DATA_NOT_CHANGED == "persist"

    def test_defaults_changed(self):
        run_ocsci.init_ocsci_conf()
        assert ocs.defaults.TEST_DATA_CHANGED == "really changed"
