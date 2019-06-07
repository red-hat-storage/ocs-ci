# -*- coding: utf-8 -*-
from pytest import fixture

import ocs.defaults
import ocsci
import ocsci.main


class TestConfig(object):
    @fixture(autouse=True)
    def reset_config(self):
        ocsci.config.reset()

    def test_defaults(self):
        ocsci.main.init_ocsci_conf()
        config_sections = ocsci.config.to_dict().keys()
        for section_name in config_sections:
            section = getattr(ocsci.config, section_name)
            assert section == getattr(ocs.defaults, section_name)
