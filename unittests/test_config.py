# -*- coding: utf-8 -*-
import ocs
import ocsci
import run_ocsci


class TestConfig(object):
    def test_defaults(self):
        run_ocsci.init_ocsci_conf()
        config_sections = [i for i in dir(ocsci.config) if not i.startswith('_')]
        for section_name in config_sections:
            if section_name == 'CONFIG':
                continue
            section = getattr(ocsci.config, section_name)
            assert section == getattr(ocs.defaults, section_name)
