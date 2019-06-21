# -*- coding: utf-8 -*-
from pytest import fixture

import ocsci
import ocsci.main


class TestConfig(object):
    @fixture(autouse=True)
    def reset_config(self):
        ocsci.config.reset()

    def test_defaults(self):
        ocsci.main.init_ocsci_conf()
        env_data = getattr(ocsci.config, 'ENV_DATA')
        reporting_data = getattr(ocsci.config, 'REPORTING')
        run_data = getattr(ocsci.config, 'RUN')
        assert env_data['rook_image'] == 'rook/ceph:master'
        assert reporting_data['email']['address'] == 'ocs-ci@redhat.com'
        assert run_data['bin_dir'] == './bin'
