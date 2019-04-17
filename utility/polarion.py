import logging
import os

from tempfile import NamedTemporaryFile
from subprocess import call
from jinja2 import Environment, FileSystemLoader
from utility.utils import get_ocsci_config

log = logging.getLogger(__name__)


def post_to_polarion(tc):
    """
    Function to post test results polarion
    It returns nothing and is essentially like noop
    in case of no polarion details found in test object

    Args:
       tc: test case object with details

    Returns:
      None
    """
    current_dir = os.getcwd()
    polarion_cred = get_ocsci_config()['polarion']

    if tc['polarion-id'] is not None:
        # add polarion attributes
        ids = tc['polarion-id'].split(',')
        tc['space'] = 'Smoke Suite'
        tc['test_run_id'] = tc['ceph-version'] + "_" + tc['suite-name'] + "_" + tc['distro'] + "_Automated_Smoke_Runs"
        tc['test_run_id'] = tc['test_run_id'].replace('.', '_')
        log.info("Updating test run: %s " % tc['test_run_id'])
        tc['ceph-build'] = '_'.join(
            [_f for _f in [tc['ceph-version'], tc['ceph-ansible-version'], tc['compose-id']] if _f])
        if tc.get('docker-containers-list'):
            tc['docker-container'] = '\ncontainer: {container}'.format(
                container=','.join(list(set(tc.get('docker-containers-list')))))
        tc['test_case_title'] = tc['desc']
        if tc['desc'] is None:
            log.info("cannot update polarion with no description")
            return
        if tc['status'] == "Pass":
            tc['result'] = ''
        else:
            tc['result'] = '<failure message="test failed" type="failure"/>'
        current_dir += '/templates/'
        j2_env = Environment(loader=FileSystemLoader(current_dir),
                             trim_blocks=True)
        for id in ids:
            tc['polarion-id'] = id
            f = NamedTemporaryFile(delete=False)
            test_results = j2_env.get_template('importer-template.xml').render(tc=tc)
            log.info("updating results for %s " % id)
            f.write(test_results.encode())
            f.close()
            url = polarion_cred.get('url')
            user = polarion_cred.get('username')
            pwd = polarion_cred.get('password')
            call(['curl', '-k', '-u',
                  '{user}:{pwd}'.format(user=user, pwd=pwd),
                  '-X', 'POST', '-F', 'file=@{name}'.format(name=f.name),
                  url])
            os.unlink(f.name)
