import json
import logging

from dcos_launch import util
from dcos_test_utils import gce
from dcos_test_utils.helpers import Host

log = logging.getLogger(__name__)


class BareClusterLauncher(util.AbstractLauncher):
    # Launches a homogeneous cluster of plain GMIs intended for onprem DC/OS
    def __init__(self, config):
        credentials_path = util.set_from_env('GOOGLE_APPLICATION_CREDENTIALS')

        with open(credentials_path, 'r') as creds:
            self.gce_wrapper = gce.GceWrapper(json.load(creds))

        self.config = config
        self.deployment = gce.Deployment(self.gce_wrapper, config['deployment_name'], config['zone'])
        self.insert_request_errors = None

    def create(self) -> dict:
        response = self.gce_wrapper.deploy_instances(self.config)
        self.insert_request_errors = response.get('error')
        self.key_helper()
        return self.config

    def key_helper(self):
        # Generate the private key file and applies the public key to the instance group
        if not self.config.get('key_helper'):
            return
        private_key, public_key = util.generate_rsa_keypair()
        self.config['ssh_private_key'] = private_key.decode()
        self.config['ssh_public_key'] = public_key.decode()

    def get_hosts(self) -> [Host]:
        return self.deployment.get_hosts()

    def wait(self) -> dict:
        response = self.deployment.wait()
        self.deployment.allow_all_ports()
        self.deployment.apply_ssh_key(self.config['ssh_user'], self.config['ssh_public_key'])
        return response

    def delete(self) -> dict:
        return self.deployment.delete()

    def describe(self) -> dict:
        return self.deployment.get_info()

    def test(self, args, env_dict, test_host=None, test_port=22):
        raise NotImplementedError('Bare clusters cannot be tested!')
