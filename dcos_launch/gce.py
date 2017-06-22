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
        credentials = util.read_file(credentials_path)
        self.gce_wrapper = gce.GceWrapper(json.loads(credentials), credentials_path)
        self.config = config
        self.deployment = gce.Deployment(self.gce_wrapper, self.config['deployment_name'], self.config['gce_zone'])

    def create(self) -> dict:
        self.key_helper()
        node_count = 1 + self.config['num_masters'] + self.config['num_public_agents'] \
                       + self.config['num_private_agents']
        self.deployment.create(self.deployment.name, self.deployment.zone, self.deployment.gce_wrapper,
                               self.deployment.instance_group_name, node_count, self.config['source_image'],
                               self.config['machine_type'], self.config['image_project'],
                               self.config['gce_zone'], self.config['ssh_user'],
                               self.config['ssh_public_key'])
        return self.config

    def key_helper(self):
        """ Generates a public key and a private key and stores them in the config. The public key will be applied to
        all the instances in the deployment later on when wait() is called.
        """
        if self.config['key_helper']:
            private_key, public_key = util.generate_rsa_keypair()
            self.config['ssh_private_key'] = private_key.decode()
            self.config['ssh_public_key'] = public_key.decode()

    def get_hosts(self) -> [Host]:
        return self.deployment.hosts

    def wait(self):
        """ Waits for the deployment to complete: first, the network that will contain the cluster is deployed. Once
            the network is deployed, a firewall for the network and an instance template are deployed. Finally,
            once the instance template is deployed, an instance group manager and all its instances are deployed.
        """
        self.deployment.wait_for_completion()

    def delete(self):
        """ Deletes all the resources associated with the deployment (instance template, network, firewall, instance
            group manager and all its instances.
        """
        self.deployment.delete()

    def test(self, args, env_dict, test_host=None, test_port=22):
        raise NotImplementedError('Bare clusters cannot be tested!')
