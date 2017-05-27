import json
import logging

from dcos_launch import util
from dcos_test_utils import gce

log = logging.getLogger(__name__)

TEMPLATE = """
- type: compute.v1.instance
  name: {name}
  properties:
    zone: {zone}
    machineType: zones/{zone}/machineTypes/{machineType}
    disks:
    - deviceName: boot
      type: PERSISTENT
      boot: true
      autoDelete: true
      initializeParams:
        sourceImage: projects/coreos-cloud/global/images/family/{sourceImage}
    networkInterfaces:
    - network: global/networks/default
      # Access Config required to give the instance a public IP address
      accessConfigs:
      - name: External NAT
        type: ONE_TO_ONE_NAT"""


class BareClusterLauncher(util.AbstractLauncher):
    # Launches a homogeneous cluster of plain GMIs intended for onprem DC/OS
    def __init__(self, config):
        credentials_path = util.set_from_env('GOOGLE_APPLICATION_CREDENTIALS')
        node_count = 1 + config['num_masters'] + config['num_public_agents'] + config['num_private_agents']
        with open(credentials_path, 'r') as creds:
            self.gce_wrapper = gce.GceWrapper(json.load(creds))

        all_template = 'resources:'
        for i in range(node_count):
            all_template += TEMPLATE.format(name='vm' + str(i), sourceImage=config['sourceImage'],
                                            machineType=config['machineType'], zone=config['zone'],
                                            project_id=self.gce_wrapper.project_id)
        config['template_body'] = all_template

        self.config = config
        self._deployment = gce.Deployment(self.gce_wrapper, config['deployment_name'], config['zone'])
        self.insert_request_errors = None

    def get_deployment(self):
        if self._deployment is None:
            raise Exception('Deployment creation request failed with: ' + str(self.insert_request_errors))
        if self._deployment.errors is not None:
            raise Exception('Deployment failed with: ' + str(self._deployment.errors))
        return self._deployment

    deployment = property(fget=get_deployment)

    def create(self):
        response = self.gce_wrapper.deploy_instances(self.config)
        self.insert_request_errors = response.get('error')
        self.key_helper()
        return self.config

    def key_helper(self):
        # Generate the private key file and applies the public key to the instance group
        if not self.config['key_helper']:
            return
        private_key, public_key = util.generate_rsa_keypair()
        self.config['ssh_private_key'] = private_key.decode()
        self.config['ssh_public_key'] = public_key.decode()

    def get_hosts(self):
        return self.deployment.get_host_ips()

    def wait(self):
        response = self.deployment.wait()
        self.deployment.allow_external_access()

        if self.config['key_helper']:
            self.deployment.apply_ssh_key(self.config['ssh_user'], self.config['ssh_public_key'])

        return response

    def delete(self):
        return self.deployment.delete()

    def get_instances_info(self):
        return self.deployment.get_instances_info()

    def describe(self):
        return self.deployment.get_info()

    def test(self, args, env_dict, test_host=None, test_port=22):
        raise NotImplementedError('Bare clusters cannot be tested!')
