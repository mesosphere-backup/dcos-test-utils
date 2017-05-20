import os
import logging
import dcos_launch.util
from dcos_test_utils import gce
from retrying import retry

log = logging.getLogger(__name__)


class BareClusterLauncher(dcos_launch.util.AbstractLauncher):
    # Launches a homogeneous cluster of plain GMIs intended for onprem DC/OS
    def __init__(self, config):
        # config = get_hardcoded_basic_config(os_name, instance_count)
        self.cwd = os.path.dirname(os.path.realpath(__file__))
        gce_folder = os.path.expanduser('~') + '/.gce'
        credentials_path = gce_folder + '/credentials.json'
        self.keys_path = gce_folder + '/keys'
        self.gce_wrapper = gce.GceWrapper(credentials_path)
        self.private_key_path = self.keys_path + '/' + config['deployment_name'] + '.priv'
        self.public_key_path = self.keys_path + '/' + config['deployment_name'] + '.pub'

        template_path = config.get('template_path', os.path.join(self.cwd, 'templates', 'gce-template.yaml'))
        with open(template_path, 'r') as tfile:
            template_content = tfile.read()
        template_content = template_content.replace('${network}', config['network'])
        template_content = template_content.replace('${sourceImage}', config['sourceImage'])
        template_content = template_content.replace('${machineType}', config['machineType'])
        template_content = template_content.replace('${zone}', config['zone'])
        template_content = template_content.replace('${project_id}', self.gce_wrapper.project_id)
        config['template_content'] = template_content

        self.config = config
        self.deployment = gce.Deployment(self.gce_wrapper, config['deployment_name'], config['zone'])

    def create(self):
        response = self.gce_wrapper.deploy_instances(self.config)

        if self.config['key_helper']:
            self.key_helper()

        return response

    def key_helper(self):
        # Generate the private key file and applies the public key to the instance group
        private_key, public_key = dcos_launch.util.generate_rsa_keypair()

        with open(self.public_key_path, 'w+') as public_file:
            public_file.write(public_key.decode())

        private_file = open(self.private_key_path, 'w+')
        private_file.write(private_key.decode())
        private_file.close()
        os.chmod(self.private_key_path, 0o600)

    def get_hosts(self):
        return self.deployment.get_host_ips()

    def _check_public_key(public_key):
        if public_key:
            return False
        return True

    @retry(wait_fixed=1000, retry_on_result=_check_public_key, stop_max_attempt_number=7)
    def get_public_key(self):
        with open(self.public_key_path, 'r') as public_file:
            public_key = public_file.read()
            return public_key

    def wait(self):
        response = self.deployment.wait()

        if self.config['key_helper']:
            self.deployment.apply_ssh_key(self.get_public_key())

        return response

    def delete(self):
        return self.deployment.delete()

    def get_instances_info(self):
        return self.deployment.get_instances_info()

    def describe(self):
        return self.deployment.get_info()

    def test(self, args, env_dict, test_host=None, test_port=22):
        raise NotImplementedError('Bare clusters cannot be tested!')
