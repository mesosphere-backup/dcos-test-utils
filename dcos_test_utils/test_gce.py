import dcos_test_utils.gce as gce
import dcos_launch
import yaml
import os
import pytest


@pytest.fixture(scope='module')
def gce_wrapper():
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', os.path.expanduser('~') + '/.gce/credentials.json')
    return gce.GceWrapper(credentials_path)


@pytest.fixture(scope='module')
def config(gce_wrapper):
    cwd = os.path.dirname(os.path.realpath(__file__))
    with open(cwd + '/../dcos_launch/sample_configs/gce-onprem.yaml') as cfile:
        config_content = cfile.read()
    conf = yaml.load(config_content)
    template_path = conf.get('template_path', cwd + '/../dcos_launch/templates/gce-template.yaml')
    with open(template_path, 'r') as tfile:
        template_content = tfile.read()
    template_content = template_content.replace('${network}', conf['network'])
    template_content = template_content.replace('${sourceImage}', conf['sourceImage'])
    template_content = template_content.replace('${machineType}', conf['machineType'])
    template_content = template_content.replace('${zone}', conf['zone'])
    template_content = template_content.replace('${project_id}', gce_wrapper.project_id)
    conf['template_content'] = template_content
    return conf


@pytest.fixture(scope='module')
def deployment(gce_wrapper, config):
    return gce.Deployment(gce_wrapper, config['deployment_name'], config['zone'])


@pytest.fixture(scope='module')
def instances(deployment):
    return deployment.get_instances()


def test_gce_init(gce_wrapper):
    assert gce_wrapper


def test_config(config):
    assert config


def test_deployment(deployment):
    assert deployment


def test_deploy(gce_wrapper, config):
    assert gce_wrapper.deploy_instances(config)


def test_wait(deployment, config):
    private_key, public_key = dcos_launch.util.generate_rsa_keypair()
    if config['key_helper']:
        deployment.apply_ssh_key(public_key.decode())
    assert deployment.wait()


def test_get_instances(instances):
    assert instances


def test_get_host_ips(deployment):
    assert deployment.get_host_ips()


def test_get_fingerprint(deployment):
    assert deployment.get_fingerprint()


def test_delete(deployment):
    return deployment.delete()
