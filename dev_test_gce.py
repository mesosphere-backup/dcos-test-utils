import logging

import pkg_resources
import pytest
import yaml

from dcos_launch import gce

log = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def config():
    conf = yaml.load(pkg_resources.resource_string('dcos_launch', 'sample_configs/gce-onprem.yaml').decode('utf-8'))
    return conf


@pytest.fixture(scope='module')
def launcher(config):
    return gce.BareClusterLauncher(config)


def test_create(launcher):
    assert launcher.create()


def test_wait(launcher):
    assert launcher.wait()


def test_get_instances_info(launcher):
    assert launcher.get_instances_info()


def test_describe(launcher):
    assert launcher.describe()


@pytest.fixture(scope='module')
def hosts(launcher):
    return launcher.get_hosts()


def test_ssh(launcher, hosts):
    for host in hosts:
        ssh = launcher.get_ssh_client()
        ssh.wait_for_ssh_connection(host['public_ip'])
        assert ssh.get_home_dir(host['public_ip'])


def test_delete(launcher):
    assert launcher.delete()
