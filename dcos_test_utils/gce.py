import logging

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from retrying import retry

from dcos_test_utils.helpers import Host

log = logging.getLogger(__name__)

OS_IMAGE_FAMILIES = {
    'centos': 'centos-7',
    'centos-7-dcos-prereqs': 'unsupported',
    'rhel': 'rhel-7',
    'ubuntu': 'ubuntu-1604-lts',
    'coreos': 'coreos-stable',
    'debian': 'debian-8'
}

IMAGE_PROJECTS = {
    'centos': 'centos-cloud',
    'rhel': 'rhel-cloud',
    'ubuntu': 'ubuntu-os-cloud',
    'coreos': 'coreos-cloud',
    'debian': 'debian-cloud'
}

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
        sourceImage: projects/{imageProject}/global/images/family/{sourceImage}
    networkInterfaces:
    - network: global/networks/default
      # Access Config required to give the instance a public IP address
      accessConfigs:
      - name: External NAT
        type: ONE_TO_ONE_NAT"""


class Instance:
    '''
    Contains info about a node in a deployment
    '''
    def __init__(self, name: str, priv_ip: str, pub_ip: str):
        self.name = name
        self.private_ip = priv_ip
        self.public_ip = pub_ip

    @property
    def host(self):
        return Host(self.private_ip, self.public_ip)


class Deployment:
    def __init__(self, gce_wrapper, name, zone):
        self.gce_wrapper = gce_wrapper
        self.name = name
        self.zone = zone

    @property
    def instances(self) -> [Instance]:
        for instance in self.gce_wrapper.get_instances_info(self):
            log.debug(instance)
            yield Instance(instance['name'],
                           instance['networkInterfaces'][0]['networkIP'],
                           instance['networkInterfaces'][0]['accessConfigs'][0]['natIP'])

    @property
    def fingerprint(self) -> str:
        return next(self.gce_wrapper.get_instances_info(self))['metadata']['fingerprint']

    def get_hosts(self):
        return [i.host for i in self.instances]

    def apply_ssh_key(self, user, public_key):
        body = {
            'kind': 'compute#metadata',
            'fingerprint': self.fingerprint,
            'items': [
                {
                    'key': 'ssh-keys',
                    'value': '{}:{}'.format(user, public_key)
                }
            ]
        }

        for i in self.instances:
            log.debug(self.gce_wrapper.set_metadata(self, body, i.name))

    def get_info(self) -> dict:
        response = self.gce_wrapper.get_deployment_info(self)
        log.debug(response)
        errors = response['operation'].get('error')
        if errors:
            raise Exception('The deployment you are accessing contains errors:' + str(errors))
        return response

    def delete(self) -> dict:
        response = self.gce_wrapper.delete_deployment(self)
        log.debug(response)
        return response

    def _check_status(response):
        '''
        Checks the status of the deployment until it is done or has failed
        :param response : <dict> http response containing info about the deployment
        :return: <boolean> whether to continue checking the status of the deployment (True) or not (False)
        '''
        status = response['operation']['status']
        if status == 'DONE':
            return False
        elif status == 'RUNNING' or status == 'PENDING':
            log.debug('Waiting for deployment')
            return True
        else:
            raise Exception('Deployment failed with response: ' + str(response))

    @retry(wait_fixed=2000, retry_on_result=_check_status)
    def wait(self) -> dict:
        response = self.get_info()
        log.debug(response)
        return response

    def allow_all_ports(self) -> dict:
        body = {
            "kind": "compute#firewall",
            "name": "allow all ports",
            "description": "allow all ports",
            "network": "global/networks/default",
            "sourceRanges": [
                "0.0.0.0/0"
            ],
            "allowed": [
                {
                    "IPProtocol": "tcp",
                    "ports": [
                        "0-65535"
                    ]
                },
                {
                    "IPProtocol": "udp",
                    "ports": [
                        "0-65535"
                    ]
                },
                {
                    "IPProtocol": "icmp"
                }
            ]
        }

        response = self.gce_wrapper.add_firewall_rule(body)
        log.debug(response)
        return response


def catch_http_exceptions(f):
    '''
    Runs the function "f" and raises an exception with a custom help message if a google HttpError with status 404 or
        409 is caught
    :param f: function to be executed
    :return: return value of function "f"
    '''
    def handle_exception(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except HttpError as e:
            if e.resp.status == 404:
                log.exception("The resource you are trying to access doesn't exist")
            elif e.resp.status == 409:
                log.exception('''The specified resources exist and might be under an active operation
                                   (operation conflict)''')
            raise e

    return handle_exception


class GceWrapper:
    @catch_http_exceptions
    def __init__(self, credentials_dict):
        credentials = GoogleCredentials.get_application_default()
        self.compute = discovery.build('compute', 'v1', credentials=credentials)
        self.deployment_manager = discovery.build('deploymentmanager', 'v2', credentials=credentials)
        self.project_id = credentials_dict['project_id']

    def build_template_body(self, config) -> str:
        node_count = 1 + config['num_masters'] + config['num_public_agents'] + config['num_private_agents']
        template_body = 'resources:'
        for i in range(node_count):
            template_body += TEMPLATE.format(name='vm' + str(i), sourceImage=config['source_image'],
                                             machineType=config['machine_type'], imageProject=config['image_project'],
                                             project_id=self.project_id, zone=config['zone'], )
        return template_body

    @catch_http_exceptions
    def deploy_instances(self, config) -> dict:
        config['template_body'] = self.build_template_body(config)

        body = {
            'name': config['deployment_name'],
            'target': {
                'config': {
                    'content': config['template_body']
                }
            }
        }

        return self.deployment_manager.deployments().insert(project=self.project_id, body=body).execute()

    @catch_http_exceptions
    def get_instances_info(self, deployment: Deployment) -> [dict]:
        request = self.compute.instances().list(project=self.project_id, zone=deployment.zone)
        while request is not None:
            response = request.execute()
            for instance_info in response['items']:
                yield instance_info
            request = self.deployment_manager.resources().list_next(previous_request=request,
                                                                    previous_response=response)

    @catch_http_exceptions
    def get_deployment_info(self, deployment: Deployment) -> dict:
        return self.deployment_manager.deployments().get(project=self.project_id,
                                                         deployment=deployment.name).execute()

    @catch_http_exceptions
    def delete_deployment(self, deployment: Deployment) -> dict:
        return self.deployment_manager.deployments().delete(project=self.project_id,
                                                            deployment=deployment.name).execute()

    @catch_http_exceptions
    def set_metadata(self, deployment: Deployment, body, instance_name) -> dict:
        return self.compute.instances().setMetadata(project=self.project_id, zone=deployment.zone,
                                                    instance=instance_name, body=body).execute()

    @catch_http_exceptions
    def add_firewall_rule(self, body) -> dict:
        return self.compute.firewalls().insert(project=self.project_id, body=body)
