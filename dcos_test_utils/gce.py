import logging
import typing
from functools import wraps

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from retrying import retry

from dcos_test_utils.helpers import Host

log = logging.getLogger(__name__)

# mapping used for the commonly used os name formats that differ from their respective formats in gce.
# Update these mappings to expand OS support
OS_IMAGE_FAMILIES = {
    'cent-os-7': 'centos-7',
    'ubuntu-16-04': 'ubuntu-1604-lts',
    'coreos': 'coreos-stable',
}

# used in the gce sourceImage link (instance template field)
IMAGE_PROJECTS = {
    'centos': 'centos-cloud',
    'rhel': 'rhel-cloud',
    'ubuntu': 'ubuntu-os-cloud',
    'coreos': 'coreos-cloud',
    'debian': 'debian-cloud'
}

# template for an "instance template" resource to be used in a managed instance group
INSTANCE_TEMPLATE = """
- type: compute.v1.instanceTemplate
  name: {name}
  metadata:
    dependsOn:
    - {network}
  properties:
    project: {project}
    properties:
      machineType: {machineType}
      disks:
      - deviceName: boot
        type: PERSISTENT
        boot: true
        autoDelete: true
        initializeParams:
          sourceImage: projects/{imageProject}/global/images/family/{sourceImage}
      networkInterfaces:
      - network: global/networks/{network}
        # Access Config required to give the instance a public IP address
        accessConfigs:
        - name: External NAT
          type: ONE_TO_ONE_NAT
      metadata:
        items:
        - key: ssh-keys
          value: {ssh_user}:{ssh_public_key}"""

# template for a network resource in a gce deployment
NETWORK_TEMPLATE = """
- type: compute.v1.network
  name: {name}
  properties:
    autoCreateSubnetworks: True"""

# template for an instance group manager resource in a gce deployment
MANAGED_INSTANCE_GROUP_TEMPLATE = """
- type: compute.v1.instanceGroupManager
  name: {name}
  metadata:
    dependsOn:
    - {instance_template_name}
  properties:
    baseInstanceName: vm
    instanceTemplate: global/instanceTemplates/{instance_template_name}
    zone: {zone}
    targetSize: {size}"""

# template for a firewall in the network of a gce deployment
FIREWALL_TEMPLATE = """
- type: compute.v1.firewall
  name: {name}
  metadata:
    dependsOn:
    - {network}
  properties:
    description: allow all ports
    network: global/networks/{network}
    sourceRanges:
    - 0.0.0.0/0
    allowed:
    - IPProtocol: tcp
    - IPProtocol: udp
    - IPProtocol: icmp
    - IPProtocol: sctp"""


# Function decorator that adds detail to potential googleapiclient.errors.HttpError exceptions with code 404 or 409
def catch_http_exceptions(f):
    @wraps(f)
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

    @catch_http_exceptions
    def create_deployment(self, deployment_name, resources: str):
        body = {
            'name': deployment_name,
            'target': {
                'config': {
                    'content': resources
                }
            }
        }

        response = self.deployment_manager.deployments().insert(project=self.project_id, body=body).execute()
        log.debug('GceWrapper: create_deployment response: ' + str(response))

    @catch_http_exceptions
    def get_resources_info(self, name):
        request = self.deployment_manager.resources().list(project=self.project_id, deployment=name)
        while request is not None:
            response = request.execute()
            log.debug('GceWrapper: get_resources_info response: ' + str(response))

            for resource_info in response['resources']:
                yield resource_info

            request = self.deployment_manager.resources().list_next(previous_request=request,
                                                                    previous_response=response)

    @catch_http_exceptions
    def get_instance_info(self, name, zone):
        response = self.compute.instances().get(project=self.project_id, zone=zone, instance=name).execute()
        log.debug('GceWrapper: get_instance_info response: ' + str(response))
        return response

    @catch_http_exceptions
    def get_deployment_info(self, name) -> dict:
        """ Returns the dictionary representation of a GCE deployment resource. For details on the contents of this
            resource, see https://cloud.google.com/deployment-manager/docs/reference/latest/deployments#resource"""
        response = self.deployment_manager.deployments().get(project=self.project_id,
                                                             deployment=name).execute()
        log.debug('GceWrapper: get_deployment_info response: ' + str(response))
        return response

    @catch_http_exceptions
    def delete_deployment(self, name):
        response = self.deployment_manager.deployments().delete(project=self.project_id, deployment=name).execute()
        log.debug('GceWrapper: delete_deployment response: ' + str(response))

    @catch_http_exceptions
    def list_group_instances(self, group_name, zone) -> typing.Iterator(dict):
        response = self.compute.instanceGroupManagers().listManagedInstances(project=self.project_id, zone=zone,
                                                                             instanceGroupManager=group_name).execute()
        log.debug('GceWrapper: list_group_instances response: ' + str(response))

        for instance in response['managedInstances']:
            yield instance


class Deployment:
    def __init__(self, gce_wrapper, name, zone):
        self.gce_wrapper = gce_wrapper
        self.name = name
        self.zone = zone
        self.instance_group_name = self.name + '-group'

    def create(self, num_masters, num_public_agents, num_private_agents, source_image, machine_type, image_project,
               gce_zone, ssh_user, ssh_public_key):
        template_name = self.name + '-template'
        network_name = self.name + '-network'
        node_count = 1 + num_masters + num_public_agents + num_private_agents

        deployment_resources = 'resources:'
        deployment_resources += NETWORK_TEMPLATE.format(name=network_name)
        deployment_resources += INSTANCE_TEMPLATE.format(project=self.gce_wrapper.project_id,
                                                         sourceImage=source_image,
                                                         name=template_name,
                                                         machineType=machine_type,
                                                         imageProject=image_project,
                                                         zone=gce_zone,
                                                         ssh_user=ssh_user,
                                                         ssh_public_key=ssh_public_key,
                                                         network=network_name)
        deployment_resources += MANAGED_INSTANCE_GROUP_TEMPLATE.format(name=self.instance_group_name,
                                                                       instance_template_name=template_name,
                                                                       size=node_count,
                                                                       zone=self.zone,
                                                                       network=network_name)
        deployment_resources += FIREWALL_TEMPLATE.format(name=self.name + '-norules',
                                                         network=network_name)
        deployment_resources += '\n'

        self.gce_wrapper.create_deployment(self.name, deployment_resources)

    @property
    def instance_names(self):
        for instance in self.gce_wrapper.list_group_instances(self.instance_group_name, self.zone):
            yield instance['instance'].split('/')[-1]

    @property
    @retry(wait_fixed=2000, retry_on_exception=lambda e: isinstance(e, KeyError), stop_max_attempt_number=7)
    def hosts(self):
        hosts = []
        for name in self.instance_names:
            info = self.gce_wrapper.get_instance_info(name, self.zone)
            hosts.append(Host(private_ip=info['networkInterfaces'][0]['networkIP'],
                              public_ip=info['networkInterfaces'][0]['accessConfigs'][0]['natIP']))
        return hosts

    def delete(self):
        self.gce_wrapper.delete_deployment(self.name)

    def _check_status(response):
        ''' Checks the status of the deployment until it is done or has failed
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

    @retry(wait_fixed=2000, retry_on_result=_check_status, retry_on_exception=lambda _: False)
    def wait(self) -> dict:
        response = self.gce_wrapper.get_deployment_info(self.name)
        errors = response['operation'].get('error')
        if errors:
            raise Exception('The deployment you are accessing contains errors:' + str(errors))
        return response
