import logging

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from retrying import retry

from dcos_test_utils.helpers import Host

log = logging.getLogger(__name__)


class Instance:
    def __init__(self, name, priv_ip, pub_ip):
        self.name = name
        self.private_ip = priv_ip
        self.public_ip = pub_ip
        self.host = Host(self.private_ip, self.public_ip)


class Deployment:
    def __init__(self, gce_wrapper, name, zone):
        self.gce_wrapper = gce_wrapper
        self.name = name
        self.zone = zone
        self.instances = []
        self.fingerprint = None
        self.errors = None

    def get_instances(self):
        if not self.instances:
            instance_info_list = self.gce_wrapper.list_deployment_instances(self)
            log.debug(instance_info_list)
            for i in instance_info_list:
                self.instances.append(Instance(i['name'],
                                               i['networkInterfaces'][0]['networkIP'],
                                               i['networkInterfaces'][0]['accessConfigs'][0]['natIP']))
        return self.instances

    def get_fingerprint(self):
        if not self.fingerprint:
            self.fingerprint = self.get_instances_info()[0]['metadata']['fingerprint']
        log.debug('fingerprint: ' + self.fingerprint)
        return self.fingerprint

    def get_host_ips(self):
        return [i.host for i in self.get_instances()]

    def apply_ssh_key(self, user, public_key):
        body = {
            'kind': 'compute#metadata',
            'fingerprint': self.get_fingerprint(),
            'items': [
                {
                    'key': 'ssh-keys',
                    'value': '{}:{}'.format(user, public_key)
                }
            ]
        }

        for i in self.get_instances():
            log.debug(self.gce_wrapper.set_metadata(self, body, i.name))

    def get_info(self):
        response = self.gce_wrapper.get_deployment_info(self)
        log.debug(response)
        self.errors = response['operation'].get('error')
        return response

    def delete(self):
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
    def wait(self):
        response = self.get_info()
        log.debug(response)
        return response

    def get_instances_info(self):
        response = self.gce_wrapper.list_deployment_instances(self)
        log.debug(response)
        return response

    def allow_external_access(self):
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

    @catch_http_exceptions
    def deploy_instances(self, config):
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
    def list_deployment_instances(self, deployment: Deployment):
        request = self.compute.instances().list(project=self.project_id, zone=deployment.zone)
        instance_info_list = []

        while request is not None:
            response = request.execute()
            for instance_info in response['items']:
                instance_info_list.append(instance_info)

            request = self.deployment_manager.resources().list_next(previous_request=request, previous_response=response)

        return instance_info_list

    @catch_http_exceptions
    def get_deployment_info(self, deployment: Deployment):
        return self.deployment_manager.deployments().get(project=self.project_id, deployment=deployment.name).execute()

    @catch_http_exceptions
    def delete_deployment(self, deployment: Deployment):
        return self.deployment_manager.deployments().delete(project=self.project_id, deployment=deployment.name).execute()

    @catch_http_exceptions
    def set_metadata(self, deployment: Deployment, body, instance_name):
        return self.compute.instances().setMetadata(project=self.project_id, zone=deployment.zone,
                                                    instance=instance_name, body=body).execute()

    @catch_http_exceptions
    def add_firewall_rule(self, body):
        return self.compute.firewalls().insert(project=self.project_id, body=body)
