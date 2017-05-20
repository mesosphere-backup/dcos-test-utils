import os
import json
import logging
from googleapiclient.errors import HttpError
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from retrying import retry

log = logging.getLogger(__name__)


def catch_http_exceptions(f):
    def handle_exception(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except HttpError as e:
            if 'HttpError 404' in str(e):
                raise Exception("The resource you are trying to access doesn't exist")
            elif 'HttpError 409' in str(e):
                raise Exception('''The resource you are trying to access is either being created (operation conflict) or
                    already exists''')

    return handle_exception


class GceWrapper:
    def __init__(self, credentials_path=None):
        if credentials_path is None:
            if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
                raise Exception('''You must either set the GOOGLE_APPLICATION_CREDENTIALS environment
                    variable or supply a credentials path to the GceWrapper constructor.''')    
        elif not os.path.isfile(credentials_path):
            raise Exception(credentials_path + ' is not a valid google credentials path.')
        else:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        credentials = GoogleCredentials.get_application_default()
        self.compute = discovery.build('compute', 'v1', credentials=credentials)
        self.deployment_manager = discovery.build('deploymentmanager', 'v2', credentials=credentials)

        with open(credentials_path, 'r') as creds:
            self.project_id = json.load(creds)['project_id']

    def deploy_instances(self, config):
        body = {
            'name': config['deployment_name'],
            'target': {
                'config': {
                    'content': config['template_content']
                }
            }
        }

        request = self.deployment_manager.deployments().insert(project=self.project_id, body=body)
        response = request.execute()
        return response

    def list_deployment_instances(self, deployment):
        request = self.compute.instances().list(project=self.project_id, zone=deployment.zone)
        instance_info_list = []

        while request is not None:
            response = request.execute()
            for instance_info in response['items']:
                instance_info_list.append(instance_info)

            request = self.deployment_manager.resources().list_next(previous_request=request, previous_response=response)

        return instance_info_list

    def get_deployment_info(self, deployment):
        request = self.deployment_manager.deployments().get(project=self.project_id, deployment=deployment.name)
        response = request.execute()
        return response

    def delete_deployment(self, deployment):
        request = self.deployment_manager.deployments().delete(project=self.project_id, deployment=deployment.name)
        response = request.execute()
        return response

    def set_metadata(self, deployment, body, instance_name):
        request = self.compute.instances().setMetadata(project=self.project_id, zone=deployment.zone,
                                                       instance=instance_name, body=body)
        response = request.execute()
        return response


class Instance:
    def __init__(self, gce_wrapper, name, priv_ip, pub_ip):
        self.gce_wrapper = gce_wrapper
        self.name = name
        self.priv_ip = priv_ip
        self.pub_ip = pub_ip

    def get_private_public_ip(self):
        return {
            'private_ip': self.priv_ip,
            'public_ip': self.pub_ip}


class Deployment:
    def __init__(self, gce_wrapper, name, zone):
        self.gce_wrapper = gce_wrapper
        self.name = name
        self.zone = zone
        self.instances = []
        self.fingerprint = None

    @catch_http_exceptions
    def get_instances(self):
        if not self.instances:
            instance_info_list = self.gce_wrapper.list_deployment_instances(self)
            for i in instance_info_list:
                self.instances.append(Instance(self.gce_wrapper, i['name'],
                                               i['networkInterfaces'][0]['networkIP'],
                                               i['networkInterfaces'][0]['accessConfigs'][0]['natIP']))
        return self.instances

    @catch_http_exceptions
    def get_fingerprint(self):
        if not self.fingerprint:
            self.fingerprint = self.get_info()['fingerprint']
        return self.fingerprint

    @catch_http_exceptions
    def get_host_ips(self):
        return [i.get_private_public_ip() for i in self.instances]

    @catch_http_exceptions
    def apply_ssh_key(self, public_key):
        body = {
            "kind": "compute#metadata",
            'fingerprint': self.get_fingerprint(),
            'items': [
                {
                    'key': 'ssh-keys',
                    'value': 'dcos:ssh-rsa ' + public_key + ' dcos'
                }
            ]
        }

        for i in self.get_instances():
            yield self.gce_wrapper.set_metadata(self, body, i['name'])

    @catch_http_exceptions
    def get_info(self):
        return self.gce_wrapper.get_deployment_info(self)

    @catch_http_exceptions
    def delete(self):
        return self.gce_wrapper.delete_deployment(self)

    def _check_status(response):
        status = response['operation']['status']
        if status == 'DONE':
            return False
        elif status == 'RUNNING' or status == 'PENDING':
            print('Waiting for deployment')
            return True
        else:
            raise Exception('Deployment failed with response: ' + str(response))

    @catch_http_exceptions
    @retry(wait_fixed=2000, retry_on_result=_check_status, stop_max_attempt_number=15)
    def wait(self):
        response = self.get_info()
        errors = response['operation'].get('errors')
        if errors:
            self.delete()
            raise Exception('Deployment was deleted because it failed with these errors: ' + errors)
        return response

    def get_instances_info(self):
        return self.gce_wrapper.list_deployment_instances(self)
