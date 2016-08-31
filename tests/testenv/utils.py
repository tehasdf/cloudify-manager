#########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import json
import os
import sys
import uuid
import time
import tempfile
import tarfile
import shutil
from os import path
from functools import wraps
from multiprocessing import Process

import elasticsearch
import pika
import sh
from wagon import wagon
from celery import Celery

from cloudify.utils import setup_logger
from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.executions import Execution
from manager_rest.storage.es_storage_manager import ESStorageManager

from testenv import constants

PROVIDER_CONTEXT = {
    'cloudify': {
        'workflows': {
            'task_retries': 0,
            'task_retry_interval': 0,
            'subgraph_retries': 0
        }
    }
}
PROVIDER_NAME = 'integration_tests'

logger = setup_logger('testenv.utils')


def get_manager_ip():
    return os.environ[constants.DOCL_CONTAINER_IP]


def create_rest_client():
    return CloudifyClient(host=get_manager_ip(), port=80)


def create_es_db_client():
    return ESStorageManager(host=get_manager_ip(), port=9200)


def create_es_client():
    return elasticsearch.Elasticsearch(hosts=[{'host': get_manager_ip(),
                                               'port': 9200}])


def create_pika_connection():
    credentials = pika.credentials.PlainCredentials(
        username='cloudify',
        password='c10udify')
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=get_manager_ip(),
                                  credentials=credentials))


def create_celery_client():
    celery = Celery(broker='amqp://cloudify:c10udify@{0}'
                    .format(get_manager_ip()),
                    backend='amqp://cloudify:c10udify@{0}'
                    .format(get_manager_ip()))
    celery.conf.update(
        CELERY_TASK_SERIALIZER="json",
        CELERY_TASK_RESULT_EXPIRES=600)
    return celery


def get_cfy():
    return sh.cfy.bake(_err_to_out=True,
                       _out=lambda l: sys.stdout.write(l),
                       _tee=True)


def deploy_application(dsl_path,
                       timeout_seconds=30,
                       blueprint_id=None,
                       deployment_id=None,
                       wait_for_execution=True,
                       inputs=None):
    """
    A blocking method which deploys an application from the provided dsl path.
    """
    return deploy_and_execute_workflow(dsl_path=dsl_path,
                                       workflow_name='install',
                                       timeout_seconds=timeout_seconds,
                                       blueprint_id=blueprint_id,
                                       deployment_id=deployment_id,
                                       wait_for_execution=wait_for_execution,
                                       inputs=inputs)


def deploy(dsl_path, blueprint_id=None, deployment_id=None, inputs=None):
    client = create_rest_client()
    if not blueprint_id:
        blueprint_id = str(uuid.uuid4())
    blueprint = client.blueprints.upload(dsl_path, blueprint_id)
    if deployment_id is None:
        deployment_id = str(uuid.uuid4())
    deployment = client.deployments.create(
        blueprint.id,
        deployment_id,
        inputs=inputs)

    wait_for_deployment_creation_to_complete(
        deployment_id=deployment_id)
    return deployment


def wait_for_deployment_creation_to_complete(
        deployment_id, timeout_seconds=30):
    do_retries(func=verify_deployment_environment_creation_complete,
               timeout_seconds=timeout_seconds,
               deployment_id=deployment_id)


def deploy_and_execute_workflow(dsl_path,
                                workflow_name,
                                timeout_seconds=240,
                                blueprint_id=None,
                                deployment_id=None,
                                wait_for_execution=True,
                                parameters=None,
                                inputs=None):
    """
    A blocking method which deploys an application from the provided dsl path.
    and runs the requested workflows
    """
    deployment = deploy(dsl_path, blueprint_id, deployment_id, inputs)
    execution = execute_workflow(workflow_name, deployment.id, parameters,
                                 timeout_seconds, wait_for_execution)
    return deployment, execution.id


def execute_workflow(workflow_name, deployment_id,
                     parameters=None,
                     timeout_seconds=240,
                     wait_for_execution=True):
    """
    A blocking method which runs the requested workflow
    """
    client = create_rest_client()

    execution = client.executions.start(deployment_id, workflow_name,
                                        parameters=parameters or {})

    if wait_for_execution:
        wait_for_execution_to_end(execution,
                                  timeout_seconds=timeout_seconds)

    return execution


def verify_deployment_environment_creation_complete(deployment_id):
    # a workaround for waiting for the deployment environment creation to
    # complete
    client = create_rest_client()
    execs = client.executions.list(deployment_id)
    if not execs \
            or execs[0].status != Execution.TERMINATED \
            or execs[0].workflow_id != 'create_deployment_environment':
        # cyclic imports :(
        from testenv import docl
        log_path = ('/var/log/cloudify/mgmtworker/'
                    'cloudify.management_worker.log')
        logs = docl.execute('tail -n 100 {0}'.format(log_path))
        raise RuntimeError(
            "Expected a single execution for workflow "
            "'create_deployment_environment' with status 'terminated'; "
            "Found these executions instead: {0}.\nLast 100 lines for "
            "management worker log:\n{1}".format(
                json.dumps(execs.items, indent=2), logs))


def undeploy_application(deployment_id,
                         timeout_seconds=240,
                         is_delete_deployment=False,
                         parameters=None):
    """
    A blocking method which undeploys an application from the provided dsl
    path.
    """
    client = create_rest_client()
    execution = client.executions.start(deployment_id,
                                        'uninstall',
                                        parameters=parameters)
    wait_for_execution_to_end(execution, timeout_seconds=timeout_seconds)

    if execution.error and execution.error != 'None':
        raise RuntimeError(
            'Workflow execution failed: {0}'.format(execution.error))
    if is_delete_deployment:
        time.sleep(5)  # elasticsearch...
        delete_deployment(deployment_id)


def delete_deployment(deployment_id, ignore_live_nodes=False):
    client = create_rest_client()
    return client.deployments.delete(deployment_id,
                                     ignore_live_nodes=ignore_live_nodes)


def is_node_started(node_id):
    client = create_rest_client()
    node_instance = client.node_instances.get(node_id)
    return node_instance['state'] == 'started'


def get_resource(resource):

    """
    Gets the path for the provided resource.
    :param resource: resource name relative to /resources.
    """
    import resources
    resources_path = path.dirname(resources.__file__)
    resource_path = path.join(resources_path, resource)
    if not path.exists(resource_path):
        raise RuntimeError("Resource '{0}' not found in: {1}".format(
            resource, resource_path))
    return resource_path


def wait_for_execution_to_end(execution, timeout_seconds=240):
    client = create_rest_client()
    deadline = time.time() + timeout_seconds
    while execution.status not in Execution.END_STATES:
        time.sleep(0.5)
        execution = client.executions.get(execution.id)
        if time.time() > deadline:
            raise TimeoutException('Execution timed out: \n{0}'
                                   .format(json.dumps(execution, indent=2)))
    if execution.status == Execution.FAILED:
        raise RuntimeError(
            'Workflow execution failed: {0} [{1}]'.format(execution.error,
                                                          execution.status))
    return execution


def do_retries(func,
               timeout_seconds=10,
               exception_class=BaseException,
               **kwargs):
    deadline = time.time() + timeout_seconds
    while True:
        try:
            return func(**kwargs)
        except exception_class:
            if time.time() > deadline:
                raise
            time.sleep(0.5)


def do_retries_boolean(func, timeout_seconds=10, **kwargs):
    deadline = time.time() + timeout_seconds
    while True:
            return_value = func(**kwargs)
            if return_value:
                break
            else:
                if time.time() > deadline:
                    raise RuntimeError(
                        'function {0} did not return True in {1} seconds'
                        .format(func.__name__, timeout_seconds)
                    )
                time.sleep(1)


def timeout(seconds=60):
    def decorator(func):
        def wrapper(*args, **kwargs):
            process = Process(None, func, None, args, kwargs)
            process.start()
            process.join(seconds)
            if process.is_alive():
                process.terminate()
                raise TimeoutException(
                    'test timeout exceeded [timeout={0}'.format(seconds))
        return wraps(func)(wrapper)
    return decorator


def send_task(task, queue, args=None):
    celery = create_celery_client()
    task_name = task.name.replace('mock_plugins.', '')
    return celery.send_task(
        name=task_name,
        args=args,
        queue=queue)


def publish_event(queue,
                  routing_key,
                  event,
                  exchange_name='cloudify-monitoring',
                  exchange_type='topic'):
    connection = create_pika_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name,
                             type=exchange_type,
                             durable=False,
                             auto_delete=True,
                             internal=False)
    channel.queue_declare(
        queue=queue,
        auto_delete=True,
        durable=False,
        exclusive=False)
    channel.queue_bind(exchange=exchange_name,
                       queue=queue,
                       routing_key=routing_key)
    channel.basic_publish(exchange=exchange_name,
                          routing_key=routing_key,
                          body=json.dumps(event))
    channel.close()
    connection.close()


def delete_provider_context():
    from testenv.services.postgresql import run_query
    run_query("DELETE from provider_context", 'cloudify')


def restore_provider_context():
    delete_provider_context()
    client = create_rest_client()
    client.manager.create_context(PROVIDER_NAME, PROVIDER_CONTEXT)


def timestamp():
    now = time.strftime("%c")
    return now.replace(' ', '-')


def tar_blueprint(blueprint_path, dest_dir):
    """
    creates a tar archive out of a blueprint dir.

    :param blueprint_path: the path to the blueprint.
    :param dest_dir: destination dir for the path
    :return: the path for the dir.
    """
    blueprint_path = os.path.expanduser(blueprint_path)
    app_name = os.path.basename(os.path.splitext(blueprint_path)[0])
    blueprint_directory = os.path.dirname(blueprint_path) or os.getcwd()
    return tar_file(blueprint_directory, dest_dir, app_name)


def tar_file(file_to_tar, destination_dir, tar_name=''):
    """
    tar a file into a destination dir.
    :param file_to_tar:
    :param destination_dir:
    :param tar_name: optional tar name.
    :return:
    """
    tar_name = tar_name or os.path.basename(file_to_tar)
    tar_path = os.path.join(destination_dir, '{0}.tar.gz'.format(tar_name))
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(file_to_tar, arcname=tar_name)
    return tar_path


class TimeoutException(Exception):

    def __init__(self, message):
        Exception.__init__(self, message)

    def __str__(self):
        return self.message


def upload_plugin(package_name, package_version):
    client = create_rest_client()
    temp_file_path = _create_mock_wagon(package_name, package_version)
    response = client.plugins.upload(temp_file_path)
    os.remove(temp_file_path)
    return response


def _create_mock_wagon(package_name, package_version):
    module_src = tempfile.mkdtemp(
        prefix='plugin-{0}-'.format(package_name))
    try:
        with open(os.path.join(module_src, 'setup.py'), 'w') as f:
            f.write('from setuptools import setup\n')
            f.write('setup(name="{0}", version={1})'.format(
                package_name, package_version))
        wagon_client = wagon.Wagon(module_src)
        result = wagon_client.create(
            archive_destination_dir=tempfile.gettempdir(),
            force=True)
    finally:
        shutil.rmtree(module_src)
    return result
