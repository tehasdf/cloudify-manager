########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import glob
import os
import shutil

from cloudify.decorators import workflow
from cloudify.workflows import tasks as workflow_tasks
from cloudify.workflows import workflow_context


def generate_create_dep_tasks_graph(ctx,
                                    deployment_plugins_to_install,
                                    workflow_plugins_to_install,
                                    policy_configuration=None):
    graph = ctx.graph_mode()
    sequence = graph.sequence()

    dep_plugins = [p for p in deployment_plugins_to_install if p['install']]
    wf_plugins = [p for p in workflow_plugins_to_install if p['install']]
    plugins_to_install = dep_plugins + wf_plugins
    if plugins_to_install:
        sequence.add(
            ctx.send_event('Installing deployment plugins'),
            ctx.execute_task('cloudify_agent.operations.install_plugins',
                             kwargs={'plugins': plugins_to_install}))

    sequence.add(
        ctx.send_event('Starting deployment policy engine core'),
        ctx.execute_task('riemann_controller.tasks.create',
                         kwargs=policy_configuration or {}))

    sequence.add(
        ctx.send_event('Creating deployment work directory'),
        ctx.local_task(_create_deployment_workdir,
                       kwargs={'deployment_id': ctx.deployment.id}))

    return graph


@workflow
def create(ctx,
           deployment_plugins_to_install,
           workflow_plugins_to_install,
           policy_configuration, **_):
    graph = generate_create_dep_tasks_graph(
        ctx,
        deployment_plugins_to_install,
        workflow_plugins_to_install,
        policy_configuration)
    return graph.execute()


@workflow
def delete(ctx,
           deployment_plugins_to_uninstall,
           workflow_plugins_to_uninstall,
           **kwargs):
    graph = ctx.graph_mode()
    sequence = graph.sequence()

    dep_plugins = [p for p in deployment_plugins_to_uninstall if p['install']]
    wf_plugins = [p for p in workflow_plugins_to_uninstall if p['install']]
    plugins_to_uninstall = dep_plugins + wf_plugins
    if plugins_to_uninstall:
        sequence.add(
            ctx.send_event('Uninstalling deployment plugins'),
            ctx.execute_task(
                task_name='cloudify_agent.operations.uninstall_plugins',
                kwargs={'plugins': plugins_to_uninstall}),
            ctx.send_event('Stopping deployment policy engine core'),
            ctx.execute_task('riemann_controller.tasks.delete'),
            ctx.send_event('Deleting deployment work directory'),
            ctx.local_task(_delete_deployment_workdir,
                           kwargs={'deployment_id': ctx.deployment.id}))

    for task in graph.tasks_iter():
        _ignore_task_on_fail_and_send_event(task, ctx)

    return graph.execute()


@workflow(system_wide=True)
def delete_logs(ctx, deployment_id):
    log_dir = os.environ.get('CELERY_LOG_DIR')
    if log_dir:
        log_file_path = os.path.join(log_dir, 'logs',
                                     '{0}.log'.format(deployment_id))
        if os.path.exists(log_file_path):
            try:
                with open(log_file_path, 'w') as f:
                    # Truncating instead of deleting because the logging
                    # server currently holds a file descriptor open to this
                    # file. If we delete the file, the logs for new
                    # deployments that get created with the same deployment
                    # id, will get written to a stale file descriptor and
                    # will essentially be lost.
                    f.truncate()
            except IOError:
                ctx.logger.warn(
                        'Failed truncating {0}.'.format(log_file_path,
                                                        exc_info=True))
        for rotated_log_file_path in glob.glob('{0}.*'.format(
                log_file_path)):
            try:
                os.remove(rotated_log_file_path)
            except IOError:
                ctx.logger.exception(
                        'Failed removing rotated log file {0}.'.format(
                                rotated_log_file_path, exc_info=True))


def _ignore_task_on_fail_and_send_event(task, ctx):
    def failure_handler(tsk):
        ctx.send_event('Ignoring task {0} failure'.format(tsk.name))
        return workflow_tasks.HandlerResult.ignore()
    task.on_failure = failure_handler


@workflow_context.task_config(send_task_events=False)
def _create_deployment_workdir(deployment_id):
    os.makedirs(_workdir(deployment_id))


@workflow_context.task_config(send_task_events=False)
def _delete_deployment_workdir(deployment_id):
    shutil.rmtree(_workdir(deployment_id), ignore_errors=True)


def _workdir(deployment_id):
    base_workdir = os.environ['CELERY_WORK_DIR']
    return os.path.join(base_workdir, 'deployments', deployment_id)
