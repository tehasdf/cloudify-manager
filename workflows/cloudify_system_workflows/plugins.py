########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

from cloudify.decorators import workflow

from cloudify_system_workflows import ignore_tasks_on_fail_and_send_event


@workflow(system_wide=True)
def install(ctx, plugin):
    graph = ctx.graph_mode()
    graph.add_task(
        ctx.execute_task('cloudify_agent.operations.install_plugins',
                         kwargs={'plugins': [plugin]}))
    return graph.execute()


@workflow(system_wide=True)
def uninstall(ctx, plugin):
    graph = ctx.graph_mode()
    graph.add_task(
        ctx.execute_task('cloudify_agent.operations.uninstall_plugins',
                         kwargs={'plugins': [plugin]}))
    ignore_tasks_on_fail_and_send_event(graph, ctx)
    return graph.execute()
