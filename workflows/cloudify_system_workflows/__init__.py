#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


from cloudify.workflows import tasks


def ignore_tasks_on_fail_and_send_event(graph, ctx):
    for task in graph.tasks_iter():
        def failure_handler(tsk):
            ctx.send_event('Ignoring task {0} failure'.format(tsk.name))
            return tasks.HandlerResult.ignore()
        task.on_failure = failure_handler
