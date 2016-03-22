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
import uuid

from datetime import datetime
from flask import current_app

from manager_rest import storage_manager
from manager_rest import models

from manager_rest.blueprints_manager import tasks, BlueprintsManager
from manager_rest.blueprints_manager import get_blueprints_manager
import manager_exceptions
import manager_rest.workflow_client as wf_client

from dsl_parser import constants


class DeploymentUpdateManager(object):

    def __init__(self):
        self.sm = storage_manager.get_storage_manager()
        self.workflow_client = wf_client.get_workflow_client()

    def get_deployment_update(self, deployment_update_id, include=None):
        return self.sm.get_deployment_update(deployment_update_id,
                                             include=include)

    def deployment_updates_list(self, include=None, filters=None,
                                pagination=None, sort=None):
        return self.sm.deployment_updates_list(include=include,
                                               filters=filters,
                                               pagination=pagination,
                                               sort=sort)

    def stage_deployment_update(self, deployment_id, staged_blueprint):

        self._validate_no_active_updates_per_deployment(deployment_id)

        deployment_update = models.DeploymentUpdate(deployment_id,
                                                    staged_blueprint)
        self.sm.put_deployment_update(deployment_update)
        return deployment_update

    def create_deployment_update_step(self, deployment_update_id,
                                      operation, entity_type, entity_id):
        step = models.DeploymentUpdateStep(operation,
                                           entity_type,
                                           entity_id)
        dep_update = self.sm.get_deployment_update(deployment_update_id)

        self._validate_entity_id(dep_update.blueprint,
                                 entity_type,
                                 entity_id)

        self.sm.put_deployment_update_step(deployment_update_id, step)
        return step

    def _validate_no_active_updates_per_deployment(self, deployment_id):
        """
        Validate there are no uncommitted updates for provided deployment.
        raises conflict error if there are.
        :param deployment_id: deployment id
        """
        existing_updates = \
            self.deployment_updates_list(filters={
                'deployment_id': deployment_id
            }).items

        active_update = \
            next(iter(
                [u for u in existing_updates
                 if u.state != models.DeploymentUpdate.COMMITTED]), None)

        if active_update:
            raise manager_exceptions.ConflictError(
                'deployment update {} is not committed yet'
                .format(active_update.id)
            )

    @staticmethod
    def _validate_entity_id(blueprint, entity_type, entity_id):
        """
        validate an entity id of provided type exists in provided blueprint.
        raises error if id doesn't exist
        :param blueprint: a blueprint (plan)
        :param entity_type: singular entity type name, e.g. 'node'
        :param entity_id: id of the entity, e.g. 'node1'
        """
        validator = validation_mapper[entity_type]

        if validator(blueprint, entity_id):
            return
        else:
            raise manager_exceptions.UnknownModificationStageError(
                    "entity id {} doesn't exist".format(entity_id))

    @staticmethod
    def _add_node(dep_update, entity_id):
        get_blueprints_manager()._create_deployment_nodes(
                deployment_id=dep_update.deployment_id,
                blueprint_id='N/A',
                plan=dep_update.blueprint,
                node_ids=entity_id
        )

    def _add_relationship(self, dep_update, entity_id):
        source_node = entity_id.split(':')[0]
        pluralized_entity_type = _pluralize('node')
        modified_raw_node = \
            [n for n in dep_update.blueprint[pluralized_entity_type]
             if n['id'] == source_node][0]

        change = {
            'relationships': modified_raw_node['relationships'],
            'plugins': modified_raw_node['plugins']
        }

        self.sm.update_node(deployment_id=dep_update.deployment_id,
                            node_id=source_node,
                            changes=change)

    def _add_entity(self, dep_update, entity_type, entity_id):

        {
            'node': self._add_node,
            'relationship': self._add_relationship
        }[entity_type](dep_update, entity_id)

    def _update_nodes(self, dep_update):
        for step in dep_update.steps:
            if step.operation == 'add':
                self._add_entity(dep_update,
                                 step.entity_type,
                                 step.entity_id)

    def _extract_changes(self, dep_update):
        deployment_id_filter = \
            {'deployment_id': dep_update.deployment_id}

        # By this point the node_instances aren't updated yet
        node_instances = \
            [instance.to_dict() for instance in
             self.sm.get_node_instances(filters=deployment_id_filter).items]

        # By this point the nodes should be updated
        nodes = [node.to_dict() for node in
                 self.sm.get_nodes(filters=deployment_id_filter).items]

        # project changes in deployment
        return tasks.modify_deployment(nodes=nodes,
                                       previous_node_instances=node_instances,
                                       modified_nodes=())

    def _apply_node_instance_adding(self, instances, dep_update):
        added_raw_instances = []
        added_related_raw_instances = []

        for node_instance in instances:
            if node_instance.get('modification') == 'added':
                added_raw_instances.append(node_instance)
            else:
                added_related_raw_instances.append(node_instance)
                current = self.sm.get_node_instance(node_instance['id'])
                new_relationships = current.relationships
                new_relationships += node_instance['relationships']
                self.sm.update_node_instance(models.DeploymentNodeInstance(
                        id=node_instance['id'],
                        relationships=new_relationships,
                        version=current.version,
                        node_id=None,
                        host_id=None,
                        deployment_id=None,
                        state=None,
                        runtime_properties=None))

        get_blueprints_manager()._create_deployment_node_instances(
                dep_update.deployment_id,
                added_raw_instances
        )

        return {
            'affected': added_raw_instances,
            'related': added_related_raw_instances
        }

    def _apply_node_instnce_modifying(self, instances, dep_update):
        modified_raw_instances = []
        modify_related_raw_instances = []

        for node_instance in instances:
            if node_instance.get('modification') == 'modified':
                modified_raw_instances.append(node_instance)
                current = self.sm.get_node_instance(node_instance['id'])
                new_relationships = current.relationships
                new_relationships += node_instance['relationships']
                self.sm.update_node_instance(models.DeploymentNodeInstance(
                        id=node_instance['id'],
                        relationships=new_relationships,
                        version=current.version,
                        node_id=None,
                        host_id=None,
                        deployment_id=None,
                        state=None,
                        runtime_properties=None))
            else:
                modify_related_raw_instances.append(node_instance)

        return {
            'affected': modified_raw_instances,
            'related': modify_related_raw_instances
        }

    def _update_node_instnces(self, dep_update, updated_instances):
        instance_update_mapper = {
            'added_and_related':
                self._apply_node_instance_adding,
            'modified_and_related':
                self._apply_node_instnce_modifying,
        }

        raw_instances = {k: {} for k, _ in instance_update_mapper.iteritems()}

        for change_type, func in instance_update_mapper.iteritems():
            if updated_instances[change_type]:
                raw_instances[change_type] = \
                    func(updated_instances[change_type], dep_update)

        return raw_instances

    def _execute_update_workflow(self, dep_update, node_instances):

        instance_ids = {
            'added_instance_ids': _extract_node_instance_ids(
                    node_instances['added_and_related'].get('affected')),
            'add_related_instance_ids': _extract_node_instance_ids(
                    node_instances['added_and_related'].get('related')),
            'modified_instance_ids': _extract_node_instance_ids(
                    node_instances['modified_and_related'].get('affected')),
            # TODO: support for different types should be added right here
            # 'modify_related_instance_ids': (),
            # 'removed_instance_ids': (),
            # 'remove_related_instnce_ids': ()
        }
        self.execute_workflow(deployment_id=dep_update.deployment_id,
                              workflow_id='update',
                              parameters=instance_ids)

    def commit_deployment_update(self, deployment_update_id):
        dep_update = self.sm.get_deployment_update(deployment_update_id)

        # mark deployment update as committing
        dep_update.state = models.DeploymentUpdate.COMMITTING
        self.sm.update_deployment_update(dep_update)

        # Update the nodes on the storage
        self._update_nodes(dep_update)

        # Extract changes from updated notes
        changes = self._extract_changes(dep_update)

        # Update node instances according to the changes
        raw_node_instances = self._update_node_instnces(dep_update,
                                                        changes)

        # execute update workflow using added and related instances
        self._execute_update_workflow(dep_update, raw_node_instances)

        # mark deployment update as committed
        dep_update.state = models.DeploymentUpdate.COMMITTED
        self.sm.update_deployment_update(dep_update)

        return models.DeploymentUpdate(deployment_update_id,
                                       dep_update.blueprint)

    def execute_workflow(self, deployment_id, workflow_id,
                         parameters=None,
                         allow_custom_parameters=False, force=False):
        deployment = self.sm.get_deployment(deployment_id)
        blueprint = self.sm.get_blueprint(deployment.blueprint_id)

        if workflow_id not in deployment.workflows:
            raise manager_exceptions.NonexistentWorkflowError(
                'Workflow {0} does not exist in deployment {1}'.format(
                    workflow_id, deployment_id))
        workflow = deployment.workflows[workflow_id]

        execution_parameters = \
            BlueprintsManager._merge_and_validate_execution_parameters(
                workflow, workflow_id, parameters, allow_custom_parameters)

        execution_id = str(uuid.uuid4())

        new_execution = models.Execution(
            id=execution_id,
            status=models.Execution.PENDING,
            created_at=str(datetime.now()),
            blueprint_id=deployment.blueprint_id,
            workflow_id=workflow_id,
            deployment_id=deployment_id,
            error='',
            parameters=BlueprintsManager._get_only_user_execution_parameters(
                execution_parameters),
            is_system_workflow=False)

        self.sm.put_execution(new_execution.id, new_execution)

        # executing the user workflow
        workflow_plugins = blueprint.plan[
            constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.workflow_client.execute_workflow(
            workflow_id,
            workflow,
            workflow_plugins=workflow_plugins,
            blueprint_id=deployment.blueprint_id,
            deployment_id=deployment_id,
            execution_id=execution_id,
            execution_parameters=execution_parameters)

        return new_execution


# What we need to access this manager in Flask
def get_deployment_updates_manager():
    """
    Get the current app's deployment updates manager, create if necessary
    """
    manager = current_app.config.get('deployment_updates_manager')
    if not manager:
        current_app.config['deployment_updates_manager'] = \
            DeploymentUpdateManager()
        manager = current_app.config.get('deployment_updates_manager')
    return manager


def _pluralize(input):
    return '{}s'.format(input)


def _extract_node_instance_ids(raw_node_instances):
    if raw_node_instances:
        return [instance['id'] for instance in raw_node_instances]
    else:
        return []


def _validate_relationship_entity_id(blueprint, entity_id):
    nodes = blueprint['nodes']
    if ':' not in entity_id:
        return False

    source, target = entity_id.split(':')

    source_nodes = [n for n in nodes if n['id'] == source]

    if len(source_nodes) != 1:
        return False

    source_node = source_nodes[0]

    return any(filter(lambda r: r['target_id'] == target,
                      source_node['relationships']))


def _validate_node_entity_id(blueprint, entity_id):
    return entity_id in [e['id'] for e in blueprint['nodes']]

validation_mapper = {
    'node': _validate_node_entity_id,
    'relationship': _validate_relationship_entity_id
}
