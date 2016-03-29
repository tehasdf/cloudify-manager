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
import threading

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

    def _add_node(self, dep_update, entity_id):
        get_blueprints_manager()._create_deployment_nodes(
                deployment_id=dep_update.deployment_id,
                blueprint_id='N/A',
                plan=dep_update.blueprint,
                node_ids=entity_id
        )

        return self.sm.get_node(dep_update.deployment_id, entity_id)

    def _add_relationship(self, dep_update, entity_id):
        source_node_id, target_node_id = entity_id.split(':')
        pluralized_entity_type = _pluralize('node')
        raw_nodes = dep_update.blueprint[pluralized_entity_type]
        source_raw_node = [n for n in raw_nodes
                           if n['id'] == source_node_id][0]
        target_raw_node = [n for n in raw_nodes
                           if n['id'] == target_node_id][0]

        # This currently assures that only new plugins could be inserted,
        # no new implementation of an old plugin is currently allowed
        source_plugins = \
            self.sm.get_node(dep_update.deployment_id, source_node_id).plugins
        current_source_plugins_names = set([n['name'] for n in source_plugins])
        source_plugins.extend(
                [p for p in source_raw_node['plugins']
                 if p['name'] not in current_source_plugins_names])

        target_plugins = \
            self.sm.get_node(dep_update.deployment_id, target_node_id).plugins
        current_target_plugins_names = [n['name'] for n in target_plugins]
        target_plugins.extend(
                [p for p in target_raw_node['plugins']
                 if p['name'] not in current_target_plugins_names])

        source_changes = {
            'relationships': source_raw_node['relationships'],
            'plugins': source_plugins
        }

        target_changes = {
            'plugins': target_plugins
        }

        self.sm.update_node(deployment_id=dep_update.deployment_id,
                            node_id=source_node_id,
                            changes=source_changes)

        self.sm.update_node(deployment_id=dep_update.deployment_id,
                            node_id=target_node_id,
                            changes=target_changes)

        return self.sm.get_node(dep_update.deployment_id, source_node_id)

    def _retrieve_modified_node(self, dep_update, entity_id):
        source_node_id, target_node_id = entity_id.split(':')
        node = self.sm.get_node(dep_update.deployment_id, source_node_id)

        modified_relationship = [r for r in node.relationships
                                 if r['target_id'] == target_node_id][0]

        node.relationships.remove(modified_relationship)

        return node

    def _add_entity(self, dep_update, entity_type, entity_id):

        node = {
            'node': self._add_node,
            'relationship': self._add_relationship
        }[entity_type](dep_update, entity_id)

        return entity_id, node

    def _remove_entity(self, dep_update, entity_type, entity_id):

        node = {
            'node': None,
            'relationship': self._retrieve_modified_node
        }[entity_type](dep_update, entity_id)

        return entity_id, node

    def _handle_node_updates(self, dep_update):
        modified_nodes = []
        entities_updater = {
            'add': self._add_entity,
            'remove': self._remove_entity
        }
        modified_entities = {
            'node': [],
            'relationship': []
        }
        for step in dep_update.steps:
            entity_id, affected_node = \
                entities_updater[step.operation](dep_update,
                                                 step.entity_type,
                                                 step.entity_id)
            modified_nodes.append(affected_node.to_dict())
            modified_entities[step.entity_type].append(entity_id)

        all_nodes = [n.to_dict() for n in self.sm.get_nodes(filters={
            'deployment_id': dep_update.deployment_id}
        ).items]

        modified_node_ids = _extract_node_instance_ids(modified_nodes)

        modified_nodes.extend([n for n in all_nodes
                               if n['id'] not in modified_node_ids])

        return modified_entities, modified_nodes

    def _extract_changes(self, dep_update, modified_nodes):
        deployment_id_filter = \
            {'deployment_id': dep_update.deployment_id}

        # By this point the node_instances aren't updated yet
        node_instances = \
            [instance.to_dict() for instance in
             self.sm.get_node_instances(filters=deployment_id_filter).items]

        # project changes in deployment
        return tasks.modify_deployment(nodes=modified_nodes,
                                       previous_node_instances=node_instances,
                                       modified_nodes=())

    def _update_node_instance(self, node_instance):
        current = self.sm.get_node_instance(node_instance['id'])
        new_relationships = node_instance['relationships']
        self.sm.update_node_instance(models.DeploymentNodeInstance(
                id=node_instance['id'],
                relationships=new_relationships,
                version=current.version,
                node_id=None,
                host_id=None,
                deployment_id=None,
                state=None,
                runtime_properties=None))

    def _apply_node_instance_adding(self, instances, dep_update):
        added_raw_instances = []
        added_related_raw_instances = []

        for node_instance in instances:
            if node_instance.get('modification') == 'added':
                added_raw_instances.append(node_instance)
            else:
                added_related_raw_instances.append(node_instance)
                self._update_node_instance(node_instance)

        get_blueprints_manager()._create_deployment_node_instances(
                dep_update.deployment_id,
                added_raw_instances
        )

        return {
            'affected': added_raw_instances,
            'related': added_related_raw_instances
        }

    def _apply_node_instance_relationship_removing(self, instances, *_):
        modified_raw_instances = []
        modify_related_raw_instances = []

        for node_instance in instances:
            if node_instance.get('modification') == 'modified':
                modified_raw_instances.append(node_instance)
            else:
                modify_related_raw_instances.append(node_instance)

        return {
            'affected': modified_raw_instances,
            'related': modify_related_raw_instances
        }

    def _apply_node_instance_relationship_adding(self, instances, *_):
        modified_raw_instances = []
        modify_related_raw_instances = []

        for node_instance in instances:
            if node_instance.get('modification') == 'modified':
                modified_raw_instances.append(node_instance)
                self._update_node_instance(node_instance)
            else:
                modify_related_raw_instances.append(node_instance)

        return {
                    'affected': modified_raw_instances,
                    'related': modify_related_raw_instances
                }

    def _handle_node_instances_updates(self, dep_update,
                                       updated_instances):
        instance_update_mapper = {
            'added_and_related':
                self._apply_node_instance_adding,
            'extended_and_related':
                self._apply_node_instance_relationship_adding,
            'reduced_and_related':
                self._apply_node_instance_relationship_removing
        }

        raw_instances = {k: {} for k, _ in instance_update_mapper.iteritems()}

        for change_type, applier in instance_update_mapper.iteritems():
            if updated_instances[change_type]:
                raw_instances[change_type] = \
                    applier(updated_instances[change_type], dep_update)

        return raw_instances

    def _execute_update_workflow(self,
                                 dep_update,
                                 node_instances,
                                 modified_entity_ids):

        added_instances = node_instances['added_and_related']
        extended_instances = node_instances['extended_and_related']
        reduced_instances = node_instances['reduced_and_related']
        deleted_instances = node_instances['deleted_and_related']

        instance_ids = {
            # needed in order to finalize the commit
            'update_id': dep_update.id,

            'added_instance_ids':
                _extract_node_instance_ids(added_instances.get('affected')),
            'add_related_instance_ids':
                _extract_node_instance_ids(added_instances.get('related')),

            'modified_entity_ids': modified_entity_ids,

            'extended_instance_ids':
                _extract_node_instance_ids(extended_instances.get('affected')),
            'extend_related_instance_ids':
                _extract_node_instance_ids(extended_instances.get('related')),

            'reduced_instance_ids':
                _extract_node_instance_ids(reduced_instances.get('affected')),
            'reduce_related_instance_ids':
                _extract_node_instance_ids(reduced_instances.get('related')),

            # TODO: support for different types should be added right here
            'removed_instance_ids':
                _extract_node_instance_ids(deleted_instances.get('affected')),
            'remove_related_instnce_ids':
                _extract_node_instance_ids(deleted_instances.get('related'))
        }

        return self.execute_workflow(deployment_id=dep_update.deployment_id,
                                     workflow_id='update',
                                     parameters=instance_ids)

    def finalize_update(self, deployment_update_id):

        dep_update = self.sm.get_deployment_update(deployment_update_id)
        modified_nodes = dep_update.modified_nodes
        modified_node_instances = dep_update.modified_node_instances

        self._finalize_nodes(dep_update,
                             modified_nodes,
                             modified_node_instances)

        self._finalize_node_instances(modified_node_instances)

        # mark deployment update as committed
        dep_update.state = models.DeploymentUpdate.COMMITTED
        self.sm.update_deployment_update(dep_update)

        return models.DeploymentUpdate(deployment_update_id,
                                       dep_update.blueprint)

    def _finalize_nodes(self,
                        dep_update,
                        modified_nodes,
                        modified_node_instances):
        reduced_node_instances = \
            modified_node_instances['reduced_and_related'].get('affected', [])
        deleted_node_instances = \
            modified_node_instances['deleted_and_related'].get('affected', [])

        for reduced_node_instance in reduced_node_instances:
            node = [n for n in modified_nodes
                    if n['id'] == reduced_node_instance['node_id']][0]
            self.sm.update_node(deployment_id=dep_update.deployment_id,
                                node_id=reduced_node_instance['node_id'],
                                changes=node)

        for deleted_node_instance in deleted_node_instances:
            node = [n for n in modified_nodes
                    if n['id'] == deleted_node_instance['node_id']][0]
            self.sm.delete_node(node['id'])

    def _finalize_node_instances(self,
                                 modified_node_instances):
        reduced_node_instances = \
            modified_node_instances['reduced_and_related'].get('affected', [])
        deleted_node_instances = \
            modified_node_instances['deleted_and_related'].get('affected', [])

        for reduced_node_instance in reduced_node_instances:
            self._update_node_instance(reduced_node_instance)

        for deleted_node_instance in deleted_node_instances:
            self.sm.delete_node_instance(deleted_node_instance['id'])

    def commit_deployment_update(self, deployment_update_id):
        dep_update = self.sm.get_deployment_update(deployment_update_id)

        # mark deployment update as committing
        dep_update.state = models.DeploymentUpdate.COMMITTING
        self.sm.update_deployment_update(dep_update)

        # Update the nodes on the storage
        modified_entity_ids, raw_nodes = \
            self._handle_node_updates(dep_update)

        # Extract changes from raw nodes
        node_instance_changes = self._extract_changes(dep_update, raw_nodes)

        # Create (and update for adding step type) node instances
        # according to the changes in raw_nodes
        raw_node_instances = \
            self._handle_node_instances_updates(dep_update,
                                                node_instance_changes)

        # Saving the needed changes back to sm for future use
        # (removing entities).
        dep_update.modified_nodes = raw_nodes
        dep_update.modified_node_instances = raw_node_instances
        self.sm.update_deployment_update(dep_update)

        # Execute update workflow using added and related instances
        # This workflow will call a finalize_update, since removing entities
        # should be done after the executions.
        # The raw_node_instances are being used only for their ids, Thus
        # They should really hold the finished version for the node instance.
        self._execute_update_workflow(dep_update,
                                      raw_node_instances,
                                      modified_entity_ids)

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

    source_id, target_id = entity_id.split(':')

    source_node = [n for n in nodes if n['id'] == source_id][0]

    conditions = [n['id'] for n in nodes if n['id'] == target_id]
    conditions += filter(lambda r: r['target_id'] == target_id,
                         source_node['relationships'])

    return any(conditions)


def _validate_node_entity_id(blueprint, entity_id):
    return entity_id in [e['id'] for e in blueprint['nodes']]

validation_mapper = {
    'node': _validate_node_entity_id,
    'relationship': _validate_relationship_entity_id
}
