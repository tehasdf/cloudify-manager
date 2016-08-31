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

import jsonpickle
from flask_sqlalchemy import SQLAlchemy

from manager_rest.deployment_update.constants import ACTION_TYPES, ENTITY_TYPES


db = SQLAlchemy()


class UTCDateTime(db.TypeDecorator):

    impl = db.DateTime

    def process_result_value(self, value, engine):
        # Adhering to the same norms used in the rest of the code
        if value is not None:
            return '{0}Z'.format(value.isoformat()[:-3])


def _foreign_key_column(parent_table, id_col_name='id', nullable=False):
    """Return a ForeignKey object with the relevant

    :param parent_table: SQL name of the parent table
    :param id_col_name: Name of the parent table's ID column [default: `id`]
    :param nullable: Should the column be allowed to remain empty
    :return:
    """
    return db.Column(
        db.Text,
        db.ForeignKey(
            '{0}.{1}'.format(parent_table.__tablename__, id_col_name),
            ondelete='CASCADE'
        ),
        nullable=nullable
    )


def _relationship(
        child_class_name,
        column_name,
        parent_class_name,
        child_table_name,
        parent_id_name='id'
):
    """Return an SQL relationship object
    Meant to be used from the inside the *child* object

    :param child_class_name: Class name of the child table
    :param column_name: Name of the column pointing to the parent table
    :param parent_class_name: Class name of the parent table
    :param child_table_name: SQL name of the parent table
    :param parent_id_name: Name of the parent table's ID column [default: `id`]
    :return:
    """
    return db.relationship(
        parent_class_name,
        primaryjoin='{0}.{1} == {2}.{3}'.format(
            child_class_name,
            column_name,
            parent_class_name,
            parent_id_name
        ),
        backref=db.backref(
            child_table_name,
            # The following two lines make sure that when the *parent* is
            # deleted, all its connected children are deleted as well
            passive_deletes=True,
            cascade='all,delete'
        )
    )


class SerializableBase(db.Model):
    """Abstract base class for all SQL models that allows [de]serialization
    """
    # A list of columns that shouldn't be serialized
    __hidden__ = ()

    # SQLAlchemy syntax
    __abstract__ = True

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def to_json(self):
        return jsonpickle.encode(self.to_dict(), unpicklable=False)


class Blueprint(SerializableBase):
    __tablename__ = 'blueprints'

    id = db.Column(db.Text, primary_key=True, index=True)
    created_at = db.Column(UTCDateTime, nullable=False, index=True)
    updated_at = db.Column(UTCDateTime, nullable=True, index=True)
    description = db.Column(db.Text, nullable=True)
    main_file_name = db.Column(db.Text, nullable=False)
    plan = db.Column(db.PickleType, nullable=False)


class Snapshot(SerializableBase):
    __tablename__ = 'snapshots'

    CREATED = 'created'
    FAILED = 'failed'
    CREATING = 'creating'
    UPLOADED = 'uploaded'

    STATES = [CREATED, FAILED, CREATING, UPLOADED]
    END_STATES = [CREATED, FAILED, UPLOADED]

    id = db.Column(db.Text, primary_key=True, index=True)
    created_at = db.Column(UTCDateTime, nullable=False, index=True)
    status = db.Column(db.Enum(*STATES, name='snapshot_status'))
    error = db.Column(db.Text, nullable=True)


class Deployment(SerializableBase):
    __tablename__ = 'deployments'

    id = db.Column(db.Text, primary_key=True, index=True)
    created_at = db.Column(UTCDateTime, nullable=False, index=True)
    updated_at = db.Column(UTCDateTime, nullable=True, index=True)
    blueprint_id = _foreign_key_column(Blueprint, nullable=True)
    workflows = db.Column(db.PickleType, nullable=True)  # TODO: foreign key?
    inputs = db.Column(db.PickleType, nullable=True)
    policy_types = db.Column(db.PickleType, nullable=True)
    policy_triggers = db.Column(db.PickleType, nullable=True)
    groups = db.Column(db.PickleType, nullable=True)
    scaling_groups = db.Column(db.PickleType, nullable=True)
    description = db.Column(db.Text, nullable=True)
    outputs = db.Column(db.PickleType, nullable=True)
    permalink = db.Column(db.Text, nullable=True)  # TODO: implement (old)

    blueprint = _relationship(
        'Deployment', 'blueprint_id', 'Blueprint', 'deployments'
    )


class Execution(SerializableBase):
    __tablename__ = 'executions'

    TERMINATED = 'terminated'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    PENDING = 'pending'
    STARTED = 'started'
    CANCELLING = 'cancelling'
    FORCE_CANCELLING = 'force_cancelling'

    STATES = [TERMINATED, FAILED, CANCELLED, PENDING, STARTED,
              CANCELLING, FORCE_CANCELLING]
    END_STATES = [TERMINATED, FAILED, CANCELLED]
    ACTIVE_STATES = [state for state in STATES if state not in END_STATES]

    id = db.Column(db.Text, primary_key=True, index=True)
    status = db.Column(db.Enum(*STATES, name='execution_status'))
    deployment_id = _foreign_key_column(Deployment, nullable=True)
    workflow_id = db.Column(db.Text, nullable=False)
    blueprint_id = _foreign_key_column(Blueprint, nullable=True)
    created_at = db.Column(UTCDateTime, nullable=False, index=True)
    error = db.Column(db.Text, nullable=True)
    parameters = db.Column(db.PickleType, nullable=True)
    is_system_workflow = db.Column(db.Boolean, nullable=False)

    blueprint = _relationship(
        'Execution', 'blueprint_id', 'Blueprint', 'executions'
    )
    deployment = _relationship(
        'Execution', 'deployment_id', 'Deployment', 'executions'
    )


class DeploymentUpdateStep(SerializableBase):
    __tablename__ = 'deployment_update_steps'

    id = db.Column(db.Text, primary_key=True, index=True)
    action = db.Column(db.Enum(*ACTION_TYPES, name='action_type'))
    entity_type = db.Column(db.Enum(*ENTITY_TYPES, name='entity_type'))
    entity_id = db.Column(db.Text, nullable=False)  # TODO: foreign key?


class DeploymentUpdate(SerializableBase):
    __tablename__ = 'deployment_updates'

    id = db.Column(db.Text, primary_key=True, index=True)
    deployment_id = _foreign_key_column(Deployment)
    deployment_plan = db.Column(db.PickleType, nullable=True)
    state = db.Column(db.Text, nullable=True)  # TODO: enum?
    steps = db.Column(db.PickleType, nullable=True)  # TODO: foreign key?
    deployment_update_nodes = db.Column(db.PickleType, nullable=True)
    deployment_update_node_instances = db.Column(db.PickleType, nullable=True)
    deployment_update_deployment = db.Column(db.PickleType, nullable=True)
    modified_entity_ids = db.Column(db.PickleType, nullable=True)
    execution_id = _foreign_key_column(Execution, nullable=True)
    created_at = db.Column(UTCDateTime, nullable=False, index=True)

    deployment = _relationship(
        'DeploymentUpdate', 'deployment_id', 'Deployment', 'deployment_updates'
    )
    execution = _relationship(
        'DeploymentUpdate', 'execution_id', 'Execution', 'deployment_updates'
    )

    def to_dict(self):
        d = super(DeploymentUpdate, self).to_dict()
        # Taking care of the fact the DeploymentSteps are objects
        d['steps'] = [step.to_dict() for step in d['steps']]
        return d


class DeploymentModification(SerializableBase):
    __tablename__ = 'deployment_modifications'

    STARTED = 'started'
    FINISHED = 'finished'
    ROLLEDBACK = 'rolledback'

    STATES = [STARTED, FINISHED, ROLLEDBACK]
    END_STATES = [FINISHED, ROLLEDBACK]

    id = db.Column(db.Text, primary_key=True, index=True)
    created_at = db.Column(UTCDateTime, nullable=False, index=True)
    ended_at = db.Column(UTCDateTime, nullable=True, index=True)
    status = db.Column(db.Enum(*STATES, name='deployment_modification_status'))
    deployment_id = _foreign_key_column(Deployment)
    modified_nodes = db.Column(db.PickleType, nullable=True)
    node_instances = db.Column(db.PickleType, nullable=True)
    context = db.Column(db.PickleType, nullable=True)

    deployment = _relationship(
        'DeploymentModification', 'deployment_id', 'Deployment',
        'deployment_modifications'
    )


class Node(SerializableBase):
    __tablename__ = 'nodes'

    storage_id = db.Column(db.Text, primary_key=True, index=True)
    id = db.Column(db.Text, nullable=False)
    deployment_id = _foreign_key_column(Deployment)
    blueprint_id = _foreign_key_column(Blueprint, nullable=True)
    type = db.Column(db.Text, nullable=False, index=True)
    type_hierarchy = db.Column(db.PickleType, nullable=True)
    number_of_instances = db.Column(db.Integer, nullable=False)
    planned_number_of_instances = db.Column(db.Integer, nullable=False)
    deploy_number_of_instances = db.Column(db.Integer, nullable=False)
    min_number_of_instances = db.Column(db.Integer, nullable=False)
    max_number_of_instances = db.Column(db.Integer, nullable=False)
    # TODO: This probably should be a foreign key, but there's no guarantee
    # in the code, currently, that the host will be created beforehand
    host_id = db.Column(db.Text, nullable=True)
    properties = db.Column(db.PickleType, nullable=True)
    operations = db.Column(db.PickleType, nullable=True)
    plugins = db.Column(db.PickleType, nullable=True)
    relationships = db.Column(db.PickleType, nullable=True)
    plugins_to_install = db.Column(db.PickleType, nullable=True)

    blueprint = _relationship(
        'Node', 'blueprint_id', 'Blueprint', 'nodes'
    )
    deployment = _relationship(
        'Node', 'deployment_id', 'Deployment', 'nodes'
    )


class NodeInstance(SerializableBase):
    __tablename__ = 'node_instances'

    id = db.Column(db.Text, primary_key=True, index=True)
    node_storage_id = _foreign_key_column(Node, 'storage_id')
    node_id = db.Column(db.Text, nullable=False)
    deployment_id = _foreign_key_column(Deployment)
    runtime_properties = db.Column(db.PickleType, nullable=True)
    state = db.Column(db.Text, nullable=False)  # TODO: should be ENUM?
    version = db.Column(db.Integer, default=1)
    relationships = db.Column(db.PickleType, nullable=True)
    # TODO: This probably should be a foreign key, but there's no guarantee
    # in the code, currently, that the host will be created beforehand
    host_id = db.Column(db.Text, nullable=True)
    scaling_groups = db.Column(db.PickleType, nullable=True)

    node = _relationship(
        'NodeInstance',
        'node_storage_id',
        'Node',
        'node_instances',
        'storage_id'
    )
    deployment = _relationship(
        'NodeInstance', 'deployment_id', 'Deployment', 'node_instances'
    )


class ProviderContext(SerializableBase):
    __tablename__ = 'provider_context'

    id = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text, nullable=False)
    context = db.Column(db.PickleType, nullable=False)


class Plugin(SerializableBase):
    __tablename__ = 'plugins'

    id = db.Column(db.Text, primary_key=True, index=True)
    package_name = db.Column(db.Text, nullable=False, index=True)
    archive_name = db.Column(db.Text, nullable=False, index=True)
    package_source = db.Column(db.Text, nullable=True)
    package_version = db.Column(db.Text, nullable=True)
    supported_platform = db.Column(db.PickleType, nullable=True)
    distribution = db.Column(db.Text, nullable=True)
    distribution_version = db.Column(db.Text, nullable=True)
    distribution_release = db.Column(db.Text, nullable=True)
    wheels = db.Column(db.PickleType, nullable=False)
    excluded_wheels = db.Column(db.PickleType, nullable=True)
    supported_py_versions = db.Column(db.PickleType, nullable=True)
    uploaded_at = db.Column(UTCDateTime, nullable=False, index=True)
