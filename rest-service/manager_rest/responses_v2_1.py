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

from flask.ext.restful import fields
from flask_restful_swagger import swagger

from manager_rest.responses import (Deployment as DeploymentV1,
                                    Node as NodeV1,
                                    NodeInstance as NodeInstanceV1,
                                    Workflow)


@swagger.model
@swagger.nested(workflows=Workflow.__name__)
class Deployment(DeploymentV1):

    resource_fields = dict(DeploymentV1.resource_fields.items() + {
        'policies': fields.Raw,
    }.items())

    def __init__(self, **kwargs):
        super(Deployment, self).__init__(**kwargs)
        self.policies = kwargs['policies']


@swagger.model
class Node(NodeV1):

    resource_fields = dict(DeploymentV1.resource_fields.items() + {
        'min_number_of_instances': fields.String,
        'max_number_of_instances': fields.String,
    }.items())

    def __init__(self, **kwargs):
        super(Node, self).__init__(**kwargs)
        self.min_number_of_instances = kwargs['min_number_of_instances']
        self.max_number_of_instances = kwargs['max_number_of_instances']


@swagger.model
class NodeInstance(NodeInstanceV1):

    resource_fields = dict(NodeInstanceV1.resource_fields.items() + {
        'scaling_groups': fields.Raw
    }.items())

    def __init__(self, **kwargs):
        super(NodeInstance, self).__init__(**kwargs)
        self.scaling_groups = kwargs['scaling_groups']
