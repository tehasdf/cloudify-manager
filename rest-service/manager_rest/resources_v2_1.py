#########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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
#

from manager_rest import resources
from manager_rest import models
from manager_rest import resources_v2
from manager_rest import responses_v2_1
from manager_rest.resources import exceptions_handled, marshal_with
from manager_rest.resources_v2 import create_filters, paginate, sortable


class Deployments(resources_v2.Deployments):

    @exceptions_handled
    @marshal_with(responses_v2_1.Deployment)
    @create_filters(models.Deployment.fields)
    @paginate
    @sortable
    def get(self, _include=None, filters=None, pagination=None, sort=None,
            **kwargs):
        return super(Deployments, self).get(_include=_include,
                                            filters=filters,
                                            pagination=pagination,
                                            sort=sort,
                                            **kwargs)


class Nodes(resources_v2.Nodes):

    @exceptions_handled
    @marshal_with(responses_v2_1.Node)
    @create_filters(models.DeploymentNode.fields)
    @paginate
    @sortable
    def get(self, _include=None, filters=None, pagination=None,
            sort=None, **kwargs):
        return super(Nodes, self).get(_include=_include,
                                      filters=filters,
                                      pagination=pagination,
                                      sort=sort,
                                      **kwargs)


class NodeInstances(resources.NodeInstances):

    @exceptions_handled
    @marshal_with(responses_v2_1.NodeInstance)
    @create_filters(models.DeploymentNodeInstance.fields)
    @paginate
    @sortable
    def get(self, _include=None, filters=None, pagination=None,
            sort=None, **kwargs):
        """
        List node instances
        """
        return super(NodeInstances, self).get(_include=_include,
                                              filters=filters,
                                              pagination=pagination,
                                              sort=sort,
                                              **kwargs)


class DeploymentsId(resources.DeploymentsId):

    @exceptions_handled
    @marshal_with(responses_v2_1.Deployment)
    def get(self, deployment_id, _include=None, **kwargs):
        return super(DeploymentsId, self).get(deployment_id=deployment_id,
                                              _include=_include,
                                              **kwargs)

    @exceptions_handled
    @marshal_with(responses_v2_1.Deployment)
    def put(self, deployment_id, **kwargs):
        return super(DeploymentsId, self).put(deployment_id=deployment_id,
                                              **kwargs)

    @exceptions_handled
    @marshal_with(responses_v2_1.Deployment)
    def delete(self, deployment_id, **kwargs):
        return super(DeploymentsId, self).delete(deployment_id=deployment_id,
                                                 **kwargs)


class NodeInstancesId(resources.NodeInstancesId):

    @exceptions_handled
    @marshal_with(responses_v2_1.NodeInstance)
    def get(self, node_instance_id, _include=None, **kwargs):
        return super(NodeInstancesId, self).get(
            node_instance_id=node_instance_id,
            _include=_include,
            **kwargs)

    @exceptions_handled
    @marshal_with(responses_v2_1.NodeInstance)
    def patch(self, node_instance_id, **kwargs):
        return super(NodeInstancesId, self).patch(
            node_instance_id=node_instance_id,
            **kwargs)
