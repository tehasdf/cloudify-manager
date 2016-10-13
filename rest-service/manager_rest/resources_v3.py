#########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

from functools import wraps

from flask import current_app, request
from flask_security import current_user

from manager_rest.storage import models
from manager_rest.utils import abort_error
from manager_rest.security import SecuredResource
from manager_rest.resources import (marshal_with,
                                    exceptions_handled,
                                    MISSING_PREMIUM_PACKAGE_MESSAGE)
from manager_rest.responses_v2 import Execution
from manager_rest.resources_v2 import (create_filters,
                                       paginate,
                                       sortable,
                                       verify_json_content_type,
                                       verify_parameter_in_request_body)
from manager_rest.manager_exceptions import MissingPremiumPackage

try:
    from cloudify_premium import (TenantResponse,
                                  GroupResponse,
                                  UserResponse,
                                  SecuredTenantResource)
except ImportError:
    TenantResponse, GroupResponse, UserResponse = (None, ) * 3
    SecuredTenantResource = SecuredResource

try:
    from cloudify_premium.ha.web import (ClusterResourceBase,
                                         ClusterState,
                                         ClusterNode)
    HAS_CLUSTER = True
except ImportError:
    HAS_CLUSTER = False
    ClusterNode, ClusterState = (None, ) * 2
    ClusterResourceBase = SecuredResource


class Tenants(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(TenantResponse)
    @create_filters(models.Tenant.fields)
    @paginate
    @sortable
    def get(self, _include=None, filters=None, pagination=None, sort=None,
            multi_tenancy=None, **kwargs):
        """
        List tenants
        """
        return multi_tenancy.list_tenants(current_user.id,
                                          _include,
                                          filters,
                                          pagination,
                                          sort)


class TenantsId(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(TenantResponse)
    def post(self, tenant_name, multi_tenancy=None):
        """
        Create a tenant
        """
        return multi_tenancy.create_tenant(tenant_name)


class TenantUsers(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(UserResponse)
    def put(self, multi_tenancy):
        """
        Add a user to a tenant
        """
        verify_json_content_type()
        request_json = request.json
        verify_parameter_in_request_body('username', request_json)
        verify_parameter_in_request_body('tenant_name', request_json)
        return multi_tenancy.add_user_to_tenant(request_json['username'],
                                                request_json['tenant_name'])

    @exceptions_handled
    @marshal_with(UserResponse)
    def delete(self, multi_tenancy):
        """
        Remove a user from a tenant
        """
        verify_json_content_type()
        request_json = request.json
        verify_parameter_in_request_body('username', request_json)
        verify_parameter_in_request_body('tenant_name', request_json)
        user_name = request_json['username']
        tenant_name = request_json['tenant_name']
        return multi_tenancy.remove_user_from_tenant(user_name,
                                                     tenant_name)


class TenantGroups(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(GroupResponse)
    def put(self, multi_tenancy):
        """
        Add a group to a tenant
        """
        verify_json_content_type()
        request_json = request.json
        verify_parameter_in_request_body('group_name', request_json)
        verify_parameter_in_request_body('tenant_name', request_json)
        return multi_tenancy.add_group_to_tenant(request_json['group_name'],
                                                 request_json['tenant_name'])

    @exceptions_handled
    @marshal_with(GroupResponse)
    def delete(self, multi_tenancy):
        """
        Remove a group from a tenant
        """
        verify_json_content_type()
        request_json = request.json
        verify_parameter_in_request_body('group_name', request_json)
        verify_parameter_in_request_body('tenant_name', request_json)
        return multi_tenancy.remove_group_from_tenant(
            request_json['group_name'],
            request_json['tenant_name']
        )


class UserGroups(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(GroupResponse)
    @create_filters(models.Group.fields)
    @paginate
    @sortable
    def get(self, _include=None, filters=None, pagination=None, sort=None,
            multi_tenancy=None, **kwargs):
        """
        List groups
        """
        return multi_tenancy.list_groups(
            _include,
            filters,
            pagination,
            sort)


class UserGroupsId(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(GroupResponse)
    def post(self, group_name, multi_tenancy):
        """
        Create a group
        """
        return multi_tenancy.create_group(group_name)


class UserGroupsUsers(SecuredTenantResource):
    @exceptions_handled
    @marshal_with(UserResponse)
    def put(self, multi_tenancy):
        """
        Add a user to a group
        """
        verify_json_content_type()
        request_json = request.json
        verify_parameter_in_request_body('username', request_json)
        verify_parameter_in_request_body('group_name', request_json)
        return multi_tenancy.add_user_to_group(request_json['username'],
                                               request_json['group_name'])

    @exceptions_handled
    @marshal_with(UserResponse)
    def delete(self, multi_tenancy):
        """
        Remove a user from a group
        """
        verify_json_content_type()
        request_json = request.json
        verify_parameter_in_request_body('username', request_json)
        verify_parameter_in_request_body('group_name', request_json)
        return multi_tenancy.remove_user_from_group(request_json['username'],
                                                    request_json['group_name'])


def cluster_package_required(f):
    """
    Abort the request if the premium HA package isn't installed.
    """

    # a local check is required, because the app-wide condition checks for
    # the marshal_with parameter being None, and some cluster-related endpoints
    # use marshal responses that also exist without the premium package
    # (Execution)

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not HAS_CLUSTER:
            abort_error(MissingPremiumPackage(MISSING_PREMIUM_PACKAGE_MESSAGE),
                        current_app.logger,
                        hide_server_message=True)
        return f(*args, **kwargs)
    return wrapper


class Cluster(ClusterResourceBase):
    @exceptions_handled
    @cluster_package_required
    @marshal_with(ClusterState)
    @create_filters()
    def get(self, cluster, _include=None, filters=None):
        """
        Current state of the cluster.
        """
        return cluster.get_status(_include=_include, filters=None)

    @exceptions_handled
    @cluster_package_required
    @marshal_with(Execution)
    def post(self, cluster):
        """
        Start the "create cluster" execution.

        The created cluster will already have one node (the current manager).
        """
        verify_json_content_type()
        request_json = request.get_json()
        verify_parameter_in_request_body('config', request_json)
        config = request_json['config']
        return cluster.start(config)

    @exceptions_handled
    @cluster_package_required
    @marshal_with(ClusterState)
    def patch(self, cluster):
        """
        Update the cluster config.

        Use this to change settings or promote a replica machine to master.
        """
        verify_json_content_type()
        request_json = request.get_json()
        verify_parameter_in_request_body('config', request_json)
        config = request_json['config']
        return cluster.update_config(config)


class ClusterNodes(ClusterResourceBase):
    @exceptions_handled
    @cluster_package_required
    @marshal_with(ClusterNode)
    def get(self, cluster):
        """
        List the nodes in the current cluster.

        This will also list inactive nodes that weren't deleted. 404 if the
        cluster isn't created yet.
        """
        return cluster.list_nodes()


class ClusterNodesId(ClusterResourceBase):
    @exceptions_handled
    @cluster_package_required
    @marshal_with(ClusterNode)
    def get(self, node_id, cluster):
        """
        Details of a node from the cluster.
        """
        return cluster.get_node(node_id)

    @exceptions_handled
    @cluster_package_required
    @marshal_with(Execution)
    def put(self, node_id, cluster):
        """
        Join the current manager to the cluster.
        """
        verify_json_content_type()
        request_json = request.get_json()
        verify_parameter_in_request_body('config', request_json)
        config = request_json['config']
        return cluster.join(config)

    @exceptions_handled
    @cluster_package_required
    @marshal_with(ClusterNode)
    def delete(self, node_id, cluster):
        """
        Remove the node from the cluster.

        Use this when a node is permanently down.
        """
        return cluster.remove_node(node_id)
