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

from flask_restful_swagger import swagger
from flask import request

from manager_rest import resources_v2
from manager_rest import responses_v2_1
from manager_rest.resources import exceptions_handled
from manager_rest.resources import marshal_with
from manager_rest.resources import verify_and_convert_bool
from manager_rest.resources import verify_json_content_type
from manager_rest.resources import get_blueprints_manager


class PluginsId(resources_v2.PluginsId):

    @swagger.operation(
            responseClass=responses_v2_1.Plugin,
            nickname="deleteById",
            notes="deletes a plugin according to its ID."
    )
    @exceptions_handled
    @marshal_with(responses_v2_1.Plugin)
    def delete(self, plugin_id, **kwargs):
        """
        Delete plugin by ID
        """
        verify_json_content_type()
        request_json = request.json
        force = verify_and_convert_bool(
                'force', request_json.get('force', False))
        return get_blueprints_manager().remove_plugin(plugin_id=plugin_id,
                                                      force=force)
