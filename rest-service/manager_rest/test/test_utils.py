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

import unittest

from mock import patch
from nose.plugins.attrib import attr

from manager_rest import models
from manager_rest.utils import plugin_installable_on_current_platform
from manager_rest.test import base_test


@attr(client_min_version=1, client_max_version=base_test.LATEST_API_VERSION)
class TestUtils(unittest.TestCase):

    @attr(client_min_version=2,
          client_max_version=base_test.LATEST_API_VERSION)
    def test_plugin_installable_on_current_platform(self):
        def create_plugin(supported_platform=None,
                          distribution=None,
                          distribution_release=None):
            mock_data = {k: 'stub' for k in models.Plugin.fields}
            if supported_platform:
                mock_data['supported_platform'] = supported_platform
            if distribution:
                mock_data['distribution'] = distribution
            if distribution_release:
                mock_data['distribution_release'] = distribution_release
            return models.Plugin(**mock_data)

        plugin = create_plugin()
        self.assertFalse(plugin_installable_on_current_platform(plugin))

        plugin = create_plugin(supported_platform='any')
        self.assertTrue(plugin_installable_on_current_platform(plugin))

        platform = 'platform1'
        dist = 'dist1'
        rel = 'rel1'

        def mock_linux_dist(full_distribution_name):
            return dist, '', rel

        def mock_get_platform():
            return platform

        with patch('platform.linux_distribution', mock_linux_dist):
            with patch('wagon.utils.get_platform', mock_get_platform):
                plugin = create_plugin(supported_platform=platform)
                self.assertFalse(
                    plugin_installable_on_current_platform(plugin))

                plugin = create_plugin(distribution=dist,
                                       distribution_release=rel)
                self.assertFalse(
                    plugin_installable_on_current_platform(plugin))

                plugin = create_plugin(supported_platform=platform,
                                       distribution=dist,
                                       distribution_release=rel)
                self.assertTrue(
                    plugin_installable_on_current_platform(plugin))
