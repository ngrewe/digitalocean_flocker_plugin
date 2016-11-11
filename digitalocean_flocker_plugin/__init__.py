# Copyright 2016 Niels Grewe
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from digitalocean_flocker_plugin import do_from_configuration
from flocker.node.backends import BackendDescription
from flocker.node.backends import DeployerType
import six

FLOCKER_BACKEND = BackendDescription(name=six.text_type("digitalocean"),
                                     needs_reactor=False,
                                     needs_cluster_id=True,
                                     api_factory=do_from_configuration,
                                     deployer_type=DeployerType.block,
                                     required_config={six.text_type("token")})
