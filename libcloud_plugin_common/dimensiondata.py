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


import time
from cloudify.exceptions import NonRecoverableError
from libcloud.compute.types import NodeState
from libcloud_plugin_common import (LibcloudServerClient,
                                    LibcloudFloatingIPClient,
                                    LibcloudSecurityGroupClient,
                                    transform_resource_name,
                                    LibcloudProviderContext)
from libcloud.compute.base import NodeAuthPassword

class DimensionDataLibcloudServerClient(LibcloudServerClient):

    def get_by_name(self, server_name):
        nodes = self.driver.list_nodes()
        for node in nodes:
            if node.name == server_name:
                return node

    def get_by_id(self, server_id):
        nodes = self.driver.list_nodes(ex_node_ids=[server_id])
        return nodes[0] if nodes is not None else None

    def start_server(self, server):
        self.driver.ex_start_node(server)

    def stop_server(self, server):
        self.driver.ex_shutdown_graceful(server)

    def delete_server(self, server):
        self.driver.destroy_node(server)

    def wait_for_server_to_be_deleted(self, server, timeout, sleep_time):
        self._wait_for_server_to_obtaine_state(server,
                                               timeout,
                                               sleep_time,
                                               NodeState.TERMINATED)

    def wait_for_server_to_be_running(self, server, timeout, sleep_time):
        self._wait_for_server_to_obtaine_state(server,
                                               timeout,
                                               sleep_time,
                                               NodeState.RUNNING)

    def _wait_for_server_to_obtaine_state(self,
                                          server,
                                          timeout,
                                          sleep_time,
                                          state):
        while server.state is not state:
            timeout -= 5
            if timeout <= 0:
                raise RuntimeError('Server {} has not been deleted.'
                                   ' Waited for {} seconds'
                                   .format(server.id, timeout))
            time.sleep(sleep_time)
            server = self.get_by_id(server.id)

    def connect_floating_ip(self, server, ip):
        self.driver.ex_associate_address_with_node(server, ip)

    def disconnect_floating_ip(self, ip):
        self.driver.ex_disassociate_address(ip)

    def get_image_by_name(self, image_name):
        return list(filter(lambda x: x.name == image_name,
                           self.driver.list_images()))[0]

    def is_server_active(self, server):
        return server.state == NodeState.RUNNING

    def create(
            self,
            name,
            ctx,
            server_context,
            provider_context):

        if 'image_name' in server_context:
            image = self.get_image_by_name(server_context['image_name'])
        else:
            raise NonRecoverableError("Image name is a required parameter")

        if 'network_name' in server_context:
            network_name = server_context['network_name']
        else:
            raise NonRecoverableError("Network name is a required parameter")

        if 'node_description' in server_context:
            description = server_context['node_description']
        else:
            description = ''

        if 'node_password' in server_context:
            auth_obj = NodeAuthPassword(server_context['node_password'])
        else:
            raise NonRecoverableError("node_password is a required parameter")

        node = self.driver.create_node(name=name,
                                       image=image,
                                       auth=auth_obj,
                                       ex_description=description,
                                       ex_network=network_name)
        return node
