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

from bitmath import Byte, GiB
from digitalocean import Manager
from digitalocean import Volume as Vol
from digitalocean import Action as Act
from digitalocean.baseapi import NotFoundError
from digitalocean.Metadata import Metadata
from eliot import Message
from eliot import start_action
from flocker.node.agents.blockdevice import AlreadyAttachedVolume
from flocker.node.agents.blockdevice import BlockDeviceVolume
from flocker.node.agents.blockdevice import IBlockDeviceAPI
from flocker.node.agents.blockdevice import ICloudAPI
from flocker.node.agents.blockdevice import UnattachedVolume
from flocker.node.agents.blockdevice import UnknownVolume
from functools import reduce
from sched import scheduler
import six
import time
from uuid import UUID
from twisted.python.filepath import FilePath
from zope.interface import implementer


class DOException(Exception):
    pass


@implementer(IBlockDeviceAPI)
@implementer(ICloudAPI)
class DigitalOceanDeviceAPI(object):
    """
    A block device implementation for DigitalOcean block storage.

    The following limitation apply:

    - You need separate flocker clusters per region because volumes cannot be
      moved between regions.
    - Only five volumes can be attached to a droplet at any given time.
    - It is possible for multiple flocker clusters to coexist, but they must
      not share dataset IDs.
    """

    _ONE_GIB = int(GiB(1).to_Byte().value)

    _PREFIX = six.text_type("flocker-v1-")

    # We reassign the Volume class as an attribute to help ergonomics in our
    # test suite.
    Volume = Vol
    Action = Act

    def __init__(self, cluster_id, token):
        self._cluster_id = six.text_type(cluster_id)
        self._manager = Manager(token=token)
        self._metadata = None
        self._poll = 1
        self._timeout = 60

    @property
    def metadata(self):
        if not self._metadata:
            self._metadata = Metadata()
        if not self._metadata.droplet_id:
            with start_action(action_type=six.text_type(
                    "flocker:node:agents:do:load_metadata")) as a:
                self._metadata.load()
                a.add_success_fields(droplet_metadata={
                    'droplet_id': self._metadata.droplet_id,
                    'hostname': self._metadata.hostname,
                    'region': self._metadata.region
                })
        return self._metadata

    @property
    def volume_description(self):
        """ Returns the description this flocker cluster should use

        :return: The cluster ID property string to use as a description
        """
        return six.text_type(
            "flocker-v1-cluster-id: {cluster_id}").format(
                cluster_id=self._cluster_id)

    def allocation_unit(self):
        return self._ONE_GIB

    def compute_instance_id(self):
        return six.text_type(self.metadata.droplet_id)

    def get_volume(self, blockdevice_id):
        with start_action(action_type=six.text_type(
                    "flocker:node:agents:do:get_volume"),
                    blockdevice_id=blockdevice_id) as a:
            vol = self._manager.get_volume(blockdevice_id)
            a.add_success_fields(volume={
                'name': vol.name,
                'region': vol.region["slug"],
                'description': vol.description,
                'attached_to': vol.droplet_ids
            })
            return vol

    @classmethod
    def _unmangle_dataset(cls, vol_name):
        """Unmangles the flocker dataset from a digital ocean volume name

        :param vol_name: The name of the digitalocean volume
        :return: The dataset UUID encoded therein or None, if not a flocker
                 volume
        """
        if vol_name and vol_name.startswith(cls._PREFIX):
            return UUID(vol_name[len(cls._PREFIX):])
        return None

    @classmethod
    def _mangle_dataset(cls, dataset_id):
        """Mangles a flocker dataset UUID into a digital ocean volume name.

        :param dataset_id: The UUID of the dataset
        :return: The volumen name to use for the digitalocean volume
        """
        return cls._PREFIX + dataset_id.hex

    @staticmethod
    def _to_block_device_volume(do_volume):
        """Turns a digitalocean volume description into a flocker one

        :param do_volume: The digital ocean volume
        :return: The corresponding BlockDeviceVolume
        """
        size = int(GiB(do_volume.size_gigabytes).to_Byte().value)
        attached = None
        if do_volume.droplet_ids:
            attached = six.text_type(do_volume.droplet_ids[0])
        dataset = DigitalOceanDeviceAPI._unmangle_dataset(do_volume.name)

        return BlockDeviceVolume(blockdevice_id=six.text_type(do_volume.id),
                                 size=size,
                                 attached_to=attached,
                                 dataset_id=dataset)

    def _categorize_do_volume(self, result_dict, vol):
        """ Reduce function to categorise whether a volume is usable.
        :param result_dict: A dictionary with three keys: ignored,
                            wrong_cluster, and okay
        :param vol: A digitalocean volume
        :return: The result_dict with vol sorted into the correct slot
        """
        if not six.text_type(vol.name).startswith(self ._PREFIX):
            result_dict["ignored"].append(vol)
        elif six.text_type(vol.description) != self.volume_description:
            result_dict["wrong_cluster"].append(vol)
        else:
            result_dict["okay"].append(vol)
        return result_dict

    def _should_wait_on(self, action, from_time):
        return not action or (action.status == 'in-progress' and
                              (time.time() - from_time < self._timeout))

    def _await_action_id(self, action_id):
        action = self.Action.get_object(self._manager.token,
                                        action_id)
        self._await_action(action)

    def _await_action(self, action):
        s = scheduler(time.time, time.sleep)
        i = 0
        started_at = time.time()
        if action and action.status == 'completed':
            return True
        elif not action:
            return False
        with start_action(action_type=six.text_type(
                'flocker:node:agents:do:await'), do_action_type=action.type,
                          do_action_id=action.id) as ac:
            while self._should_wait_on(action, started_at):
                delta = max(0, min(self._poll,
                                   self._timeout - (time.time() - started_at)))
                s.enter(delta, 0, lambda x: x.load_directly(), (action,))
                s.run()
                i += 1
            if action.status == 'completed':
                ac.add_success_fields(iterations=i,
                                      do_action_status='completed')
            else:
                Message.log(message_type=six.text_type(
                    'flocker:node:agents:do:await:err'),
                            log_level=six.text_type('ERROR'),
                            message=six.text_type('Wait unsuccesful'),
                            iterations=i,
                            do_action_status=action.status
                )
                if action.status == 'in-progress':
                    raise DOException('Wait timeout')
                else:
                    raise DOException(six.text_type('Action failed ({r})').
                                      format(r=action.status))

        return action and action.status == 'completed'

    def list_volumes(self):
        with start_action(action_type=six.text_type(
                              "flocker:node:agents:do:list_volumes")) as a:
            res = reduce(self._categorize_do_volume,
                         self._manager.get_all_volumes(),
                         dict(wrong_cluster=list(),
                              ignored=list(),
                              okay=list()))

            if res["ignored"]:
                ty = six.text_type(
                    "flocker:node:agents:do:list_volumes:ignored")
                msg = six.text_type("Ignored {num} unrelated volumes").format(
                        num=len(res["ignored"]))
                Message.log(message_type=ty,
                            log_level=six.text_type("INFO"),
                            message=msg,
                            ignored_volumes=res["ignored"])

            if res["wrong_cluster"]:
                ty = six.text_type("flocker:node:agents:do") \
                     + six.text_type(":list_volumes:suspicious_disk")
                msg = six.text_type("Volume follows naming convention but") \
                    + six.text_type(" is not owned by our cluster.")
                for volume in res["wrong_cluster"]:
                    Message.log(message_type=ty,
                                log_level=six.text_type("ERROR"),
                                message=msg,
                                volume=volume.name,
                                description=volume.description)

            volumes = map(self._to_block_device_volume, res["okay"])
            a.add_success_fields(
                cluster_volumes=list(
                    {
                        'blockdevice_id': v.blockdevice_id,
                        'size': v.size,
                        'attached_to': v.attached_to,
                        'dataset_id': six.text_type(v.dataset_id),
                    } for v in volumes))
            return volumes

    def create_volume(self, dataset_id, size):
        gib = Byte(size).to_GiB()
        with start_action(action_type=six.text_type(
                "flocker:node:agents:do:create_volume"),
                dataset_id=six.text_type(dataset_id),
                size=size) as a:
            vol = self.Volume(token=self._manager.token)
            vol.name = self._mangle_dataset(dataset_id)
            vol.size_gigabytes = int(gib.value)
            vol.region = self.metadata.region
            vol.description = self.volume_description
            vol.create()
            a.add_success_fields(volume={
                    'blockdevice_id': vol.id,
                    'region': vol.region
                }
            )
            return self._to_block_device_volume(vol)

    def destroy_volume(self, blockdevice_id):
        with start_action(action_type=six.text_type(
                "flocker:node:agents:do:destroy_volume"),
                blockdevice_id=blockdevice_id):
            try:
                vol = self.get_volume(blockdevice_id)
                if vol.droplet_ids:
                    # need to detach prior to deletion
                    ty = six.text_type('flocker:node:agents:do') + \
                         six.text_type(':destroy:detach_needed')
                    Message.log(message_type=ty,
                                log_level=six.text_type('INFO'),
                                message=six.text_type(
                                    'Volume needs to be detached first'),
                                volume=vol.id,
                                attached_to=vol.droplet_ids[0])
                    r = vol.detach(vol.droplet_ids[0],
                                   vol.region['slug'])
                    self._await_action_id(r['action']['id'])

                vol.destroy()
            except NotFoundError as _:
                raise UnknownVolume(blockdevice_id)

    def attach_volume(self, blockdevice_id, attach_to):
        with start_action(action_type=six.text_type(
                "flocker:node:agents:do:attach_volume"),
                blockdevice_id=blockdevice_id,
                droplet_id=attach_to):
            try:
                vol = self.get_volume(blockdevice_id)
                if vol.droplet_ids:
                    raise AlreadyAttachedVolume(blockdevice_id)
                r = vol.attach(attach_to, vol.region["slug"])
                if self._await_action_id(r['action']['id']):
                    vol.droplet_ids = [attach_to]
                return self._to_block_device_volume(vol)
            except NotFoundError as _:
                raise UnknownVolume(blockdevice_id)

    def detach_volume(self, blockdevice_id):
        with start_action(action_type=six.text_type(
                "flocker:node:agents:do:detach_volume"),
                blockdevice_id=blockdevice_id) as a:
            try:
                vol = self.get_volume(blockdevice_id)
                if not vol.droplet_ids:
                    raise UnattachedVolume(blockdevice_id)
                detach_from = vol.droplet_ids[0]
                region = vol.region["slug"]
                r = vol.detach(detach_from, region)

                if self._await_action_id(r['action']['id']):
                    vol.droplet_ids = None
                a.add_success_fields(detached_from={
                    'droplet_id': detach_from,
                    'region': region
                })
                return self._to_block_device_volume(vol)
            except NotFoundError as _:
                raise UnknownVolume(blockdevice_id)

    def get_device_path(self, blockdevice_id):
        try:
            vol = self.get_volume(blockdevice_id)
            path = FilePath(six.text_type(
                    "/dev/disk/by-id/scsi-0DO_Volume_{name}").format(
                    name=vol.name))

            # Even if we are not attached, the agent needs to know the
            # expected path for the convergence algorithm
            # FIXME: The functional tests seem to indicate otherwise
            if not vol.droplet_ids:
                # return path
                raise UnattachedVolume(blockdevice_id)

            # But if we are attached, we might need to resolve the symlink
            # noinspection PyBroadException
            try:
                return path.realpath()
            except Exception as _:
                return path

        except NotFoundError as _:
            raise UnknownVolume(blockdevice_id)

    def list_live_nodes(self):
        return map(lambda x: six.text_type(x.id),
                   filter(lambda x: x.status == 'active',
                          self._manager.get_all_droplets()))

    def start_node(self, compute_instance_id):
        droplet = self._manager.get_droplet(compute_instance_id)
        if droplet.status != 'active':
            action = droplet.power_on(return_dict=False)
            self._await_action(action)


def do_from_configuration(cluster_id, token=None):
    return DigitalOceanDeviceAPI(cluster_id, token)
