from bitmath import Byte, GiB
from digitalocean import Manager
from digitalocean import Volume
from digitalocean.baseapi import NotFoundError
from digitalocean.Metadata import Metadata
from eliot import Message
from eliot import start_action
from flocker.node.agents.blockdevice import AlreadyAttachedVolume
from flocker.node.agents.blockdevice import BlockDeviceVolume
from flocker.node.agents.blockdevice import IBlockDeviceAPI
from flocker.node.agents.blockdevice import UnattachedVolume
from flocker.node.agents.blockdevice import UnknownVolume

from functools import reduce
import six
from twisted.python.filepath import FilePath
from zope.interface import implementer


@implementer(IBlockDeviceAPI)
class DigitalOceanDeviceAPI(object):
    """
    A block device implementation for DigitalOcean block storage.

    The following limitation apply:

    - You need separate flocker clusters per region because volumes cannot be
      moved between regions.
    - Only five volumes can be attached to a droplet at any given time.
    - It is possible for multiple flocker clusters to coexist, but they must not
      share dataset IDs.
    """

    _ONE_GIB = int(GiB(1).to_Byte().value)

    _PREFIX = six.text_type("flocker-v1-")

    def __init__(self, cluster_id, token):
        self._cluster_id = six.text_type(cluster_id)
        self._manager = Manager(token=token)
        self._metadata = None

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
                'region': vol.region.slug,
                'description': vol.description,
                'attached_to': vol.droplet_ids
            })
            return vol

    @classmethod
    def _unmangle_dataset_name(cls, vol_name):
        if vol_name and vol_name.startswith(cls._PREFIX):
            return six.text_type(vol_name)[len(cls._PREFIX):]
        return None

    @staticmethod
    def _to_block_device_volume(do_volume):
        size = int(GiB(do_volume.size_gigabytes).to_Byte().value)
        attached = None
        if do_volume.droplet_ids:
            attached = six.text_type(do_volume.droplet_ids[0])
        dataset = DigitalOceanDeviceAPI._unmangle_dataset_name(do_volume.name)

        return BlockDeviceVolume(blockdevice_id=six.text_type(do_volume.id),
                                 size=size,
                                 attached_to=attached,
                                 dataset_id=dataset)

    def _categorize_do_volume(self, result_dict, vol):
        if not six.text_type(vol.name).startswith(self ._PREFIX):
            result_dict["ignored"].append(vol)
        elif six.text_type(vol.description) != self.volume_description:
            result_dict["wrong_cluster"].append(vol)
        else:
            result_dict["okay"].append(vol)
        return result_dict

    def list_volumes(self):
        """
        Return the list of volumes currently provisioned using the API token

        :return: A list of ``BlockDeviceVolume`` instances.
        """

        with start_action(action_type=six.text_type(
                              "flocker:node:agents:do:list_volumes")) as action:
            res = reduce(self._categorize_do_volume,
                         self._manager.get_all_volumes(),
                         dict(wrong_cluster=list(),
                              ignored=list(),
                              okay=list()))
            Message.log(message_type=six.text_type("digitalocean:test"), result=res);
            if res["ignored"]:
                ty = six.text_type("flocker:node:agents:do:list_volumes:ignored")
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
                    + six.text_type("is not owned by our cluster.")
                for volume in res["wrong_cluster"]:
                    Message.log(message_type=ty,
                                log_level=six.text_type("ERROR"),
                                message=msg,
                                volume=volume)

            volumes = map(self._to_block_device_volume, res["okay"])
            action.add_success_fields(
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
        with start_action(action_type=six.text_type("flocker:node:agents:do:create_volume"),
                dataset_id=dataset_id, size=gib) as a:
            vol = Volume(token=self._manager.token)
            vol.name = self._PREFIX + dataset_id.hex
            vol.size_gigabytes = int(gib.value)
            vol.region = self.metadata.region
            vol.description = self.volume_description
            vol.create()
            a.add_success_fields(volume={
                    'blockdevice_id': vol.id,
                    'size': size,
                    'dataset_id': dataset_id,
                    'region': vol.region
                }
            )
            return self._to_block_device_volume(vol)

    def destroy_volume(self, blockdevice_id):
        with start_action(action_type=six.text_type("flocker:node:agents:do:destroy_volume"),
                blockdevice_id=blockdevice_id):
            try:
                vol = self.get_volume(blockdevice_id)
                vol.destroy()
            except NotFoundError as _:
                raise UnknownVolume(blockdevice_id)

    def attach_volume(self, blockdevice_id, attach_to):
        with start_action(action_type=six.text_type("flocker:node:agents:do:attach_volume"),
                blockdevice_id=blockdevice_id,
                droplet_id=attach_to):
            try:
                vol = self.get_volume(blockdevice_id)
                if vol.droplet_ids:
                    raise AlreadyAttachedVolume(blockdevice_id)
                vol.attach(attach_to, vol.region.slug)
            except NotFoundError as _:
                raise UnknownVolume(blockdevice_id)

    def detach_volume(self, blockdevice_id):
        with start_action(action_type=six.text_type("flocker:node:agents:do:detach_volume"),
                blockdevice_id=blockdevice_id) as a:
            try:
                vol = self.get_volume(blockdevice_id)
                if not vol.droplet_ids:
                    raise UnattachedVolume(blockdevice_id)
                detach_from = vol.droplet_ids[0]
                region = vol.region.slug
                vol.detach(detach_from, region)
                a.add_success_fields(detached_from={
                    'droplet_id': detach_from,
                    'region': region
                })
            except NotFoundError as _:
                raise UnknownVolume(blockdevice_id)

    def get_device_path(self, blockdevice_id):
        try:
            vol = self.get_volume(blockdevice_id)
            if not vol.droplet_ids:
                raise UnattachedVolume(blockdevice_id)
            return FilePath(six.text_type(
                "/dev/disk/by-id/scsi-0DO_Volume_{name}").format(name=vol.name))
        except NotFoundError as _:
            raise UnknownVolume(blockdevice_id)


def do_from_configuration(cluster_id, token=None):
    return DigitalOceanDeviceAPI(cluster_id, token)


