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

from digitalocean import baseapi
from digitalocean import Droplet
from digitalocean.Manager import Manager
from digitalocean.Metadata import Metadata
from ..digitalocean_flocker_plugin import DigitalOceanDeviceAPI
from digitalocean_flocker_plugin import digitalocean_flocker_plugin
from flocker.node.agents.blockdevice import AlreadyAttachedVolume
from flocker.node.agents.blockdevice import BlockDeviceVolume
from flocker.node.agents.blockdevice import UnattachedVolume
from flocker.node.agents.blockdevice import UnknownVolume
from flocker.testtools import cluster_utils
import mock
import six
from twisted.python.filepath import FilePath
import unittest
import uuid

VOLUME_MOCK_DATA = {
    '1234': {'name': 'flocker-v1-0ff663594f6347c8a950ff5de6f6225e',
             'size_gigabytes': 100,
             'droplet_ids': ['42'],
             'region': {'slug': 'oxia-planum'}},
    '1235': {'name': 'flocker-v1-55eacb0e962c4c60911fb43d34ec3f85',
             'size_gigabytes': 50,
             'droplet_ids': None,
             'region': {'slug': 'oxia-planum'}},
    '1236': {'name': 'custom-volume',
             'size_gigabytes': 10,
             'droplet_ids': None,
             'region': {'slug': 'oxia-planum'}},
    '1237': {'name': 'flocker-v1-bd4c3b59-6fc3-4f75-a8e6-9f314404f6a6',
             'size_gigabytes': 10,
             'description': 'this-is-not-a-cluster-volume',
             'droplet_ids': None,
             'region': {'slug': 'oxia-planum'}},
    '1238': {'name': 'flocker-v1-3383db2340e54de9b1b4b13153a97be9',
             'size_gigabytes': 50,
             'droplet_ids': ['16'],
             'region': {'slug': 'biblis-tholus'}},
    }

BLOCK_DEVICE_MOCK_DATA = {
    BlockDeviceVolume(
        blockdevice_id=six.text_type('1234'),
        size=107374182400,
        attached_to=six.text_type('42'),
        dataset_id=uuid.UUID('0ff66359-4f63-47c8-a950-ff5de6f6225e')),
    BlockDeviceVolume(
        blockdevice_id=six.text_type('1235'),
        size=53687091200,
        attached_to=None,
        dataset_id=uuid.UUID('55eacb0e-962c-4c60-911f-b43d34ec3f85')),
    BlockDeviceVolume(
        blockdevice_id=six.text_type('1238'),
        size=53687091200,
        attached_to=six.text_type('16'),
        dataset_id=uuid.UUID('3383db23-40e5-4de9-b1b4-b13153a97be9'))}

VOLUME_MOCK_KEYS = ['description', 'droplet_ids', 'name', 'region',
                    'size_gigabytes']


class MockableVolume(DigitalOceanDeviceAPI.Volume):
    """Exposes the __init__ attributes of Volume for mocking """
    id = None
    name = None
    description = None
    droplet_ids = None
    size_gigabytes = None


def mock_set_metadata(instance):
    instance.droplet_id = '42'
    instance.region = 'oxia-planum'
    instance.hostname = 'kitchen.ma.rs'


class TestBlockDeviceAPI(unittest.TestCase):

    def setUp(self):
        self._cluster_id = cluster_utils.make_cluster_id(
            cluster_utils.TestTypes.FUNCTIONAL)
        self._api = digitalocean_flocker_plugin.do_from_configuration(
            self._cluster_id, token='this-is-not-a-token')

    def _populate_volume(self, blockdevice_id, base_volume=None):
        """ Take a block device ID from the mock list and turn it into a volume

        :type blockdevice_id: str
        """
        if not base_volume:
            base_volume = mock.create_autospec(MockableVolume)
        template = VOLUME_MOCK_DATA[blockdevice_id]
        if not template:
            raise baseapi.NotFoundError()

        base_volume.id = blockdevice_id
        for key in VOLUME_MOCK_KEYS:
            if key is 'description' and 'description' not in template:
                setattr(base_volume, key, self._api.volume_description)
            else:
                setattr(base_volume, key, template[key])
        return base_volume

    @staticmethod
    def _category_unit():
        return dict(wrong_cluster=list(), ignored=list(), okay=list())

    def test_volume_description(self):
        self.assertEqual(six.text_type(
            'flocker-v1-cluster-id: {cluster_id}').format(
                cluster_id=self._cluster_id), self._api.volume_description,
            'volume description correct')

    def test_allocation_unit(self):
        self.assertEqual(1073741824, self._api.allocation_unit(),
                         'allocation unit (1GiB)')

    @mock.patch.object(Metadata, 'load',
                       autospec=True, side_effect=mock_set_metadata)
    def test_compute_instance_id(self, mock_load):
        self.assertEqual(six.text_type('42'), self._api.compute_instance_id(),
                         'compute id correct')
        # Call again to see whether we load only once
        _ = self._api.compute_instance_id
        self.assertEqual(1, mock_load.call_count, 'metadata only loaded once')

    def test_volume_name_format(self):
        dataset_id = uuid.UUID('0ff66359-4f63-47c8-a950-ff5de6f6225e')
        vol_name = self._api._mangle_dataset(dataset_id)
        self.assertEqual(
            six.text_type('flocker-v1-0ff663594f6347c8a950ff5de6f6225e'),
            vol_name, 'dataset->volume name correct')

    def test_volume_name_rountrip(self):
        dataset_id = uuid.uuid4()
        self.assertEqual(dataset_id,
                         self._api._unmangle_dataset(
                             self._api._mangle_dataset(dataset_id)),
                         'dataset<->volume name roundtrip')

    def test_volume_name_unrecognized(self):
        self.assertIsNone(self._api._unmangle_dataset('custom-volume'),
                          'unrecognised name ignored')

    @mock.patch('digitalocean.Volume', autospec=True)
    def test_volume_to_block_device(self, mock_volume):
        mock_volume = self._populate_volume('1234', base_volume=mock_volume)
        block_device = self._api._to_block_device_volume(mock_volume)
        self.assertEqual(BlockDeviceVolume(
            blockdevice_id=six.text_type('1234'),
            size=107374182400,
            attached_to=six.text_type('42'),
            dataset_id=uuid.UUID(
                '0ff66359-4f63-47c8-a950-ff5de6f6225e')),
            block_device,
            'correct conversion from do volume to BlockDeviceVolume')

    def test_categorize_good_volume(self):
        volume = self._populate_volume('1234')
        categorized = self._api._categorize_do_volume(self._category_unit(),
                                                      volume)
        self.assertEqual(dict(ignored=list(),
                              wrong_cluster=list(),
                              okay=[volume]),
                         categorized, 'recognised good volume')

    def test_categorize_foreign_volume(self):
        volume = self._populate_volume('1236')
        categorized = self._api._categorize_do_volume(self._category_unit(),
                                                      volume)
        self.assertEqual(dict(ignored=[volume],
                              wrong_cluster=list(),
                              okay=list()),
                         categorized, 'recognised volume to ignore')

    def test_categorize_wrong_cluster_volume(self):
        volume = self._populate_volume('1237')
        categorized = self._api._categorize_do_volume(self._category_unit(),
                                                      volume)
        self.assertEqual(dict(ignored=list(),
                              wrong_cluster=[volume],
                              okay=list()),
                         categorized, 'recognised volume from other cluster')

    @mock.patch.object(Manager, 'get_all_volumes', autospec=True)
    def test_list_volumes(self, mock_all_volumes):
        # noinspection PyTypeChecker
        mock_all_volumes.return_value = map(self._populate_volume,
                                            VOLUME_MOCK_DATA.keys())
        actual_list = self._api.list_volumes()
        self.assertEqual(1, mock_all_volumes.call_count,
                         'volume list API called')
        self.assertEqual(set(actual_list), BLOCK_DEVICE_MOCK_DATA,
                         'correct volume list returned')

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    def test_get_volume(self, mock_get_volume):
        volumes = dict(map(lambda k: (k, self._populate_volume(k)),
                           VOLUME_MOCK_DATA.keys()))
        mock_get_volume.side_effect = lambda s, x: volumes[x]
        v = self._api.get_volume(six.text_type('1234'))
        self.assertEqual(v, volumes['1234'])

    @mock.patch.object(Metadata, 'load',
                       autospec=True, side_effect=mock_set_metadata)
    @mock.patch.object(DigitalOceanDeviceAPI, 'Volume',
                       autospec=MockableVolume)
    def test_create_volume(self, mock_volume, _):
        mock_volume.return_value.id = '1239'
        dataset_id = uuid.UUID('1d5866a7-9d12-4497-a102-0f23ec4ae1c4')
        self._api.create_volume(dataset_id, 107374182400)
        mock_volume.assert_called_with(token='this-is-not-a-token')
        self.assertEqual(1, mock_volume().create.call_count, 'create called')
        self.assertEqual(mock_volume().name, six.text_type(
            'flocker-v1-1d5866a79d124497a1020f23ec4ae1c4'))
        self.assertEqual(mock_volume().description,
                         self._api.volume_description, 'correct cluster')
        self.assertEqual(mock_volume().size_gigabytes, 100, 'correct size')
        self.assertEqual(mock_volume().region, 'oxia-planum', 'correct region')

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    @mock.patch('digitalocean.Volume', autospec=MockableVolume)
    @mock.patch.object(DigitalOceanDeviceAPI, 'Action', autospec=True)
    def test_destroy_volume(self, mock_action, mock_volume, mock_get_volume):
        mock_get_volume.side_effect = lambda s, x: \
            self._populate_volume(x, mock_volume())
        mock_action.get_object.status = 'completed'
        self._api.destroy_volume('1235')
        self.assertEqual(1, mock_volume().destroy.call_count,
                         'volume destroyed')

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    def test_destroy_volume_nx(self, mock_get_volume):
        volumes = dict(map(lambda k: (k,
                                      self._populate_volume(k)),
                           VOLUME_MOCK_DATA.keys()))

        def _r():
            raise baseapi.NotFoundError()

        mock_get_volume.side_effect = lambda s, x: \
            volumes[x] if x in volumes else _r()
        with self.assertRaises(UnknownVolume):
            self._api.destroy_volume(six.text_type('1240'))

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    @mock.patch.object(DigitalOceanDeviceAPI, 'Action', autospec=True)
    @mock.patch('digitalocean.Volume', autospec=MockableVolume)
    def test_destroy_volume_attached(self, mock_volume, mock_action,
                                     mock_get_volume):
        mock_action.get_object.status = 'completed'
        mock_get_volume.side_effect = lambda s, x: \
            self._populate_volume(x, mock_volume())
        self._api.destroy_volume('1234')
        self.assertEqual(1, mock_volume().detach.call_count,
                         'volume detached prior to destruction')
        self.assertEqual(1, mock_volume().destroy.call_count,
                         'volume destroyed')

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    def test_attach_volume_attached(self, mock_get_volume):
        mock_get_volume.side_effect = lambda s, x: self._populate_volume(x)
        with self.assertRaises(AlreadyAttachedVolume):
            self._api.attach_volume(six.text_type('1234'), six.text_type('16'))

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    @mock.patch('digitalocean.Volume', autospec=MockableVolume)
    @mock.patch.object(DigitalOceanDeviceAPI, 'Action', autospec=True)
    def test_attach_volume(self, mock_action, mock_volume, mock_get_volume):
        mock_get_volume.side_effect = lambda s, x:\
            self._populate_volume(x, mock_volume())
        mock_action.get_object.status = 'completed'
        self._api.attach_volume(six.text_type('1235'), six.text_type('42'))
        mock_volume().attach.assert_called_with(six.text_type('42'),
                                                'oxia-planum')

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    def test_detach_volume_deattached(self, mock_get_volume):
        mock_get_volume.side_effect = lambda s, x: self._populate_volume(x)
        with self.assertRaises(UnattachedVolume):
            self._api.detach_volume(six.text_type('1235'))

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    @mock.patch('digitalocean.Volume', autospec=MockableVolume)
    @mock.patch.object(DigitalOceanDeviceAPI, 'Action', autospec=True)
    def test_detach_volume(self, mock_action, mock_volume, mock_get_volume):
        mock_get_volume.side_effect = lambda s, x:\
            self._populate_volume(x, mock_volume())
        mock_action.get_object.status = 'completed'
        self._api.detach_volume(six.text_type('1234'))
        mock_volume().detach.assert_called_with(six.text_type('42'),
                                                'oxia-planum')

    @mock.patch.object(Manager, 'get_volume', autospec=True)
    def test_device_path(self, mock_get_volume):
        mock_get_volume.side_effect = lambda s, x: self._populate_volume(x)
        self.assertEqual(FilePath(
            six.text_type('/dev/disk/by-id/scsi-0DO_Volume_') +
            six.text_type('flocker-v1-0ff663594f6347c8a950ff5de6f6225e')),
                         self._api.get_device_path(six.text_type('1234')))

    def tearDown(self):
        self._api = None
        self._cluster_id = None


INSTANCE_MOCK_DATA = {
    six.text_type("42"): {
        "id": 42,
        "status": "active"
    },
    six.text_type("16"): {
        "id": 16,
        "status": "active"
    },
    six.text_type("32"): {
        "id": 32,
        "status": "off"
    }
}


class MockableDroplet(Droplet):
    id = None
    name = None
    status = None


class TestCloudAPI(unittest.TestCase):

    def setUp(self):
        self._cluster_id = cluster_utils.make_cluster_id(
            cluster_utils.TestTypes.FUNCTIONAL)
        self._api = digitalocean_flocker_plugin.do_from_configuration(
            self._cluster_id, token='this-is-not-a-token')

    @staticmethod
    def _populate_droplet(droplet_id, base_droplet=None):
        """ Take a droplet ID from the mock list and turn it into a droplet"""
        if not base_droplet:
                return Droplet(**INSTANCE_MOCK_DATA[droplet_id])

        template = INSTANCE_MOCK_DATA[droplet_id]

        for key in template.keys():
            setattr(base_droplet, key, template[key])
        return base_droplet

    @mock.patch.object(Manager, 'get_all_droplets', autospec=True)
    def test_list_live_nodes(self, mock_all_droplets):
        mock_all_droplets.return_value = map(self._populate_droplet,
                                             INSTANCE_MOCK_DATA.keys())
        nodes = self._api.list_live_nodes()
        self.assertSetEqual(set(nodes), {six.text_type("16"),
                                         six.text_type("42")},
                            'Live nodes returned')

    @mock.patch.object(Manager, 'get_droplet', autospec=True)
    @mock.patch('digitalocean.Droplet', autospec=MockableDroplet)
    @mock.patch.object(DigitalOceanDeviceAPI, 'Action', autospec=True)
    def test_start_node(self, mock_action, mock_droplet, mock_get_droplet):
        mock_action().status = 'completed'
        mock_droplet().power_on.return_value = mock_action()
        mock_get_droplet.side_effect = lambda s, x: \
            self._populate_droplet(x, mock_droplet())
        self._api.start_node(six.text_type('32'))
        self.assertEqual(1, mock_droplet().power_on.call_count,
                         'Node started')

    @mock.patch.object(Manager, 'get_droplet', autospec=True)
    @mock.patch('digitalocean.Droplet', autospec=MockableDroplet)
    def test_start_node_running(self, mock_droplet, mock_get_droplet):
        mock_get_droplet.side_effect = lambda s, x: \
            self._populate_droplet(x, mock_droplet())
        self._api.start_node(six.text_type('16'))
        self.assertEqual(0, mock_droplet().power_on.call_count,
                         'Running node node started again')

    def tearDown(self):
        self._api = None
        self._cluster_id = None
