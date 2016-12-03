from bitmath import GiB
from flocker.node.agents.testtools import get_blockdeviceapi_with_cleanup
from flocker.node.agents.testtools import make_iblockdeviceapi_tests
from flocker.node.agents.testtools import make_icloudapi_tests
from flocker.node.agents.testtools import require_backend
import six


@require_backend('digitalocean_flocker_plugin')
def do_blockdeviceapi_for_test(test_case):
    return get_blockdeviceapi_with_cleanup(test_case)


MIN_ALLOCATION_SIZE = GiB(1).to_Byte().value

MIN_ALLOCATION_UNIT = GiB(1).to_Byte().value


class DigitalOceanBlockDeviceAPITests(
    make_iblockdeviceapi_tests(
        blockdevice_api_factory=do_blockdeviceapi_for_test,
        unknown_blockdevice_id_factory=lambda x: six.text_type(2147483647))):
    """Functional tests for DigitalOcean ``IBlockDeviceAPI`` implementation
        """


class DigitalOceanCloudAPITests(
    make_icloudapi_tests(
        blockdevice_api_factory=do_blockdeviceapi_for_test)):
    """Functional tests for DigitalOcean ``ICloudAPI`` implementation"""
