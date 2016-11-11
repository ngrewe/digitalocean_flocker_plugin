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
