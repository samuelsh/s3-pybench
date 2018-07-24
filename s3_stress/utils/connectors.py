
import boto3
import random

from s3_stress.utils import ip_utils
from s3_stress.utils.s3_auth_utils import do_sign_request

"""
Connector decorator, provides boto resource and client, or http generated from VIP range
"""


def s3_connector(s3_method):
    """
    s3_connection decorator method
    :param s3_method: function
    :return:
    """

    def method_wrapper(**kwargs):
        config = kwargs['config']
        vip_range = [ip for ip in ip_utils.range_ipv4(config['vip_start'], config['vip_end'])]
        final_vip = random.choice(vip_range)

        endpoint_url = "http://{}:{}".format(final_vip, config['port'])
        s3_resource = boto3.resource('s3', use_ssl=False,
                                     endpoint_url=endpoint_url,
                                     aws_access_key_id=config['aws_access_key_id'],
                                     aws_secret_access_key=config['aws_secret_key'],
                                     config=boto3.session.Config(config['extra_params']),
                                     region_name='us-east-1'
                                     )
        s3_client = s3_resource.meta.client
        auth = do_sign_request(config, f"{final_vip}:{config['port']}")
        s3_method(s3_resource=s3_resource, s3_client=s3_client, endpoint_url=endpoint_url, auth=auth, **kwargs)

    return method_wrapper
