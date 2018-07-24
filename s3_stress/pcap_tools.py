import json

import os
import pyshark
import boto3

from s3_stress.s3_stress_runner import server_logger

logger = server_logger.Logger(name=__name__).logger
TMP_FILE = 'tmpfile'


def open_pcap_file(file_name, display_filter='http'):
    """
    :param file_name: str
    :param display_filter: str
    :return: FileCapture
    """
    return pyshark.FileCapture(file_name, display_filter=display_filter)


def get_next_packet(cap):
    """
    Yield next http request packet
    :param cap: FileCapture
    :return: Layer
    """
    for packet in cap:
        try:
            if packet['http'].request_method:
                yield packet['http']
        except AttributeError:
            pass


def s3_connector(s3_method):
    """
    s3_connection decorator method
    :param s3_method: function
    :return:
    """
    with open('.s3_config.json', 'r') as f:
        config = json.load(f)
    s3_resource = boto3.resource('s3', use_ssl=False,
                                 endpoint_url=config['endpoint_url'],
                                 aws_access_key_id=config['aws_access_key_id'],
                                 aws_secret_access_key=config['aws_secret_key'],
                                 config=boto3.session.Config(config['extra_params']),
                                 region_name='us-east-1'
                                 )
    s3_client = s3_resource.meta.client

    def method_wrapper(**kwargs):
        s3_method(s3_resource=s3_resource, s3_client=s3_client, **kwargs)

    return method_wrapper


def s3_path_parser(request_uri):
    path_to_list = request_uri.strip('/').split('/')
    bucket_name = path_to_list[0]
    object_name = '/' + '/'.join(path_to_list[1:])

    return bucket_name, object_name


def handle_http_methods(method, **kwargs):
    return {
        'GET': handle_get,
        'PUT': handle_put,
        'DELETE': handle_delete,
    }[method](**kwargs)


@s3_connector
def handle_get(**kwargs):
    s3 = kwargs['s3_resource']
    packet = kwargs['packet']

    if packet.request_uri != '/':  # Skip
        if packet.request_uri[-1] == '/':
            bucket = s3.Bucket(packet.request_uri.strip('/'))
            logger.debug("Get Bucket {}".format(bucket.name))
        else:
            bucket_name, object_name = s3_path_parser(packet.request_uri)
            logger.debug("Request URI parsed >>> bucket: {}, object: {}".format(bucket_name, object_name))
            s3_object = s3.Object(bucket_name, object_name)
            s3_object.get()['Body'].read()


@s3_connector
def handle_put(**kwargs):
    s3 = kwargs['s3_resource']
    packet = kwargs['packet']

    if packet.request_uri != '/':
        if packet.request_uri[-1] == '/':  # This is bucket
            bucket = s3.create_bucket(Bucket=packet.request_uri.strip('/'))
            logger.debug("Bucket {} created".format(bucket.name))
        else:  # This is object
            bucket_name, object_name = s3_path_parser(packet.request_uri)
            logger.debug("Request URI parsed >>> bucket: {}, object: {}".format(bucket_name, object_name))
            with open(TMP_FILE, 'wb') as fh:
                fh.write(os.urandom(int(packet.content_length)))
                fh.flush()
                os.fsync(fh.fileno())
            with open(TMP_FILE, 'rb') as fh:
                s3.Bucket(bucket_name).put_object(Key=object_name, Body=fh)
            os.remove(TMP_FILE)


@s3_connector
def handle_delete(**kwargs):
    s3 = kwargs['s3_resource']
    packet = kwargs['packet']

    if packet.request_uri != '/':
        if packet.request_uri[-1] == '/':  # This is bucket
            s3.Bucket(packet.request_uri.strip('/')).delete()
        else:  # This is object
            bucket_name, object_name = s3_path_parser(packet.request_uri)
            logger.debug("Request URI parsed >>> bucket: {}, object: {}".format(bucket_name, object_name))
            s3.Object(bucket_name, object_name).delete()
