import gevent.monkey
gevent.monkey.patch_all()

from botocore.exceptions import ClientError
from s3_stress.utils import connectors, server_logger

logger = server_logger.Logger(name=__name__).logger


@connectors.s3_connector
def mkbucket(**kwargs):
    s3 = kwargs['s3_resource']
    bucket_name = kwargs['bucket_name']
    try:
        bucket = s3.create_bucket(Bucket=bucket_name)
        logger.debug("Bucket {} created".format(bucket.name))
    except ClientError as boto_err:
        # allow rewriting existing bucket content
        if boto_err.response['ResponseMetadata']['HTTPStatusCode'] == 409 and kwargs['ignore_existing']:
            pass
        else:
            raise boto_err
