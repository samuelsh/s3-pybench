#!/usr/bin/env python3

"""
S3 Stress testing tool - 2018 (c)
"""

__author__ = 'samuel'

import gevent.monkey

gevent.monkey.patch_all()

import sys
import argparse
import random
import requests
import os
from concurrent.futures import ProcessPoolExecutor
from string import ascii_letters
from threading import Event
from botocore.exceptions import ClientError
from gevent.pool import Pool
from gevent.queue import Queue
from s3_stress.utils import server_logger
from s3_stress.utils.connectors import s3_connector
from s3_stress.utils import config

logger = server_logger.Logger(name=__name__).logger
stop_event = Event()

MAX_WORKER_THREADS = 50
MAX_WORKER_PROCESSES = 20
RANDOM_CHUNK_2M = os.urandom(2 * 1024 * 1024)
RANDOM_CHUNK_1M = os.urandom(1 * 1024 * 1024)
RANDOM_CHUNK_256K = os.urandom(256 * 1024)
RANDOM_CHUNK_4K = os.urandom(4 * 1024)
DATA_CHUNKS = {"4K": 4 * 1024, "256K": 256 * 1024, "1M": 1024 * 1024, "2M": 2 * 1024 * 1024}
TMP_FILE = 'tmpfile'


def generate_valid_object_name_ascii(length):
    specials = "!-_.*'()"
    return ''.join(random.choice(ascii_letters + specials) for _ in range(length))


def generate_valid_object_name():
    return "{}".format(random.randint(1, 64000))


def get_args():
    parser = argparse.ArgumentParser(
        description='S3 Stress tool (c) - 2018')
    parser.add_argument('bucket', type=str, help="Bucket name")
    parser.add_argument('--hostname', type=str, help="Host name", default=None)
    parser.add_argument('-t', '--threads', type=int, help="Number of concurrent threads", default=MAX_WORKER_THREADS)
    parser.add_argument('-p', '--processes', type=int, help="Number of concurrent processes",
                        default=MAX_WORKER_PROCESSES)
    parser.add_argument('-s', '--size', choices=['4K', '256K', '1M', '2M'], default="4K", help="Data file size")
    parser.add_argument('-w', '--workload', choices=['GET', 'PUT', 'DELETE', 'MIXED'], default="PUT",
                        help="Workload")
    args = parser.parse_args()
    return args


def gevent_pool_starter(method, bucket_name, path, threads, data_chunk_size, s3_config):
    objects_queue = Queue()
    pool = Pool(threads)
    for i in range(threads):
        pool.apply_async(handle_http_methods(method if method != 'MIXED' else random.choice(['GET', 'PUT', 'DELETE'])),
                         kwds=dict(bucket_name=bucket_name, path=path, objects_queue=objects_queue,
                                   thread_id="{}".format(10000 + i), data_chunk_size=data_chunk_size,
                                   config=s3_config))
    pool.join()


def handle_http_methods(method):
    return {
        'GET': s3_get_worker,
        'PUT': s3_put_worker,
        'DELETE': s3_delete_worker,
    }[method]


@s3_connector
def s3_put_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    data_chunk = os.urandom(kwargs['data_chunk_size'])
    logger.info("PUT Worker started: {}".format(kwargs))
    path = os.path.join(path, kwargs['thread_id'])
    path_counter = 100
    session = requests.Session()
    while not stop_event.is_set():
        file_counter = 0
        while file_counter < 5000 and not stop_event.is_set():
            object_name = str(file_counter)
            full_object_name = "/".join([path, str(path_counter), object_name])
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                session.put(url, data=data_chunk, auth=kwargs['auth'])
            except requests.ConnectionError as con_err:
                logger.error("{} : PUT {}".format(con_err.args[0], full_object_name))
            except requests.Timeout as timeout:
                logger.error("PUT request {} Timed out. {}".format(full_object_name, timeout.strerror))
            except KeyboardInterrupt:
                stop_event.set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.info("Worker stopped")


@s3_connector
def s3_get_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    path = os.path.join(path, kwargs['thread_id'])
    logger.info("GET Worker started: {}".format(kwargs))
    path_counter = 100
    session = requests.Session()
    while not stop_event.is_set():
        file_counter = 0
        while file_counter < 5000 and not stop_event.is_set():
            object_name = str(file_counter)
            full_object_name = "/".join([path, str(path_counter), object_name])
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                session.get(url, auth=kwargs['auth'])
            except requests.ConnectionError as con_err:
                logger.error("{} : GET {}".format(con_err.strerror, full_object_name))
            except requests.Timeout as timeout:
                logger.error("GET request {} Timed out. {}".format(full_object_name, timeout.strerror))
            except KeyboardInterrupt:
                stop_event.set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.info("Worker stopped")


@s3_connector
def s3_delete_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    logger.info("DELETE Worker started: {}".format(kwargs))
    path = os.path.join(path, kwargs['thread_id'])
    session = requests.Session()
    path_counter = 100
    while not stop_event.is_set():
        file_counter = 0
        while file_counter < 5000 and not stop_event.is_set():
            object_name = str(file_counter)
            full_object_name = os.path.join(path, str(path_counter), object_name)
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                session.delete(url, auth=kwargs['auth'])
            except requests.ConnectionError as con_err:
                logger.error("{} : DELETE {}".format(con_err.strerror, full_object_name))
                raise con_err
            except requests.Timeout as timeout:
                logger.error("DELETE request {} Timed out. {}".format(full_object_name, timeout.strerror))
            except KeyboardInterrupt:
                stop_event.set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.info("Worker stopped")


@s3_connector
def mkbucket(**kwargs):
    s3 = kwargs['s3_resource']
    bucket_name = kwargs['bucket_name']
    bucket = s3.create_bucket(Bucket=bucket_name)
    logger.debug("Bucket {} created".format(bucket.name))


def process_pool_starter(method, bucket_name, processes, threads, hostname, data_chunk_size, s3_config):
    try:
        mkbucket(config=s3_config, bucket_name=bucket_name)
    except ClientError as boto_err:
        if boto_err.response['ResponseMetadata']['HTTPStatusCode'] != 409:  # allow rewriting existing bucket content
            raise boto_err

    with ProcessPoolExecutor(processes) as s3_process_pool:
        for i in range(processes):
            path = '/' + (hostname + '/' if hostname is not None else "") + str(i)
            logger.info("Started work on path {}".format(path))
            s3_process_pool.submit(gevent_pool_starter, method, bucket_name, path, threads, data_chunk_size, s3_config)


def main():
    args = get_args()
    s3_config = config.ensure_config()
    if not s3_config:
        raise RuntimeError("Bad or missing config file")
    process_pool_starter(args.workload, args.bucket, args.processes, args.threads, args.hostname,
                         DATA_CHUNKS[args.size], s3_config)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
    except KeyboardInterrupt:
        stop_event.set()
