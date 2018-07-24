#!/usr/bin/env python3

"""
S3 Stress testing tool - 2018 (c)
"""
import gevent.monkey

gevent.monkey.patch_all()

import sys
import requests
import itertools
import argparse
import queue
import random
import os
from string import ascii_letters
from threading import Event
from botocore.exceptions import ClientError
from gevent.pool import Pool
from  s3_stress.utils import server_logger
from s3_stress.utils.connectors import s3_connector
from s3_stress.utils import config

__author__ = 'samuel'

logger = server_logger.Logger(name=__name__).logger
stop_event = Event()

MAX_WORKER_THREADS = 50
MAX_WORKER_PROCESSES = 20
RANDOM_CHUNK_2M = os.urandom(2 * 1024 * 1024)
RANDOM_CHUNK_1M = os.urandom(1 * 1024 * 1024)
RANDOM_CHUNK_256K = os.urandom(256 * 1024)
RANDOM_CHUNK_4K = os.urandom(4 * 1024)
RANDOM_CHUNK_36K = os.urandom(36 * 1024)
ZEROED_CHUNK_36K = '\0' * (36 * 1024)
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
    parser.add_argument('-n', '--numfiles', type=int, help="Number of files per directory", default=5000)
    parser.add_argument('-w', '--width', type=int, help="Number of directories", default=100)
    parser.add_argument('-d', '--depth', type=int, help="Directory tree depth", default=3)
    parser.add_argument('-t', '--threads', type=int, help="Number of concurrent threads", default=MAX_WORKER_THREADS)
    args = parser.parse_args()
    return args


@s3_connector
def mkbucket(**kwargs):
    s3 = kwargs['s3_resource']
    bucket_name = kwargs['bucket_name']
    bucket = s3.create_bucket(Bucket=bucket_name)
    logger.debug("Bucket {} created".format(bucket.name))


def build_s3_dir_tree(**kwargs):
    bucket_name = kwargs['bucket_name']
    depth = kwargs['depth']
    num_files = kwargs['numfiles']
    keys_queue = queue.Queue()
    permutations = list(itertools.permutations(range(1, depth + 1)))
    random.shuffle(permutations)
    for p in permutations:
        full_object_path = os.path.join('/', '/'.join(map(str, p)))
        for object_index in range(num_files):
            keys_queue.put(os.path.join(full_object_path, str(object_index)))
    logger.info("Starting PUT TreadPool")
    run_gevent_put_pool(kwargs['threads'], bucket_name, keys_queue)


@s3_connector
def put_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    keys_queue = kwargs['keys_queue']
    session = requests.Session()
    full_object_name = None
    while True or not stop_event.is_set():
        try:
            full_object_name = keys_queue.get_nowait()
            url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
            session.put(url, data=RANDOM_CHUNK_4K, auth=kwargs['auth'])
        except requests.ConnectionError as con_err:
            logger.error("{} : PUT {}".format(con_err.args[0], full_object_name))
        except requests.Timeout as timeout:
            logger.error("PUT request {} Timed out. {}".format(full_object_name, timeout.strerror))
        except queue.Empty:
            logger.info("Empty queue. Stopping PUT worker")
            break


def s3_delete_all_objects(**kwargs):
    bucket_name = kwargs['bucket_name']
    depth = kwargs['depth']
    num_files = kwargs['numfiles']
    keys_queue = queue.Queue()
    permutations = list(itertools.permutations(range(1, depth + 1)))
    for p in permutations:
        full_object_path = os.path.join('/', '/'.join(map(str, p)))
        for object_index in range(num_files):
            keys_queue.put(os.path.join(full_object_path, str(object_index)))
    logger.info("Starting DELETE TreadPool")
    run_gevent_delete_pool(kwargs['threads'], bucket_name, keys_queue)


@s3_connector
def delete_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    keys_queue = kwargs['keys_queue']
    session = requests.Session()
    full_object_name = None
    while True or not stop_event.is_set():
        try:
            full_object_name = keys_queue.get_nowait()
            url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
            session.delete(url, auth=kwargs['auth'])
        except requests.ConnectionError as conn_err:
            logger.error("{} : PUT {}".format(conn_err.args[0], full_object_name))
        except requests.Timeout as timeout:
            logger.error("PUT request {} Timed out. {}".format(full_object_name, timeout.strerror))
        except queue.Empty:
            break
        except KeyboardInterrupt:
            stop_event.set()


def run_gevent_put_pool(threads, bucket_name, keys_queue):
    pool = Pool(threads)
    for i in range(threads):
        pool.apply_async(put_worker, kwds=dict(bucket_name=bucket_name, keys_queue=keys_queue))
    pool.join()


def run_gevent_delete_pool(threads, bucket_name, keys_queue):
    pool = Pool(threads)
    for i in range(threads):
        pool.apply_async(delete_worker, kwds=dict(bucket_name=bucket_name, keys_queue=keys_queue))
    pool.join()


def main():
    args = get_args()
    config.ensure_config()
    try:
        mkbucket(bucket_name=args.bucket)
    except ClientError as boto_err:
        if boto_err.response['ResponseMetadata']['HTTPStatusCode'] != 409:  # allow rewriting existing bucket content
            raise boto_err
    build_s3_dir_tree(bucket_name=args.bucket, depth=args.depth, numfiles=args.numfiles, threads=args.threads)
    s3_delete_all_objects(bucket_name=args.bucket, depth=args.depth, numfiles=args.numfiles, threads=args.threads)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
    except KeyboardInterrupt:
        stop_event.set()
