#!/usr/bin/env python3

"""
S3 Stress testing tool - 2018 (c)
"""
__author__ = 'samuel'

import gevent.monkey
gevent.monkey.patch_all()

import requests
import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from gevent.pool import Pool
from gevent.queue import Queue
from threading import Event
from s3_stress.utils.connectors import s3_connector
from s3_stress.utils import server_logger
from s3_stress.utils import config

logger = server_logger.Logger(name=__name__).logger
stop_event = Event()

MAX_WORKER_THREADS = 50
MAX_WORKER_PROCESSES = 20


def get_args():
    parser = argparse.ArgumentParser(
        description='S3 Stress tool (c) - 2018')
    parser.add_argument('bucket', type=str, help="Bucket name")
    parser.add_argument('-t', '--threads', type=int, help="Number of concurrent threads", default=MAX_WORKER_THREADS)
    parser.add_argument('-p', '--processes', type=int, help="Number of concurrent processes",
                        default=MAX_WORKER_PROCESSES)
    args = parser.parse_args()
    return args


def gevent_pool_starter(bucket_name, path, threads):
    objects_queue = Queue()
    pool = Pool(threads)
    for i in range(threads):
        pool.apply_async(s3_delete_worker, kwds=dict(bucket_name=bucket_name, path=path, objects_queue=objects_queue,
                                                     thread_id="{}".format(10000 + i)))
    pool.join()


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


def process_pool_starter(bucket_name, processes, threads):
    with ProcessPoolExecutor(processes) as s3_process_pool:
        for i in range(processes):
            path = '/' + str(i)
            logger.info("Started work on path {}".format(path))
            s3_process_pool.submit(gevent_pool_starter, bucket_name, path, threads)


def main():
    args = get_args()
    config.ensure_config()
    process_pool_starter(args.bucket, args.processes, args.threads)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
    except KeyboardInterrupt:
        stop_event.set()
