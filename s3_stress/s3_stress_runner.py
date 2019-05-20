#!/usr/bin/env python3

"""
S3 Stress testing tool - 2018 (c)
"""
import json

__author__ = 'samuel'

import gevent.monkey

gevent.monkey.patch_all()

import argparse
import random
import requests
import os
import uuid
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor
from string import ascii_letters
from botocore.exceptions import ClientError
from gevent.event import Event
from gevent.pool import Pool
from gevent.queue import Queue
from s3_stress.utils import server_logger
from s3_stress.utils.connectors import s3_connector
from s3_stress.utils import config
from timeit import default_timer as timer

logger = server_logger.Logger(name=__name__).logger
stop_event = Event()

KB1 = 1024
MB1 = KB1 * 1024
MAX_WORKER_THREADS = 50
MAX_WORKER_PROCESSES = 20
RANDOM_CHUNK_2M = os.urandom(2 * 1024 * 1024)
RANDOM_CHUNK_1M = os.urandom(1 * 1024 * 1024)
RANDOM_CHUNK_256K = os.urandom(256 * 1024)
RANDOM_CHUNK_4K = os.urandom(4 * 1024)
DATA_CHUNKS = {"4K": 4 * KB1, "256K": 256 * KB1, "1M": MB1, "2M": 2 * MB1, "5M": 5 * MB1,
               "10M": 10 * MB1, "20M": 20 * MB1}
DATA_BUFFERS = {"4K": os.urandom(4 * KB1), "256K": os.urandom(256 * KB1), "1M": os.urandom(MB1),
                "2M": os.urandom(2 * MB1), "5M": os.urandom(5 * MB1),
                "10M": os.urandom(10 * MB1), "20M": os.urandom(20 * MB1)}
TMP_FILE = 'tmpfile'

total_uploaded_files = multiprocessing.Value('L', 0)
total_time_spent_in_upload = multiprocessing.Value('d', 0)

total_downloaded_files = multiprocessing.Value('L', 0)
total_time_spent_in_download = multiprocessing.Value('d', 0)

total_deleted_files = multiprocessing.Value('L', 0)
total_time_spent_in_deletion = multiprocessing.Value('d', 0)

io_size = multiprocessing.Value('I', 0)

s3_process_pool = None


class TimerThread(threading.Timer):
    def __init__(self, func, interval=60):
        super().__init__(interval, func)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)


def print_stats(workload):
    global total_uploaded_files, total_time_spent_in_upload, io_size
    global total_downloaded_files, total_time_spent_in_download
    workload_stats = {}

    if workload == "PUT":
        total_bytes_written = total_uploaded_files.value * io_size.value
        logger.info(f"S3_STATS: Total uploaded files: {total_uploaded_files.value}")
        logger.info(f"S3_STATS: IO chunk size: {io_size.value}")
        logger.info(f"S3_STATS: Total time spent in uploads (sec): {total_time_spent_in_upload.value}")
        logger.info(f"S3_STATS: Total bytes written to storage: {total_bytes_written}")
        logger.info(f"S3_STATS: Average MB per upload: {total_bytes_written / total_uploaded_files.value / MB1}")
        logger.info(f"S3_STATS: Average Write Bandwidth (MB/s): "
                    f"{total_bytes_written / MB1 / total_time_spent_in_upload.value}")
        logger.info(f"S3_STATS: Average Latency (ms): "
                    f"{total_time_spent_in_upload.value / (total_bytes_written / io_size.value) * 1000}")
        logger.info(f"S3_STATS: Average Write IOPS: "
                    f"{(total_bytes_written / io_size.value) / total_time_spent_in_upload.value}")
        workload_stats = {
            'total_uploaded_files': total_uploaded_files.value,
            'io_chunk_size': io_size.value,
            'total_time_spent_in_uploads': total_time_spent_in_upload.value,
            'total_bytes_written': total_bytes_written,
            'average_mb_per_upload': total_bytes_written / total_uploaded_files.value / MB1,
            'average_write_bw': total_bytes_written / MB1 / total_time_spent_in_upload.value,
            'average_latency': total_time_spent_in_upload.value / (total_bytes_written / io_size.value) * 1000,
            'average_write_iops': (total_bytes_written / io_size.value) / total_time_spent_in_upload.value
        }
    elif workload == "GET":
        total_bytes_read = total_downloaded_files.value * io_size.value
        logger.info(f"S3_STATS: Total downloaded files: {total_downloaded_files.value}")
        logger.info(f"S3_STATS: IO chunk size: {io_size.value}")
        logger.info(f"S3_STATS: Total time spent in downloads (sec): {total_time_spent_in_download.value}")
        logger.info(f"S3_STATS: Total bytes read from storage: {total_bytes_read}")
        logger.info(f"S3_STATS: Average MB per download: {total_bytes_read / total_downloaded_files.value / MB1}")
        logger.info(f"S3_STATS: Average Read Bandwidth (MB/s): "
                    f"{total_bytes_read / MB1 / total_time_spent_in_download.value}")
        logger.info(f"S3_STATS: Average Latency (ms): "
                    f"{total_time_spent_in_download.value / (total_bytes_read / io_size.value) * 1000}")
        logger.info(f"S3_STATS: Average Read IOPS: "
                    f"{(total_bytes_read / io_size.value) / total_time_spent_in_download.value}")
        workload_stats = {
            'total_downloaded_files': total_downloaded_files.value,
            'io_chunk_size': io_size.value,
            'total_time_spent_in_downloads': total_time_spent_in_download.value,
            'total_bytes_read': total_bytes_read,
            'average_mb_per_download': total_bytes_read / total_downloaded_files.value / MB1,
            'average_read_bw': total_bytes_read / MB1 / total_time_spent_in_download.value,
            'average_latency': total_time_spent_in_download.value / (total_bytes_read / io_size.value) * 1000,
            'average_read_iops': (total_bytes_read / io_size.value) / total_time_spent_in_download.value
        }
    with open('/tmp/s3_stats.json', 'w') as f:
        json.dump(workload_stats, f)


def upload_profiler(f):
    """
    Profiler decorator to measure duration of S3 PUT method
    """

    def wrapper(session, url, **kwargs):
        global total_time_spent_in_upload
        start = timer()
        res = f(session, url, **kwargs)
        end = timer()
        with total_time_spent_in_upload.get_lock():
            total_time_spent_in_upload.value += (end - start)
        return res

    return wrapper


def download_profiler(f):
    """
    Profiler decorator to measure duration of S3 GET method
    """

    def wrapper(session, url, **kwargs):
        global total_time_spent_in_download
        start = timer()
        res = f(session, url, **kwargs)
        end = timer()
        with total_time_spent_in_download.get_lock():
            total_time_spent_in_download.value += (end - start)
        return res

    return wrapper


def delete_profiler(f):
    """
    Profiler decorator to measure duration of S3 GET method
    """

    def wrapper(**kwargs):
        global total_time_spent_in_deletion
        start = timer()
        f(**kwargs)
        end = timer()
        with total_time_spent_in_deletion.get_lock():
            total_time_spent_in_deletion.value += (end - start)

    return wrapper


def get_random_string(length):
    return ''.join(random.choice(ascii_letters) for _ in range(length))


def generate_valid_object_name_ascii(length):
    specials = "!-_.*'()"
    return ''.join(random.choice(ascii_letters + specials) for _ in range(length))


def generate_valid_object_name():
    return "{}".format(random.randint(1, 64000))


def generate_metadata_dict(md_dict_size=KB1 * 2):
    size = 0
    md_dict = {}
    while size < md_dict_size:
        md_key = uuid.uuid4().hex[:4]
        #  We'll use weighted probability to generate various metadata sizes until 2KB is filled up
        md_value = get_random_string(random.choices([1, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
                                                    weights=[20, 20, 20, 20, 10, 6, 1, 1, 1, 1])[0])
        if size + len(md_key + md_value) > md_dict_size:
            new_entry_size = md_dict_size - size
            if new_entry_size == 1:  # we can make additional key:value pair with one byte left so stopping here
                break
            leftover = 1 if new_entry_size % 2 else 0
            md_key = uuid.uuid4().hex[:new_entry_size // 2 + leftover]
            md_value = get_random_string(new_entry_size // 2)
        md_dict[f"x-amz-meta-{md_key}"] = md_value
        size += len(md_key + md_value)
    logger.info(f"MD Dictionary size: "
                f"{sum([len(k + v) for k, v in md_dict.items()]) - len('x-amz-meta-' * len(md_dict))}")
    return md_dict


def get_args():
    parser = argparse.ArgumentParser(
        description='S3 Stress tool (c) - 2018')
    parser.add_argument('bucket', type=str, help="Bucket name")
    parser.add_argument('--hostname', type=str, help="Host name", default=None)
    parser.add_argument('-t', '--threads', type=int, help="Number of concurrent threads", default=MAX_WORKER_THREADS)
    parser.add_argument('-p', '--processes', type=int, help="Number of concurrent processes",
                        default=MAX_WORKER_PROCESSES)
    parser.add_argument('-s', '--size', choices=['4K', '256K', '1M', '2M', '5M', '10M', '20M', 'MIXED'], default="4K",
                        help="Data file size")
    parser.add_argument('--min_size', type=int, default=KB1 * 4, help="Minimal upload size in bytes")
    parser.add_argument('--max_size', type=int, default=MB1 * 2, help="Maximal upload size in bytes")
    parser.add_argument('-w', '--workload', choices=['GET', 'PUT', 'DELETE', 'MIXED'], default="PUT",
                        help="Workload")
    parser.add_argument('-mb', '--multibucket', action="store_true", help="Creates new bucket for each process")
    parser.add_argument('-e', '--stone', action="store_true", help="Stop on Error")
    parser.add_argument('--metadata', action="store_true", help="Generated MetaData attributes for each object")
    parser.add_argument('--timeout', type=int, help="Runtime timeout (in seconds)", default=0)
    args = parser.parse_args()
    return args


def gevent_pool_starter(method, bucket_name, path, threads, metadata, data_chunk_size, min_size, max_size, stone,
                        s3_config):
    objects_queue = Queue()
    pool = Pool(threads)
    for i in range(threads):
        pool.apply_async(handle_http_methods(method if method != 'MIXED' else random.choice(['GET', 'PUT', 'DELETE'])),
                         kwds=dict(bucket_name=bucket_name, path=path, objects_queue=objects_queue,
                                   thread_id="{}".format(10000 + i), data_chunk_size=data_chunk_size, metadata=metadata,
                                   min_size=min_size, max_size=max_size, stone=stone, config=s3_config))
    pool.join()


def handle_http_methods(method):
    return {
        'GET': s3_get_worker,
        'PUT': s3_put_worker,
        'DELETE': s3_delete_worker,
    }[method]


@upload_profiler
def s3_put(session, url, **kwargs):
    return session.put(url, **kwargs)


@s3_connector
def s3_put_worker(**kwargs):
    global total_uploaded_files, io_size
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    metadata = generate_metadata_dict() if kwargs['metadata'] else {}
    random_chunk_size = random.randint(kwargs['min_size'], kwargs['max_size'])
    # in case user selected MIXED size, we use min/max size arguments to calculate size of random data chunk
    data_chunk = DATA_BUFFERS["20M"][:random_chunk_size] if kwargs['data_chunk_size'] == 'MIXED' \
        else DATA_BUFFERS[kwargs['data_chunk_size']]
    logger.info(f"PUT Worker started: URL: {kwargs['endpoint_url']} Bucket: {kwargs['bucket_name']} "
                f"Path: {kwargs['path']} Thread ID: {kwargs['thread_id']} Data Chunk Size: {kwargs['data_chunk_size']}")
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
                res = s3_put(session, url, data=data_chunk, auth=kwargs['auth'], headers=metadata)
                res.raise_for_status()
                with total_uploaded_files.get_lock():
                    total_uploaded_files.value += 1
            except (requests.ConnectionError, requests.HTTPError) as requests_err:
                logger.error("{} : PUT {}".format(requests_err.args[0], full_object_name))
                if kwargs['stone']:
                    stop_event.set()
                    raise
            except requests.Timeout as timeout:
                logger.error("PUT request {} Timed out. {}".format(full_object_name, timeout.strerror))
                if kwargs['stone']:
                    stop_event.set()
                    raise
            except KeyboardInterrupt:
                stop_event.set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.debug("Worker stopped")


@download_profiler
def s3_get(session, url, **kwargs):
    return session.get(url, **kwargs)


@s3_connector
def s3_get_worker(**kwargs):
    global total_downloaded_files
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    path = os.path.join(path, kwargs['thread_id'])
    logger.info(f"GET Worker started: URL: {kwargs['endpoint_url']} Bucket: {kwargs['bucket_name']} "
                f"Path: {kwargs['path']} Thread ID: {kwargs['thread_id']} Data Chunk Size: {kwargs['data_chunk_size']}")
    path_counter = 100
    session = requests.Session()
    while not stop_event.is_set():
        file_counter = 0
        while file_counter < 5000 and not stop_event.is_set():
            object_name = str(file_counter)
            full_object_name = "/".join([path, str(path_counter), object_name])
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                res = s3_get(session, url, auth=kwargs['auth'])
                res.raise_for_status()
                with total_downloaded_files.get_lock():
                    total_downloaded_files.value += 1
            except requests.HTTPError as http_err:
                if http_err.errno == 404:
                    logger.info("Can't find objects to read, work done...")
                    break
                else:
                    logger.error("{} : PUT {}".format(http_err.args[0], full_object_name))
                    if kwargs['stone']:
                        stop_event.set()
            except requests.ConnectionError as con_err:
                logger.error("{} : GET {}".format(con_err.strerror, full_object_name))
                if kwargs['stone']:
                    stop_event.set()
                    raise
            except requests.Timeout as timeout:
                logger.error("GET request {} Timed out. {}".format(full_object_name, timeout.strerror))
                if kwargs['stone']:
                    stop_event.set()
                    raise
            except KeyboardInterrupt:
                stop_event.set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.debug("Worker stopped")


@s3_connector
@delete_profiler
def s3_delete_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    logger.info(f"DELETE Worker started: URL: {kwargs['endpoint_url']} Bucket: {kwargs['bucket_name']} "
                f"Path: {kwargs['path']} Thread ID: {kwargs['thread_id']} Data Chunk Size: {kwargs['data_chunk_size']}")
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
                if kwargs['stone']:
                    stop_event.set()
                    raise
            except requests.Timeout as timeout:
                logger.error("DELETE request {} Timed out. {}".format(full_object_name, timeout.strerror))
                if kwargs['stone']:
                    stop_event.set()
                    raise
            except KeyboardInterrupt:
                stop_event.set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.debug("Worker stopped")


@s3_connector
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


def process_pool_starter(args, s3_config):
    global s3_process_pool
    if not args.multibucket:
        mkbucket(config=s3_config, bucket_name=args.bucket, ignore_existing=True)
    else:
        for i in range(args.processes):
            mkbucket(config=s3_config, bucket_name=f"{args.bucket}-{i}", ignore_existing=True)

    futures = []
    s3_process_pool = ProcessPoolExecutor(args.processes)
    with s3_process_pool:
        for i in range(args.processes):
            path = '/' + (args.hostname + '/' if args.hostname is not None else "") + str(i)
            logger.info("Started work on path {}".format(path))
            bucket_name = args.bucket if not args.multibucket else f"{args.bucket}-{i}"
            futures.append(s3_process_pool.submit(gevent_pool_starter, args.workload, bucket_name, path, args.threads,
                                                  args.metadata, args.size, args.min_size, args.max_size, args.stone,
                                                  s3_config))
            for future in futures:
                logger.info(f"Future {future}: {future.result()}")


def main():
    global io_size
    args = get_args()
    try:
        io_size.value = DATA_CHUNKS[args.size]
    except KeyError:
        # if chunk size is MIXED, io_size will be average of all possible data chunks
        io_size.value = sum([val for val in DATA_CHUNKS.values()]) // len(DATA_CHUNKS)
    timer_thread = TimerThread(stop_event.set, interval=args.timeout)
    s3_config = config.ensure_config()
    if args.timeout > 0:
        timer_thread.start()
    if not s3_config:
        raise RuntimeError("Bad or missing config file")
    try:
        process_pool_starter(args, s3_config)
    except KeyboardInterrupt:
        stop_event.set()
    except Exception as e:
        logger.exception(e)
        raise RuntimeError(f"{e}")
    finally:
        logger.info("Shutting down process pool")
        s3_process_pool.shutdown()
        logger.info("Printing stats")
        print_stats(args.workload)
        if timer_thread.is_alive():
            logger.info("Stopping timer thread")
            timer_thread.cancel()
        logger.info("All task stopped. Bye Bye...")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError:
        exit(1)
