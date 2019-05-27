import gevent.monkey
gevent.monkey.patch_all()

import os
import random
import requests
from s3_stress.utils import profilers, connectors, helpers, consts
from s3_stress.utils import server_logger

logger = server_logger.Logger(name=__name__).logger


@profilers.upload_profiler
def s3_put(session, url, **kwargs):
    return session.put(url, data=kwargs['data'], auth=kwargs['auth'], headers=kwargs['headers'])


@connectors.s3_connector
def s3_put_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    metadata = helpers.generate_metadata_dict() if kwargs['metadata'] else {}
    random_chunk_size = random.randint(kwargs['min_size'], kwargs['max_size'])
    # in case user selected MIXED size, we use min/max size arguments to calculate size of random data chunk
    data_chunk = consts.DATA_BUFFERS["20M"][:random_chunk_size] if kwargs['data_chunk_size'] == 'MIXED' \
        else consts.DATA_BUFFERS[kwargs['data_chunk_size']]
    logger.info(f"PUT Worker started: URL: {kwargs['endpoint_url']} Bucket: {kwargs['bucket_name']} "
                f"Path: {kwargs['path']} Thread ID: {kwargs['thread_id']} Data Chunk Size: {kwargs['data_chunk_size']}")
    path = os.path.join(path, kwargs['thread_id'])
    path_counter = 100
    session = requests.Session()
    while not kwargs['stop_event'].is_set():
        file_counter = 0
        while file_counter < 5000 and not kwargs['stop_event'].is_set():
            object_name = str(file_counter)
            full_object_name = "/".join([path, str(path_counter), object_name])
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                res = s3_put(session, url, data=data_chunk, auth=kwargs['auth'], headers=metadata,
                             counter=kwargs['stats_collector'].total_time_spent_in_upload)
                res.raise_for_status()
                with kwargs['stats_collector'].total_uploaded_files.get_lock():
                    kwargs['stats_collector'].total_uploaded_files.value += 1
            except (requests.ConnectionError, requests.HTTPError) as requests_err:
                logger.error("{} : PUT {}".format(requests_err.args[0], full_object_name))
                if kwargs['stone']:
                    kwargs['results_queue'].put(requests_err)
                    kwargs['stop_event'].set()
                    raise
            except requests.Timeout as timeout:
                logger.error("PUT request {} Timed out. {}".format(full_object_name, timeout.strerror))
                if kwargs['stone']:
                    kwargs['results_queue'].put(timeout)
                    kwargs['stop_event'].set()
                    raise
            except KeyboardInterrupt:
                kwargs['stop_event'].set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.debug("Worker stopped")


@profilers.download_profiler
def s3_get(session, url, **kwargs):
    return session.get(url, **kwargs)


@connectors.s3_connector
def s3_get_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    path = os.path.join(path, kwargs['thread_id'])
    logger.info(f"GET Worker started: URL: {kwargs['endpoint_url']} Bucket: {kwargs['bucket_name']} "
                f"Path: {kwargs['path']} Thread ID: {kwargs['thread_id']} Data Chunk Size: {kwargs['data_chunk_size']}")
    path_counter = 100
    session = requests.Session()
    while not kwargs['stop_event'].is_set():
        file_counter = 0
        while file_counter < 5000 and not kwargs['stop_event'].is_set():
            object_name = str(file_counter)
            full_object_name = "/".join([path, str(path_counter), object_name])
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                res = s3_get(session, url, auth=kwargs['auth'],
                             counter=kwargs['stats_collector'].total_time_spent_in_download)
                res.raise_for_status()
                with kwargs['stats_collector'].total_downloaded_files.get_lock():
                    kwargs['stats_collector'].total_downloaded_files.value += 1
            except requests.HTTPError as http_err:
                if http_err.errno == 404:
                    logger.info("Can't find objects to read, work done...")
                    break
                else:
                    logger.error("{} : PUT {}".format(http_err.args[0], full_object_name))
                    if kwargs['stone']:
                        kwargs['results_queue'].put(http_err)
                        kwargs['stop_event'].set()
            except requests.ConnectionError as con_err:
                logger.error("{} : GET {}".format(con_err.strerror, full_object_name))
                if kwargs['stone']:
                    kwargs['results_queue'].put(con_err)
                    kwargs['stop_event'].set()
                    raise
            except requests.Timeout as timeout:
                logger.error("GET request {} Timed out. {}".format(full_object_name, timeout.strerror))
                if kwargs['stone']:
                    kwargs['results_queue'].put(timeout)
                    kwargs['stop_event'].set()
                    raise
            except KeyboardInterrupt:
                kwargs['stop_event'].set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.debug("Worker stopped")


@profilers.delete_profiler
def s3_delete(session, url, **kwargs):
    return session.delete(url, **kwargs)


@connectors.s3_connector
def s3_delete_worker(**kwargs):
    bucket_name = kwargs['bucket_name']
    path = kwargs['path']
    logger.info(f"DELETE Worker started: URL: {kwargs['endpoint_url']} Bucket: {kwargs['bucket_name']} "
                f"Path: {kwargs['path']} Thread ID: {kwargs['thread_id']} Data Chunk Size: {kwargs['data_chunk_size']}")
    path = os.path.join(path, kwargs['thread_id'])
    session = requests.Session()
    path_counter = 100
    while not kwargs['stop_event'].is_set():
        file_counter = 0
        while file_counter < 5000 and not kwargs['stop_event'].is_set():
            object_name = str(file_counter)
            full_object_name = os.path.join(path, str(path_counter), object_name)
            try:
                url = "/".join([kwargs['endpoint_url'], bucket_name, full_object_name])
                res = s3_delete(session, url, auth=kwargs['auth'],
                                counter=kwargs['stats_collector'].total_time_spent_in_deletion)
                res.raise_for_status()
                with kwargs['stats_collector'].total_deleted_files.get_lock():
                    kwargs['stats_collector'].total_deleted_files.value += 1
            except (requests.ConnectionError, requests.HTTPError) as requests_err:
                logger.error("{} : DELETE {}".format(requests_err.strerror, full_object_name))
                if kwargs['stone']:
                    kwargs['results_queue'].put(requests_err)
                    kwargs['stop_event'].set()
                    raise
            except requests.Timeout as timeout:
                logger.error("DELETE request {} Timed out. {}".format(full_object_name, timeout.strerror))
                if kwargs['stone']:
                    kwargs['results_queue'].put(timeout)
                    kwargs['stop_event'].set()
                    raise
            except KeyboardInterrupt:
                kwargs['stop_event'].set()
            file_counter = file_counter + 1
        path_counter = path_counter + 1
    logger.debug("Worker stopped")
