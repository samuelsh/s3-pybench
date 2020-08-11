import gevent.monkey
import requests

gevent.monkey.patch_all()

import threading
import multiprocessing
import random
import os
import gipc
from gevent import pool, queue
from s3_stress.utils import s3_utils, workers, server_logger, stats_collector, helpers, config

logger = server_logger.Logger(name=__name__).logger


class WorkloadManager:
    def __init__(self, args, events_queue=None, _sentinel=None):
        self.args = args
        self.stop_event = multiprocessing.Event()
        self.events_queue = events_queue
        self._sentinel = _sentinel
        self.s3_process_pool = []
        self._stats_collector = stats_collector.StatsCollector(args.size)
        self.timer_thread = None
        self.event_listener_thread = None
        self.s3_config = config.ensure_config()
        self.results_queue = queue.Queue()
        if not self.s3_config:
            raise RuntimeError("Bad or missing config file")

    @property
    def stats_collector(self):
        return self._stats_collector

    def process_pool_starter(self):
        logger.info(f"Process Pool [{os.getpid()}] started")
        self.event_listener_thread = threading.Thread(target=event_queue_listener,
                                                      args=(self.events_queue, self.stop_event, self._sentinel))
        self.timer_thread = threading.Thread(target=helpers.timeout_and_signal,
                                             args=(self.args.timeout, self.events_queue, self._sentinel))
        self.event_listener_thread.start()
        if self.args.timeout > 0:
            self.timer_thread.start()
        if not self.args.multibucket:
            s3_utils.mkbucket(config=self.s3_config, bucket_name=self.args.bucket, ignore_existing=True)
        else:
            for i in range(self.args.processes):
                s3_utils.mkbucket(config=self.s3_config, bucket_name=f"{self.args.bucket}-{i}", ignore_existing=True)

        for i in range(self.args.processes):
            path = '/' + (self.args.hostname + '/' if self.args.hostname is not None else "") + str(i)
            logger.info("Started work on path {}".format(path))
            bucket_name = self.args.bucket if not self.args.multibucket else f"{self.args.bucket}-{i}"
            self.s3_process_pool.append(
                gipc.start_process(self.gevent_pool_starter, args=(bucket_name,)))
        for p in self.s3_process_pool:
            p.join()
        res = None
        while True:
            try:
                res = self.results_queue.get()
                logger.info(f"Results queue res={res}")
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as requests_err:
                raise RuntimeError(f"Future {res} raised exception: {requests_err}")
            except queue.Empty:
                break
            except Exception as e:
                raise RuntimeError(f"Internal Error: {e}")

    def gevent_pool_starter(self, bucket_name):
        the_pool = pool.Pool(self.args.threads)
        for i in range(self.args.threads):
            path = '/' + (self.args.hostname + '/' if self.args.hostname is not None else "") + str(i)
            the_pool.apply_async(
                handle_http_methods(self.args.workload if self.args.workload != 'MIXED' else
                                    random.choice(['GET', 'PUT', 'DELETE'])),
                kwds=dict(bucket_name=bucket_name, path=path, thread_id=f"{10000 + i}",
                          data_chunk_size=self.args.size, metadata=self.args.metadata,
                          min_size=self.args.min_size, max_size=self.args.max_size, stone=self.args.stone,
                          config=self.s3_config, stats_collector=self._stats_collector, stop_event=self.stop_event,
                          results_queue=self.results_queue))
        the_pool.join()

    def shutdown(self):
        if self.timer_thread and self.timer_thread.is_alive():
            logger.info("Stopping timer thread")
            self.timer_thread.cancel()


def event_queue_listener(events_queue, stop_event, _sentinel):
    while True:
        try:
            event = events_queue.get(timeout=1)
            logger.info(f"{__name__} received event={event}")
            if event is _sentinel or stop_event.is_set():
                logger.info("Workload manager received stop event. Stopping the workload...")
                stop_event.set()
                break
        except queue.Empty:
            pass


def handle_http_methods(method):
    return {
        'GET': workers.s3_get_worker,
        'PUT': workers.s3_put_worker,
        'DELETE': workers.s3_delete_worker,
    }[method]
