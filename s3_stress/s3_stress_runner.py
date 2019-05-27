#!/usr/bin/env python3

"""
S3 Stress testing tool - 2018 (c)
"""

import botocore

__author__ = 'samuel'

import gevent.monkey
gevent.monkey.patch_all()

import argparse
from gevent import queue
from s3_stress.utils import consts, workload_manager
from s3_stress.utils import server_logger

logger = server_logger.Logger(name=__name__).logger


def get_args():
    parser = argparse.ArgumentParser(
        description='S3 Stress tool (c) - 2018')
    parser.add_argument('bucket', type=str, help="Bucket name")
    parser.add_argument('--hostname', type=str, help="Host name", default=None)
    parser.add_argument('-t', '--threads', type=int, help="Number of concurrent threads",
                        default=consts.MAX_WORKER_THREADS)
    parser.add_argument('-p', '--processes', type=int, help="Number of concurrent processes",
                        default=consts.MAX_WORKER_PROCESSES)
    parser.add_argument('-s', '--size', choices=['4K', '256K', '1M', '2M', '5M', '10M', '20M', 'MIXED'], default="4K",
                        help="Data file size")
    parser.add_argument('--min_size', type=int, default=consts.KB1 * 4, help="Minimal upload size in bytes")
    parser.add_argument('--max_size', type=int, default=consts.MB1 * 2, help="Maximal upload size in bytes")
    parser.add_argument('-w', '--workload', choices=['GET', 'PUT', 'DELETE', 'MIXED'], default="PUT",
                        help="Workload")
    parser.add_argument('-mb', '--multibucket', action="store_true", help="Creates new bucket for each process")
    parser.add_argument('-e', '--stone', action="store_true", help="Stop on Error")
    parser.add_argument('--metadata', action="store_true", help="Generate MetaData attributes for each object")
    parser.add_argument('--timeout', type=int, help="Runtime (in seconds)", default=0)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    events_queue = queue.Queue()
    _sentinel = object()

    manager = workload_manager.WorkloadManager(args, events_queue, _sentinel)
    try:
        manager.process_pool_starter()
    except KeyboardInterrupt:
        logger.info("User sent CTRL + C, stopping the workload...")
        events_queue.put(_sentinel)
    except botocore.exceptions.ClientError as boto_error:
        logger.error(f"Fatal boto error: {boto_error}")
        raise
    except RuntimeError as runtime_error:
        logger.error(f"Fatal runtime error error: {runtime_error}")
        raise
    except Exception as e:
        logger.error(f"Main process raised unhandled exception: {e}")
        raise
    finally:
        logger.info("Shutting down Workload Manager")
        manager.shutdown()
        logger.info("Printing stats")
        manager.stats_collector.dump_stats()
        logger.info("All task stopped. Bye Bye...")


if __name__ == "__main__":
    main()
