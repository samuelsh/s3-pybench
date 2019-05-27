import gevent.monkey

gevent.monkey.patch_all()

import threading
import json
import random
import uuid
from string import ascii_letters

from s3_stress.utils import server_logger
from s3_stress.utils import consts

logger = server_logger.Logger(name=__name__).logger


class TimerThread(threading.Timer):
    def __init__(self, func, interval=60):
        super().__init__(interval, func)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)


def fetch_workload_stats(total_uploaded_files, total_downloaded_files, total_time_spent_in_upload,
                         total_time_spent_in_download, io_size):
    total_bytes_written = total_uploaded_files * io_size
    total_bytes_read = total_downloaded_files * io_size
    average_mb_per_upload = expr_calculator(lambda: total_bytes_written / total_uploaded_files / consts.MB1)
    average_mb_per_download = expr_calculator(lambda: total_bytes_read / total_downloaded_files / consts.MB1)
    average_write_bw = expr_calculator(lambda: total_bytes_written / consts.MB1 / (total_time_spent_in_upload / 60))
    average_read_bw = expr_calculator(lambda: total_bytes_read / consts.MB1 / (total_time_spent_in_download / 60))
    average_write_iops = expr_calculator(lambda: (total_bytes_written / io_size) / 60)
    average_read_iops = expr_calculator(lambda: (total_bytes_read / io_size) / 60)
    average_latency_write = expr_calculator(lambda: total_bytes_written / average_write_iops / 1000)
    average_latency_read = expr_calculator(lambda: total_bytes_read / average_read_iops / 1000)

    logger.info(f"S3_STATS: IO chunk size: {io_size}")
    logger.info(f"S3_STATS: Total uploaded files: {total_uploaded_files}")
    logger.info(f"S3_STATS: Total downloaded files: {total_downloaded_files}")
    logger.info(f"S3_STATS: Total time spent in upload (sec): {total_time_spent_in_upload}")
    logger.info(f"S3_STATS: Total time spent in download (sec): {total_time_spent_in_download}")
    logger.info(f"S3_STATS: Total bytes written to storage: {total_bytes_written}")
    logger.info(f"S3_STATS: Total bytes read from storage: {total_bytes_read}")
    logger.info(f"S3_STATS: Average MB per upload: "
                f"{average_mb_per_upload}")
    logger.info(f"S3_STATS: Average MB per download: "
                f"{average_mb_per_download}")
    logger.info(f"S3_STATS: Average Write Bandwidth (MB/s): "
                f"{average_write_bw}")
    logger.info(f"S3_STATS: Average Read Bandwidth (MB/s): "
                f"{average_read_bw}")
    logger.info(f"S3_STATS: Average Write Latency (ms): "
                f"{average_latency_write}")
    logger.info(f"S3_STATS: Average Read Latency (ms): "
                f"{average_latency_read}")
    logger.info(f"S3_STATS: Average Write IOPS: "
                f"{average_write_iops}")
    logger.info(f"S3_STATS: Average Read IOPS: "
                f"{average_read_iops}")

    workload_stats = {
        'io_chunk_size': io_size,
        'total_downloaded_files': total_uploaded_files,
        'total_uploaded_files': total_downloaded_files,
        'total_time_spent_in_uploads': total_time_spent_in_upload,
        'total_time_spent_in_downloads': total_time_spent_in_download,
        'total_bytes_read': total_bytes_read,
        'total_bytes_written': total_bytes_written,
        'average_mb_per_upload': average_mb_per_upload,
        'average_mb_per_download': average_mb_per_download,
        'average_write_bw': average_write_bw,
        'average_read_bw': average_read_bw,
        'average_latency_write': average_latency_write,
        'average_latency_read': average_latency_read,
        'average_write_iops': average_write_iops,
        'average_read_iops': average_read_iops
    }

    return workload_stats


def assert_raises(exc_class, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exc_class as e:
        return e
    except Exception as e:
        raise AssertionError("{} raised - when {} was expected".format(e, exc_class))
    else:
        raise AssertionError("{} not raised".format(exc_class))


def is_raised(exc_class, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exc_class:
        return True
    else:
        return False


def expr_calculator(f):
    return 0 if is_raised(ZeroDivisionError, f) else f()


def dump_stats(total_uploaded_files, total_downloaded_files, total_time_spent_in_upload,
               total_time_spent_in_download, io_size):
    workload_stats = fetch_workload_stats(total_uploaded_files, total_downloaded_files, total_time_spent_in_upload,
                                          total_time_spent_in_download, io_size)
    with open('/tmp/s3_stats.json', 'w') as f:
        json.dump(workload_stats, f)


def get_random_string(length):
    return ''.join(random.choice(ascii_letters) for _ in range(length))


def generate_valid_object_name_ascii(length):
    specials = "!-_.*'()"
    return ''.join(random.choice(ascii_letters + specials) for _ in range(length))


def generate_valid_object_name():
    return "{}".format(random.randint(1, 64000))


def generate_metadata_dict(md_dict_size=consts.KB1 * 2):
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
