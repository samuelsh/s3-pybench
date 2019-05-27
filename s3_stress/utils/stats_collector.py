import gevent.monkey

gevent.monkey.patch_all()

from s3_stress.utils import helpers, consts
import multiprocessing


class StatsCollector:
    def __init__(self, io_size):
        self.total_uploaded_files = multiprocessing.Value('L', 0)
        self.total_time_spent_in_upload = multiprocessing.Value('d', 0)

        self.total_downloaded_files = multiprocessing.Value('L', 0)
        self.total_time_spent_in_download = multiprocessing.Value('d', 0)

        self.total_deleted_files = multiprocessing.Value('L', 0)
        self.total_time_spent_in_deletion = multiprocessing.Value('d', 0)

        self.io_size = multiprocessing.Value('I', 0)

        try:
            self.io_size.value = consts.DATA_CHUNKS[io_size]
        except KeyError:
            # if chunk size is MIXED, io_size will be average of all possible data chunks
            self.io_size.value = sum([val for val in consts.DATA_CHUNKS.values()]) // len(consts.DATA_CHUNKS)

    def dump_stats(self):
        helpers.dump_stats(self.total_uploaded_files.value, self.total_downloaded_files.value,
                           self.total_time_spent_in_upload.value, self.total_time_spent_in_download.value,
                           self.io_size.value)
