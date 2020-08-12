import gevent.monkey
gevent.monkey.patch_all()
from timeit import default_timer as timer


def upload_profiler(f):
    """
    Profiler decorator to measure duration of S3 PUT method
    """

    def wrapper(session, url, **kwargs):
        total_time_spent_in_upload = kwargs['counter']
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
        total_time_spent_in_download = kwargs['counter']
        start = timer()
        res = f(session, url, **kwargs)
        end = timer()
        with total_time_spent_in_download.get_lock():
            total_time_spent_in_download.value += (end - start)
        return res

    return wrapper


def delete_profiler(f):
    """
    Profiler decorator to measure duration of S3 DELETE method
    """

    def wrapper(session, url, **kwargs):
        total_time_spent_in_deletion = kwargs['counter']
        start = timer()
        res = f(session, url, **kwargs)
        end = timer()
        with total_time_spent_in_deletion.get_lock():
            total_time_spent_in_deletion.value += (end - start)
        return res

    return wrapper
