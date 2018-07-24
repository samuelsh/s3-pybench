import gevent.monkey
gevent.monkey.patch_all()

try:
    import StringIO
except ImportError:
    # python3
    from io import StringIO, BytesIO

from .base import RequestsBase
import pycurl

__all__ = ("PyCurlClient",)

CONTINUE_HEADER = "HTTP/1.1 100 (Continue)"


def performer(func):
    """
    Decorator to simplify work with PyCURL
    """

    def wrap(cls, *args, **kwargs):
        client, buf = func(cls, *args, **kwargs)

        return PyCurlClient.build_response(
            *PyCurlClient.perform(client, buf))

    return wrap


class PyCurlClient(RequestsBase):
    """
    PyCURL-based HTTP client
    """
    DEBUG = False
    IPRESOLVE = pycurl.IPRESOLVE_V4

    encoding = "utf-8"

    @classmethod
    def client(cls, url):
        client = pycurl.Curl()
        buf = BytesIO()

        if cls.DEBUG:
            client.setopt(pycurl.VERBOSE, True)

        # TODO: do not attempt to connect via IPv6
        # XXX: NEED TO BE CONFIGURABLE!!!
        #client.setopt(pycurl.IPRESOLVE, cls.IPRESOLVE)
        client.setopt(pycurl.URL, url)
        #client.setopt(pycurl.HEADER, 1)
        #client.setopt(pycurl.NOSIGNAL, 1)
        client.setopt(pycurl.WRITEDATA, buf)

        return client, buf

    @classmethod
    def perform(cls, client, buf):
        client.perform()
        client.close()

        return cls.parse_response(buf)

    @classmethod
    def parse_response(cls, buf):
        #response = buf.getvalue().decode('utf-8')

        #if CONTINUE_HEADER in response:
        #    response = response.split("\r\n\r\n", 1)[-1]

#        headers, body = response.split("\r\n\r\n", 1)
#        print("DEBUG: {}".format(headers))
#        status, heads = headers.split("\r\n", 1)
#
#        # NB: mimetools.Message too slow
#        headers = dict([map(str.strip, h.split(":", 1))
#                        for h in heads.split("\r\n") if h])

#       proto, status, message = status.split(" ", 2)
        #return int(status), message, headers, body
        return int(200), "aaa", "vvvv", "cccc"

    @classmethod
    @performer
    def get(cls, url):
        return cls.client(url)

    @classmethod
    @performer
    def post(cls, url, data=None):
        client, buf = cls.client(url)

        client.setopt(pycurl.POST, True)
        data = data or ""
        client.setopt(pycurl.POSTFIELDS, data.encode(cls.encoding))

        return client, buf

    @classmethod
    @performer
    def delete(cls, url, data=None):
        client, buf = cls.client(url)

        client.setopt(pycurl.CUSTOMREQUEST, 'delete')

        return client, buf

    @classmethod
    @performer
    def put(cls, url, data=None):
        data = data or ""

        content = BytesIO(data)
        client, buf = cls.client(url)

        client.setopt(pycurl.PUT, True)
        client.setopt(pycurl.UPLOAD, True)
        client.setopt(pycurl.READDATA, content)
        client.setopt(pycurl.INFILESIZE, len(data))

        return client, buf
