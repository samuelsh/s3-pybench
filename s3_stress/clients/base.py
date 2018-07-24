__all__ = ("RequestsBase",)


class RequestsBase(object):
    """
    Base class to implement HTTP client
    """

    @classmethod
    def build_response(cls, status, message, headers, body):
        # NB: py3k
        #if str(type(body)) == "<class 'bytes'>":
        #    body = body.decode("utf-8")

        d = {
            "text": body,
            "headers": headers,
            "message": message,
            "status_code": status}

        return type('ArangoHttpResponse', (object,), d)

    @classmethod
    def get(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def post(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def put(cls, *args, **kwargs):
        raise NotImplementedError

    def delete(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def multipart(cls, requests):
        """
        Method to collecto multiple requests and
        send it as a batch using **HttpBatch API**.
        """
        pass
