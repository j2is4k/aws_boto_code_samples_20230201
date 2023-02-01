class CaseInsensitiveDict(dict):
    """The spec in https://datatracker.ietf.org/doc/html/rfc2616#section-4.2 states:

    HTTP Header field names are case-insensitive.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._convert_keys()

    def copy(self):
        return CaseInsensitiveDict(self.items())

    @staticmethod
    def _k(key):
        return key.lower() if isinstance(key, str) else key

    def _convert_keys(self):
        for k in list(self.keys()):
            v = super().pop(k)
            self.__setitem__(k, v)

    def __getitem__(self, key):
        return super().__getitem__(self._k(key))

    def __setitem__(self, key, value):
        super().__setitem__(self._k(key), value)

    def __delitem__(self, key):
        return super().__delitem__(self._k(key))

    def __contains__(self, key):
        return super().__contains__(self._k(key))

    def pop(self, key, default=None):
        return super().pop(self._k(key), default)

    def get(self, key, *args, **kwargs):
        return super().get(self._k(key), *args, **kwargs)

    def setdefault(self, key, *args, **kwargs):
        return super().setdefault(self._k(key), *args, **kwargs)

    def update(self, E={}, **F):
        super().update(self.__class__(E))
        super().update(self.__class__(**F))


class HttpHeaders(CaseInsensitiveDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cast_headers(self)

    def cast_headers(self, headers):
        for k, v in headers.items():
            if isinstance(headers[k], str):
                if headers[k].lower() == "true":
                    headers[k] = True
                elif headers[k].lower() == "false":
                    headers[k] = False
            elif isinstance(headers[k], dict):
                self.cast_headers(headers[k])
