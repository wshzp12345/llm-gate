"""Incremental byte-bounded SSE decoder, including split UTF-8 and CR/LF."""


class SSEProtocolError(ValueError):
    def __init__(self):
        super().__init__("Invalid bounded SSE stream")


class SSEDecoder:
    def __init__(self, maximum_event_bytes):
        if type(maximum_event_bytes) is not int or maximum_event_bytes < 1:
            raise ValueError("Positive SSE event bound required")
        self._maximum = maximum_event_bytes
        self._line = bytearray()
        self._data = []
        self._event = b""
        self._size = 0
        self._pending_cr = False
        self._first_line = True

    def feed(self, chunk):
        for byte in chunk:
            if self._pending_cr:
                self._pending_cr = False
                if byte == 10:
                    self._account()
                    value = self._newline()
                    if value is not None:
                        yield value
                    continue
                value = self._newline()
                if value is not None:
                    yield value
            self._account()
            if byte == 13:
                self._pending_cr = True
            elif byte == 10:
                value = self._newline()
                if value is not None:
                    yield value
            else:
                self._line.append(byte)

    def _account(self):
        self._size += 1
        if self._size > self._maximum:
            raise SSEProtocolError()

    def _newline(self):
        line = bytes(self._line)
        self._line.clear()
        if self._first_line:
            line = line.removeprefix(b"\xef\xbb\xbf")
            self._first_line = False
        try:
            line.decode("utf-8")
        except UnicodeError:
            raise SSEProtocolError() from None
        if not line:
            value = None
            if self._data:
                if self._event not in (b"", b"message"):
                    raise SSEProtocolError()
                value = b"\n".join(self._data).decode("utf-8")
                self._data.clear()
            self._event, self._size = b"", 0
            return value
        if not line.startswith(b":"):
            name, _, value = line.partition(b":")
            if value.startswith(b" "):
                value = value[1:]
            if name == b"data":
                self._data.append(value)
            elif name == b"event":
                self._event = value
            # id/retry/unknown fields never trigger reconnect or replay.
        return None

    def finish(self):
        value = None
        if self._pending_cr:
            self._pending_cr = False
            value = self._newline()
        if self._line or self._data or self._event or self._size:
            raise SSEProtocolError()
        return (value,) if value is not None else ()
