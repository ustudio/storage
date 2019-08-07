import contextlib
import io
import logging
import unittest
import socket
from threading import Thread, Event
import time
from wsgiref.headers import Headers
from wsgiref.simple_server import make_server
from wsgiref.util import request_uri
from urllib.parse import urlparse

from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union


def get_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 0))

    port = s.getsockname()[1]

    s.close()
    return port


ResponseCallback = Callable[[str, Sequence[Tuple[str, str]]], None]


Handler = Callable[
    [
        Dict[str, Any],
        ResponseCallback
    ],
    Union[Sequence[bytes], Iterator[bytes]]
]


HandlerIdentifier = Tuple[str, str]


class ServiceRequest(object):
    # eventually this can contain headers, body, etc. as necessary for comparison

    def __init__(
            self, headers: Dict[str, str], method: str, path: str, body: Optional[bytes]) -> None:
        self.headers = Headers([(key.replace("_", "-"), value) for key, value in headers.items()])
        self.method = method
        self.path = path
        self.body = body

    def assert_header_equals(self, header_key: str, header_value: str) -> None:
        assert header_key in self.headers, \
            f"Expected header {header_key} not in request headers."
        actual_value = self.headers[header_key]
        assert actual_value == header_value, \
            f"Request header {header_key} unexpectedly set to " \
            f"`{actual_value}` instead of `{header_value}`."

    def assert_body_equals(self, body: bytes) -> None:
        assert body == self.body, f"Body unexpectedly equals {self.body} instead of {body}"


class Service(object):

    fetches: Dict[Tuple[str, str], List[ServiceRequest]]

    def __init__(self) -> None:
        self.port: int = get_port()
        self.handlers: Dict[HandlerIdentifier, Handler] = {}
        self.thread = None
        self.fetches = {}
        self.event = None

    def url(self, path: str) -> str:
        return f"http://localhost:{self.port}{path}"

    def add_handler(self, method: str, path: str, callback: Handler) -> None:
        identifier: HandlerIdentifier = (method, path)
        self.handlers[identifier] = callback

    def handler(self, environ, start_response):
        uri = request_uri(environ, include_query=1)
        path = urlparse(uri).path
        method = environ["REQUEST_METHOD"]
        body: Optional[bytes] = None
        if method in ("POST", "PUT"):
            content_length = int(environ.get("CONTENT_LENGTH", 0) or "0")
            if content_length > 0:
                body = environ["wsgi.input"].read(content_length)
                environ["wsgi.input"] = io.BytesIO(body)
            else:
                logging.warning(f"Unable to determine content length for request {method} {path}")

        headers = {
            key: value for key, value in environ.copy().items()
            if key.startswith("HTTP_") or key == "CONTENT_TYPE"
        }
        request = ServiceRequest(headers=headers, method=method, path=path, body=body)

        logging.info(f"Received {method} request for localhost:{self.port}{path}.")

        identifier = (method, path)
        if identifier not in self.handlers:
            logging.warning(
                f"No handler registered for {method} "
                f"localhost:{self.port}{path}")
            start_response("404 Not Found", [("Content-type", "text/plain")])
            return [f"No handler registered for {identifier}".encode("utf8")]

        environ["REQUEST_PATH"] = path
        self.fetches.setdefault(identifier, [])
        self.fetches[identifier].append(request)
        return self.handlers[identifier](environ, start_response)

    def start(self):
        if self.event is not None:
            raise Exception(f"Service already started on port {self.port}")

        self.event = Event()
        self.event.set()
        self.thread = Thread(target=lambda: self.loop(self.event))
        self.thread.start()

        logging.info(f"Starting server on port {self.port}...")

        while self.event.is_set():
            # waiting until the event is clear, e.g. the server has been
            # started and is ready for connections
            time.sleep(0.005)

        logging.info(f"Server on port {self.port} ready for requests.")

    def stop(self):
        if self.event is not None:
            self.event.set()
            self.thread.join()
        self.event = None

    def loop(self, event):
        httpd = make_server("localhost", self.port, self.handler)
        httpd.timeout = 0.01

        event.clear()
        while not event.is_set():
            httpd.handle_request()

        httpd.server_close()

    def assert_requested(
            self, method: str, path: str,
            headers: Optional[Dict[str, str]] = None) -> ServiceRequest:
        identifier = (method, path)
        assert identifier in self.fetches, f"Could not find request matching {method} {path}"
        request = self.fetches[identifier][0]
        if headers is not None:
            for expected_header, expected_value in headers.items():
                request.assert_header_equals(expected_header, expected_value)
        return request

    def get_all_requests(self, method: str, path: str) -> List[ServiceRequest]:
        identifier = (method, path)
        return self.fetches.get(identifier, [])

    def assert_not_requested(self, method: str, path: str) -> None:
        identifier = (method, path)
        assert identifier not in self.fetches, f"Unexpected request found for {method} {path}"

    def assert_requested_n_times(
            self, method: str, path: str, n: int) -> List[ServiceRequest]:
        requests = self.get_all_requests(method, path)
        assert len(requests) == n, \
            f"Expected request count for {method} {path} ({n}) did not match " \
            f"actual count: {len(requests)}"
        return requests


class ServiceTestCase(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.services: List[Service] = []

    def tearDown(self) -> None:
        super().tearDown()
        self.stop_services()

    def add_service(self) -> Service:
        service = Service()
        self.services.append(service)
        return service

    def start_services(self) -> None:
        for service in self.services:
            service.start()

    def stop_services(self) -> None:
        for service in self.services:
            service.stop()

    @contextlib.contextmanager
    def run_services(self) -> Iterator[None]:
        self.start_services()
        try:
            yield
        finally:
            self.stop_services()
