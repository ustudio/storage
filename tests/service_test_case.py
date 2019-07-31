import contextlib
import logging
import unittest
import socket
from threading import Thread, Event
import time
from wsgiref.simple_server import make_server
from wsgiref.util import request_uri
from urllib.parse import urlparse

from typing import Any, Callable, Dict, Iterator, List, Sequence, Tuple, Union


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


class Service(object):

    def __init__(self) -> None:
        self.port: int = get_port()
        self.handlers: Dict[HandlerIdentifier, Handler] = {}
        self.thread = None
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

        logging.info(f"Received {method} request for localhost:{self.port}{path}.")

        identifier = (method, path)
        if identifier not in self.handlers:
            logging.warning(
                f"No handler registered for {method} "
                f"localhost:{self.port}{path}")
            start_response("404 Not Found", [("Content-type", "text/plain")])
            return [f"No handler registered for {identifier}".encode("utf8")]

        environ["REQUEST_PATH"] = path
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
