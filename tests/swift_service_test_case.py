import contextlib
import json
import os
import tempfile
from unittest import mock
from urllib.parse import parse_qsl

from tests.helpers import NamedIO
from tests.service_test_case import ServiceTestCase

from typing import Any, cast, Dict, Generator, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from tests.service_test_case import Environ
    from wsgiref.types import StartResponse  # type: ignore[import-not-found]


def strip_slashes(path: str) -> str:
    while path.endswith(os.path.sep):
        path = path[:-1]
    while path.startswith(os.path.sep):
        path = path[1:]
    return path


class SwiftServiceTestCase(ServiceTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_files: List[NamedIO] = []

        self.remaining_container_failures: List[str] = []
        self.remaining_file_failures: List[str] = []
        self.remaining_object_put_failures: List[str] = []
        self.remaining_file_delete_failures: List[str] = []

        self.container_contents: Union[Dict[str, bytes], Any] = {}
        self.directory_contents: Dict[str, bytes] = {}
        self.object_contents: Dict[str, bytes] = {}

        # this is fragile, but they import and use the function directly, so we mock in-module
        self.mock_swiftclient_sleep_patch = mock.patch("swiftclient.client.sleep")
        self.mock_swiftclient_sleep = self.mock_swiftclient_sleep_patch.start()
        self.mock_swiftclient_sleep.side_effect = lambda x: None

        self.swift_service = self.add_service()

    def tearDown(self) -> None:
        super().tearDown()
        for fp in self.tmp_files:
            fp.close()
        self.tmp_dir.cleanup()
        self.mock_swiftclient_sleep_patch.stop()

    def _add_file_to_directory(self, filepath: str, file_content: bytes) -> None:
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        self.container_contents[filepath] = file_content
        container_path = "/v2.0/1234/CONTAINER"
        self.swift_service.add_handler("GET", container_path, self.swift_container_handler)
        self.add_container_object(container_path, filepath, file_content)

    def _add_tmp_file_to_dir(
            self, directory: str,
            file_content: bytes,
            suffix: Optional[str] = None) -> NamedIO:
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        os.makedirs(directory, exist_ok=True)

        tmp_file = cast(
            NamedIO, tempfile.NamedTemporaryFile(dir=directory, suffix=suffix))
        tmp_file.write(file_content)
        tmp_file.flush()

        self.tmp_files.append(tmp_file)
        return tmp_file

    def add_file_error(self, error: str) -> None:
        self.remaining_file_failures.append(error)

    def add_container_object(
            self, container_path: str, object_path: str, content: bytes) -> None:
        if type(content) is not bytes:
            raise Exception("Object file contents numst be bytes")

        self.object_contents[object_path] = content

        get_path = f"{container_path}{object_path}"
        self.swift_service.add_handler("GET", get_path, self.object_handler)

    @contextlib.contextmanager
    def expect_put_object(
            self,
            container_path: str,
            object_path: str,
            content: bytes) -> Generator[None, None, None]:
        put_path = f"{container_path}{object_path}"
        self.swift_service.add_handler("PUT", put_path, self.object_put_handler)
        yield
        self.assertEqual(content, self.container_contents[object_path])
        self.swift_service.assert_requested("PUT", put_path)

    @contextlib.contextmanager
    def expect_delete_object(
            self, container_path: str, object_path: str) -> Generator[None, None, None]:
        self.container_contents[object_path] = b"UNDELETED!"
        delete_path = f"{container_path}{object_path}"
        self.swift_service.add_handler("DELETE", delete_path, self.object_delete_handler)
        yield
        self.assertNotIn(
            object_path, self.container_contents,
            f"File {object_path} was not deleted as expected.")

    @contextlib.contextmanager
    def expect_directory(self, filepath: str) -> Generator[None, None, None]:
        for root, _, files in os.walk(self.tmp_dir.name):
            dirpath = strip_slashes(root.split(self.tmp_dir.name)[1])
            for basepath in files:
                relative_path = os.path.join(dirpath, basepath)
                remote_path = "/".join([filepath, relative_path])

                put_path = f"/v2.0/1234/CONTAINER{remote_path}"
                self.swift_service.add_handler("PUT", put_path, self.object_put_handler)
        yield

        self.assert_container_contents_equal(filepath)

    @contextlib.contextmanager
    def expect_delete_directory(self, filepath: str) -> Generator[None, None, None]:
        expected_delete_paths = []
        for name in self.container_contents:
            delete_path = f"/v2.0/1234/CONTAINER/{strip_slashes(name)}"
            expected_delete_paths.append(delete_path)
            self.swift_service.add_handler("DELETE", delete_path, self.object_delete_handler)

        yield

        for delete_path in expected_delete_paths:
            self.swift_service.assert_requested("DELETE", delete_path)

    def object_handler(self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        if len(self.remaining_file_failures) > 0:
            failure = self.remaining_file_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal Server Error"]

        if path not in self.object_contents:
            start_response("404 NOT FOUND", [("Content-Type", "text/plain")])
            return [f"Object file {path} not in file contents dictionary".encode("utf8")]

        start_response("200 OK", [("Content-type", "video/mp4")])
        return [self.object_contents[path]]

    def object_put_handler(
            self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        contents = b""
        while True:
            header = b""
            while not header.endswith(b"\r\n"):
                header += environ["wsgi.input"].read(1)

            body_size = int(header.strip())
            contents += environ["wsgi.input"].read(body_size)
            environ["wsgi.input"].read(2)  # read trailing "\r\n"

            if body_size == 0:
                break

        self.container_contents[path] = contents

        if len(self.remaining_object_put_failures) > 0:
            failure = self.remaining_object_put_failures.pop(0)
            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error"]

        start_response("201 OK", [("Content-type", "text/plain")])
        return [b""]

    def object_delete_handler(
            self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        if len(self.remaining_file_delete_failures) > 0:
            failure = self.remaining_file_delete_failures.pop(0)
            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error."]

        del self.container_contents[path]
        start_response("204 OK", [("Content-type", "text-plain")])
        return [b""]

    def swift_container_handler(
            self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        if len(self.remaining_container_failures) > 0:
            failure = self.remaining_container_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error"]

        parsed_args = dict(parse_qsl(environ["QUERY_STRING"]))

        if "json" == parsed_args.get("format"):
            start_response("200 OK", [("Content-Type", "application/json")])
            if len(self.container_contents) == 0:
                return [json.dumps([]).encode("utf8")]

            return [json.dumps([
                {"name": v} for v in self.container_contents.keys()
            ]).encode("utf8")]

        start_response("200 OK", [("Content-Type", "text/plain")])
        return ["\n".join(self.container_contents).encode("utf8")]

    def assert_container_contents_equal(self, object_path: str) -> None:
        written_files = {}
        expected_files = {
            strip_slashes(f.split(object_path)[1]): v
            for f, v in self.container_contents.items() if not f.endswith("/")
        }

        for root, dirs, files in os.walk(self.tmp_dir.name):
            dirpath = strip_slashes(root.split(self.tmp_dir.name)[1])
            for basepath in files:
                fullpath = os.path.join(root, basepath)
                relpath = os.path.join(dirpath, basepath)
                with open(fullpath, "rb") as fp:
                    written_files[relpath] = fp.read()

        self.assertCountEqual(written_files, expected_files)
