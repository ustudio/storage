import contextlib
import json
import os
import tempfile
from unittest import mock
from urllib.parse import parse_qsl

from tests.service_test_case import ServiceTestCase


def strip_slashes(path):
    while path.endswith(os.path.sep):
        path = path[:-1]
    while path.startswith(os.path.sep):
        path = path[1:]
    return path


class SwiftServiceTestCase(ServiceTestCase):

    def setUp(self):
        super().setUp()
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_files = []

        self.remaining_container_failures = []
        self.remaining_file_failures = []
        self.remaining_object_put_failures = []
        self.remaining_file_delete_failures = []

        self.container_contents = {}
        self.directory_contents = {}
        self.file_contents = {}

        # this is fragile, but they import and use the function directly, so we mock in-module
        self.mock_swiftclient_sleep_patch = mock.patch("swiftclient.client.sleep")
        self.mock_swiftclient_sleep = self.mock_swiftclient_sleep_patch.start()
        self.mock_swiftclient_sleep.side_effect = lambda x: None

        self.swift_service = self.add_service()

    def tearDown(self):
        super().tearDown()
        for fp in self.tmp_files:
            fp.close()
        self.tmp_dir.cleanup()
        self.mock_swiftclient_sleep_patch.stop()

    def _add_file_to_directory(self, filepath, file_content) -> None:
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        self.container_contents[filepath] = file_content
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self._add_file(filepath, file_content)

    def _add_tmp_file_to_dir(self, directory, file_content, suffix=None):
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        os.makedirs(directory, exist_ok=True)

        tmp_file = tempfile.NamedTemporaryFile(dir=directory, suffix=suffix)
        tmp_file.write(file_content)
        tmp_file.flush()

        self.tmp_files.append(tmp_file)
        return tmp_file

    def _add_file_error(self, error: str) -> None:
        self.remaining_file_failures.append(error)

    def _add_file(self, filepath, file_content) -> None:
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        self.file_contents[filepath] = file_content
        self.swift_service.add_handler(
            "GET", f"/v2.0/1234/CONTAINER{filepath}", self.swift_object_handler)

    @contextlib.contextmanager
    def _expect_file(self, filepath, file_content) -> None:
        put_path = f"/v2.0/1234/CONTAINER{filepath}"
        self.swift_service.add_handler("PUT", put_path, self.swift_object_put_handler)
        yield
        self.assertEqual(file_content, self.container_contents[filepath])
        self.swift_service.assert_requested("PUT", put_path)

    @contextlib.contextmanager
    def _expect_delete(self, filepath) -> None:
        self.container_contents[filepath] = b"UNDELETED!"
        self.swift_service.add_handler(
            "DELETE", f"/v2.0/1234/CONTAINER{filepath}", self.swift_object_delete_handler)

        yield

        self.assertNotIn(
            filepath, self.container_contents, f"File {filepath} was not deleted as expected.")

    @contextlib.contextmanager
    def _expect_directory(self, filepath) -> None:
        for root, _, files in os.walk(self.tmp_dir.name):
            dirpath = strip_slashes(root.split(self.tmp_dir.name)[1])
            for basepath in files:
                relative_path = os.path.join(dirpath, basepath)
                remote_path = "/".join([filepath, relative_path])

                self.swift_service.add_handler(
                    "PUT", f"/v2.0/1234/CONTAINER{remote_path}", self.swift_object_put_handler)
        yield

        self.assert_container_contents_equal(filepath)

    @contextlib.contextmanager
    def _expect_delete_directory(self, filepath) -> None:
        expected_delete_paths = []
        for name in self.container_contents:
            delete_path = f"/v2.0/1234/CONTAINER/{strip_slashes(name)}"
            expected_delete_paths.append(delete_path)
            self.swift_service.add_handler("DELETE", delete_path, self.swift_object_delete_handler)

        yield

        for delete_path in expected_delete_paths:
            self.swift_service.assert_requested("DELETE", delete_path)

    def swift_container_handler(self, environ, start_response):
        if len(self.remaining_container_failures) > 0:
            failure = self.remaining_container_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error"]

        parsed_args = dict(parse_qsl(environ["QUERY_STRING"]))

        if "json" == parsed_args.get("format"):
            start_response("200 OK", [("Content-Type", "application/json")])
            return [json.dumps([
                {"name": v} for v in self.container_contents.keys()
            ]).encode("utf8")]

        start_response("200 OK", [("Content-Type", "text/plain")])
        return ["\n".join(self.container_contents).encode("utf8")]

    def swift_object_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        if len(self.remaining_file_failures) > 0:
            failure = self.remaining_file_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal Server Error"]

        if path not in self.file_contents:
            start_response("404 NOT FOUND", [("Content-Type", "text/plain")])
            return [f"Object file {path} not in file contents dictionary".encode("utf8")]

        start_response("200 OK", [("Content-Type", "video/mp4")])
        return [self.file_contents[path]]

    def swift_object_put_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        if len(self.remaining_object_put_failures) > 0:
            failure = self.remaining_object_put_failures.pop(0)
            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error."]

        header = b""
        while not header.endswith(b"\r\n"):
            header += environ["wsgi.input"].read(1)

        body_size = int(header.strip())
        self.container_contents[path] = environ["wsgi.input"].read(body_size)

        start_response("201 OK", [("Content-Type", "text/plain")])
        return [b""]

    def swift_object_delete_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        if len(self.remaining_file_delete_failures) > 0:
            failure = self.remaining_file_delete_failures.pop(0)
            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error."]

        del self.container_contents[path]
        start_response("204 OK", [("Content-type", "text/plain")])
        return [b""]

    def assert_container_contents_equal(self, object_path):
        written_files = {}
        expected_files = {
            strip_slashes(f.split(object_path)[1]): v
            for f, v in self.container_contents.items()
        }

        for root, dirs, files in os.walk(self.tmp_dir.name):
            dirpath = strip_slashes(root.split(self.tmp_dir.name)[1])
            for basepath in files:
                fullpath = os.path.join(root, basepath)
                relpath = os.path.join(dirpath, basepath)
                with open(fullpath, "rb") as fp:
                    written_files[relpath] = fp.read()

        self.assertCountEqual(written_files, expected_files)
