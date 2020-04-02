from urllib.parse import urlparse

from unittest import TestCase

from storage import url_parser


class TestUrlParser(TestCase):

    def test_sanitized_uri_removes_username_and_password(self) -> None:
        storage_uri = urlparse("https://username:password@bucket/path/filename")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_sanitized_uri_preserves_parameters(self) -> None:
        storage_uri = urlparse("https://username:password@bucket/path/filename?other=parameter")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("https://bucket/path/filename?other=parameter", sanitized_uri)

    def test_sanitized_uri_preserves_port_number(self) -> None:
        storage_uri = urlparse("ftp://username:password@ftp.foo.com:8080/path/filename")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("ftp://ftp.foo.com:8080/path/filename", sanitized_uri)

    def test_sanitized_uri_removes_download_url_key(self) -> None:
        storage_uri = urlparse(
            "https://username:password@bucket/path/filename?download_url_key=key")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_sanitized_uri_removes_download_url_key_and_preserves_parameters(self) -> None:
        storage_uri = urlparse(
            "https://username:password@bucket/path/filename?other=parameter&download_url_key=key")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("https://bucket/path/filename?other=parameter", sanitized_uri)
