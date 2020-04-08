from urlparse import urlparse

from unittest import TestCase

from storage import url_parser


class TestUrlParser(TestCase):

    def test_remove_user_info_removes_username_and_password(self):
        storage_uri = urlparse("https://username:password@bucket/path/filename")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_remove_user_info_preserves_parameters(self):
        storage_uri = urlparse("https://username:password@bucket/path/filename?other=parameter")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("https://bucket/path/filename?other=parameter", sanitized_uri)

    def test_remove_user_info_preserves_port_number(self):
        storage_uri = urlparse("ftp://username:password@ftp.foo.com:8080/path/filename")
        sanitized_uri = url_parser.remove_user_info(storage_uri)

        self.assertEqual("ftp://ftp.foo.com:8080/path/filename", sanitized_uri)

    def test_sanitize_resource_uri_removes_username_and_password(self):
        storage_uri = urlparse("https://username:password@bucket/path/filename")
        sanitized_uri = url_parser.sanitize_resource_uri(storage_uri)

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_sanitize_resource_uri_does_not_preserves_parameters(self):
        storage_uri = urlparse("https://username:password@bucket/path/filename?other=parameter")
        sanitized_uri = url_parser.sanitize_resource_uri(storage_uri)

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_sanitize_resource_uri_preserves_port_number(self):
        storage_uri = urlparse("ftp://username:password@ftp.foo.com:8080/path/filename")
        sanitized_uri = url_parser.sanitize_resource_uri(storage_uri)

        self.assertEqual("ftp://ftp.foo.com:8080/path/filename", sanitized_uri)
