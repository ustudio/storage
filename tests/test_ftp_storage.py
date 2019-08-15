import contextlib
import socket
from io import BytesIO
import tempfile

from typing import Any, Callable, Collection, cast, Generator, List, Optional, Union
from unittest import mock, TestCase
from urllib.parse import quote_plus

from storage.storage import get_storage, DownloadUrlBaseUndefinedError
from storage.storage import DEFAULT_FTP_KEEPALIVE_ENABLE, DEFAULT_FTP_KEEPCNT, DEFAULT_FTP_KEEPIDLE
from storage.storage import DEFAULT_FTP_KEEPINTVL, DEFAULT_FTP_TIMEOUT

from tests.helpers import create_temp_nested_directory_with_files, NestedDirectoryDict
from tests.helpers import cleanup_nested_directory


ListingCallback = Callable[[str], None]


DirectoryListing = Collection[Union[List[str], str]]


@contextlib.contextmanager
def patch_ftp_client(
        directory_listing: Optional[DirectoryListing] = None,
        expected_port: int = 21) -> Generator[mock.Mock, None, None]:

    if directory_listing is None:
        dir_listing: DirectoryListing = []
    else:
        dir_listing = directory_listing

    def side_effect(_: Any, fn: ListingCallback) -> None:
        listing: List[str] = []
        if len(dir_listing):
            # the cast is necessary because there's not an easy available protocol
            # which handles variants (e.g. Union[List, etc.]) and supports pop(), etc.
            # (List does not handle variants properly in this case.)
            value = cast(List[Union[List[str], str]], dir_listing).pop(0)
            if isinstance(value, str):
                listing = [value]
            else:
                listing = value
        list(map(fn, listing))

    with mock.patch("ftplib.FTP", autospec=True) as mock_ftp_class:
        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrlines.side_effect = side_effect
        yield mock_ftp
        assert_connected(mock_ftp_class, mock_ftp, expected_port)


def assert_connected(
        mock_ftp_class: mock.Mock,
        mock_ftp: mock.Mock, expected_port: Optional[int] = 21) -> None:
    mock_ftp_class.assert_called_with(timeout=DEFAULT_FTP_TIMEOUT)
    mock_ftp.connect.assert_called_with("ftp.foo.com", port=expected_port)
    mock_ftp.sock.setsockopt.assert_any_call(
        socket.SOL_SOCKET, socket.SO_KEEPALIVE, DEFAULT_FTP_KEEPALIVE_ENABLE)
    mock_ftp.login.assert_called_with("user", "password")


class TestFTPStorage(TestCase):

    temp_directory: Optional[NestedDirectoryDict]

    def setUp(self) -> None:
        super().setUp()
        self.temp_directory = None

    def tearDown(self) -> None:
        super().tearDown()
        if self.temp_directory is not None:
            cleanup_nested_directory(self.temp_directory)

    @mock.patch("storage.ftp_storage.socket")
    @mock.patch("ftplib.FTP", autospec=True)
    def test_connect_sets_tcp_keepalive_options_when_supported(
            self, mock_ftp_class: mock.Mock, mock_socket: mock.Mock) -> None:
        mock_socket.SOL_SOCKET = socket.SOL_SOCKET
        mock_socket.SOL_TCP = socket.SOL_TCP
        mock_socket.SO_KEEPALIVE = socket.SO_KEEPALIVE

        mock_socket.TCP_KEEPCNT = 1
        mock_socket.TCP_KEEPIDLE = 2
        mock_socket.TCP_KEEPINTVL = 3

        mock_ftp = mock_ftp_class.return_value
        in_file = BytesIO(b"foobar")

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        mock_ftp.sock.setsockopt.assert_has_calls([
            mock.call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, DEFAULT_FTP_KEEPALIVE_ENABLE),
            mock.call(socket.SOL_TCP, mock_socket.TCP_KEEPCNT, DEFAULT_FTP_KEEPCNT),
            mock.call(socket.SOL_TCP, mock_socket.TCP_KEEPIDLE, DEFAULT_FTP_KEEPIDLE),
            mock.call(socket.SOL_TCP, mock_socket.TCP_KEEPINTVL, DEFAULT_FTP_KEEPINTVL)
        ])

    @mock.patch("storage.ftp_storage.socket")
    @mock.patch("ftplib.FTP", autospec=True)
    def test_connect_only_enables_tcp_keepalive_options_when_options_not_supported(
            self, mock_ftp_class: mock.Mock, mock_socket: mock.Mock) -> None:
        mock_socket.SOL_SOCKET = socket.SOL_SOCKET
        mock_socket.SOL_TCP = socket.SOL_TCP
        mock_socket.SO_KEEPALIVE = socket.SO_KEEPALIVE

        del mock_socket.TCP_KEEPCNT
        del mock_socket.TCP_KEEPIDLE
        del mock_socket.TCP_KEEPINTVL

        mock_ftp = mock_ftp_class.return_value
        in_file = BytesIO(b"foobar")

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        mock_ftp.sock.setsockopt.assert_called_once_with(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, DEFAULT_FTP_KEEPALIVE_ENABLE)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_save_to_filename(self, mock_ftp_class: mock.Mock) -> None:
        temp_output = tempfile.NamedTemporaryFile()

        mock_results = [b"foo", b"bar"]

        def mock_retrbinary(command: str, callback: Callable[[bytes], None]) -> str:
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_filename(temp_output.name)

        assert_connected(mock_ftp_class, mock_ftp)

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        with open(temp_output.name) as output_fp:
            self.assertEqual("foobar", output_fp.read())

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_save_to_file(self, mock_ftp_class: mock.Mock) -> None:
        out_file = BytesIO(b"")

        mock_results = [b"foo", b"bar"]

        def mock_retrbinary(command: str, callback: Callable[[bytes], None]) -> str:
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_file(out_file)

        assert_connected(mock_ftp_class, mock_ftp)

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        self.assertEqual(b"foobar", out_file.getvalue())

    @mock.patch("ftplib.FTP", autospec=True)
    def test_save_to_file_with_specific_port(self, mock_ftp_class: mock.Mock) -> None:
        out_file = BytesIO(b"")

        mock_results = [b"foo", b"bar"]

        def mock_retrbinary(command: str, callback: Callable[[bytes], None]) -> str:
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = get_storage("ftp://user:password@ftp.foo.com:12345/some/dir/file")

        storage.save_to_file(out_file)

        assert_connected(mock_ftp_class, mock_ftp, 12345)

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        self.assertEqual(b"foobar", out_file.getvalue())

    @mock.patch("os.chdir")
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists", return_value=False)
    def test_ftp_save_to_directory_creates_destination_directory_if_needed(
            self, mock_path_exists: mock.Mock, mock_makedirs: mock.Mock,
            mock_chdir: mock.Mock) -> None:

        with patch_ftp_client() as mock_ftp:
            mock_ftp.pwd.return_value = "some/dir/file"

            mock_path_exists.return_value = False

            storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

            storage.save_to_directory("/cat/pants")

        mock_ftp.cwd.assert_has_calls([
            mock.call("/some/dir/file"),
        ])

        mock_makedirs.assert_has_calls([
            mock.call("/cat/pants"),
        ])

        mock_chdir.assert_has_calls([
            mock.call("/cat/pants"),
        ])

        mock_ftp.retrbinary.assert_not_called()

    @mock.patch("builtins.open", autospec=True)
    @mock.patch("os.chdir")
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists", return_value=False)
    def test_ftp_save_to_directory_downloads_nested_files(
            self, mock_path_exists: mock.Mock, mock_makedirs: mock.Mock,
            mock_chdir: mock.Mock, mock_open: mock.Mock) -> None:

        expected_calls = [
            mock.call("/cat/pants"),
            mock.call("/cat/pants/dir1"),
            mock.call("/cat/pants/dir1/dir with spaces"),
            mock.call("/cat/pants/dir2"),
            mock.call("/cat/pants/dir2/dir4"),
        ]

        directory_listing = [
            # root
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir1",
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir2",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file1",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file2",
            ],
            # dir1
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir with spaces",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file3",
            ],
            # dir with spaces
            [
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file with spaces"
            ],
            # dir2
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir4",
            ],
        ]

        with patch_ftp_client(directory_listing) as mock_ftp:
            mock_ftp.pwd.return_value = "some/place/special"
            storage = get_storage("ftp://user:password@ftp.foo.com/some/place/special")
            storage.save_to_directory("/cat/pants")

        self.assertEqual(5, mock_ftp.cwd.call_count)
        mock_ftp.cwd.assert_has_calls([
            mock.call("/some/place/special"),
            mock.call("some/place/special/dir1"),
            mock.call("some/place/special/dir1/dir with spaces"),
            mock.call("some/place/special/dir2"),
            mock.call("some/place/special/dir2/dir4"),
        ])

        self.assertEqual(5, mock_makedirs.call_count)
        mock_makedirs.assert_has_calls(expected_calls)

        self.assertEqual(5, mock_chdir.call_count)
        mock_chdir.assert_has_calls(expected_calls)

        self.assertEqual(4, mock_open.call_count)
        mock_open.assert_has_calls([
            mock.call("/cat/pants/file1", "wb"),
            mock.call("/cat/pants/file2", "wb"),
            mock.call("/cat/pants/dir1/file3", "wb"),
            mock.call("/cat/pants/dir1/dir with spaces/file with spaces", "wb"),
        ], any_order=True)

        self.assertEqual(4, mock_ftp.retrbinary.call_count)
        mock_ftp.retrbinary.assert_has_calls([
            mock.call("RETR file1", callback=mock_open.return_value.__enter__.return_value.write),
            mock.call("RETR file2", callback=mock_open.return_value.__enter__.return_value.write),
            mock.call("RETR file3", callback=mock_open.return_value.__enter__.return_value.write),
            mock.call(
                "RETR file with spaces",
                callback=mock_open.return_value.__enter__.return_value.write),
        ])

        mock_ftp.storbinary.assert_not_called()

    @mock.patch("builtins.open", autospec=True)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_filename(self, mock_ftp_class: mock.Mock, mock_open: mock.Mock) -> None:
        mock_ftp = mock_ftp_class.return_value

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_filename("some_file")

        assert_connected(mock_ftp_class, mock_ftp)

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_open.assert_called_with("some_file", "rb")
        mock_ftp.storbinary.assert_called_with(
            "STOR file", mock_open.return_value.__enter__.return_value)

    @mock.patch("builtins.open", autospec=True)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_load_from_filename_with_specific_port(
            self, mock_ftp_class: mock.Mock, mock_open: mock.Mock) -> None:
        mock_ftp = mock_ftp_class.return_value

        storage = get_storage("ftp://user:password@ftp.foo.com:12345/some/dir/file")

        storage.load_from_filename("some_file")

        assert_connected(mock_ftp_class, mock_ftp, 12345)

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_open.assert_called_with("some_file", "rb")
        mock_ftp.storbinary.assert_called_with(
            "STOR file", mock_open.return_value.__enter__.return_value)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_file(self, mock_ftp_class: mock.Mock) -> None:
        mock_ftp = mock_ftp_class.return_value
        in_file = BytesIO(b"foobar")

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        assert_connected(mock_ftp_class, mock_ftp)

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_ftp.storbinary.assert_called_with("STOR file", in_file)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_directory_creates_directories_from_storage_URI_if_not_present(
            self, mock_ftp_class: mock.Mock) -> None:
        mock_ftp = mock_ftp_class.return_value

        mock_ftp.retrlines.return_value = []

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        # empty folder
        storage.load_from_directory(tempfile.mkdtemp())

        assert_connected(mock_ftp_class, mock_ftp)

        mock_ftp.mkd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("file")
        ])

    def test_ftp_load_from_directory_does_not_create_dirs_from_storage_URI_if_present(self) -> None:
        directory_listing = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 some",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir",
        ]
        with patch_ftp_client(directory_listing) as mock_ftp:
            storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

            # empty folder
            storage.load_from_directory(tempfile.mkdtemp())

        self.assertEqual(1, mock_ftp.mkd.call_count)
        mock_ftp.mkd.assert_has_calls([mock.call("file")])
        mock_ftp.cwd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("file")
        ])

    def test_ftp_load_from_directory_create_dirs_from_load_directory(self) -> None:
        self.temp_directory = create_temp_nested_directory_with_files()

        directory_listing = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 some",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir",
        ]

        with patch_ftp_client(directory_listing) as mock_ftp:
            mock_ftp.pwd.return_value = "pwd_return_value"

            storage = get_storage("ftp://user:password@ftp.foo.com/some/dir")

            # empty folder
            directory = self.temp_directory["temp_directory"]["path"]
            storage.load_from_directory(directory)

        mock_ftp.mkd.assert_has_calls([
            mock.call(self.temp_directory["nested_temp_directory"]["name"])
        ])

        mock_ftp.cwd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("/some/dir"),
            mock.call(self.temp_directory["nested_temp_directory"]["name"]),
            mock.call("pwd_return_value"),
            mock.call("/some/dir/" + self.temp_directory["nested_temp_directory"]["name"])
        ])

    def test_ftp_load_from_directory_does_not_create_existing_dirs_from_load_directory(
            self) -> None:
        self.temp_directory = create_temp_nested_directory_with_files()

        directory_listing = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 some",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 {0}".format(
                self.temp_directory["nested_temp_directory"]["name"]),
        ]

        with patch_ftp_client(directory_listing) as mock_ftp:
            mock_ftp.pwd.return_value = "pwd_return_value"

            storage = get_storage("ftp://user:password@ftp.foo.com/some/dir")

            # empty folder
            directory = self.temp_directory["temp_directory"]["path"]
            storage.load_from_directory(directory)

        mock_ftp.mkd.assert_not_called()

        mock_ftp.cwd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("/some/dir"),
            mock.call(self.temp_directory["nested_temp_directory"]["name"]),
            mock.call("pwd_return_value"),
            mock.call("/some/dir/" + self.temp_directory["nested_temp_directory"]["name"])
        ])

    @mock.patch("builtins.open", autospec=True)
    def test_ftp_load_from_directory_creates_files_from_local_source_directory(
            self, mock_open: mock.Mock) -> None:

        self.temp_directory = create_temp_nested_directory_with_files()
        mock_open_return = mock_open.return_value.__enter__.return_value

        directory_listing = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 {0}".format(
                self.temp_directory["nested_temp_directory"]["name"]),
        ]

        with patch_ftp_client(directory_listing) as mock_ftp:
            mock_ftp.pwd.return_value = "pwd_return_value"

            storage = get_storage("ftp://user:password@ftp.foo.com/dir")

            # empty folder
            directory = self.temp_directory["temp_directory"]["path"]
            storage.load_from_directory(directory)

        mock_ftp.storbinary.assert_has_calls([
            mock.call(
                "STOR {0}".format(self.temp_directory["temp_input_one"]["name"]), mock_open_return),
            mock.call(
                "STOR {0}".format(self.temp_directory["temp_input_two"]["name"]), mock_open_return),
            mock.call(
                "STOR {0}".format(
                    self.temp_directory["nested_temp_input"]["name"]), mock_open_return)
        ], any_order=True)

        mock_ftp.cwd.assert_has_calls([
            mock.call("dir"),
            mock.call("/dir"),
            mock.call(self.temp_directory["nested_temp_directory"]["name"]),
            mock.call("pwd_return_value"),
            mock.call("/dir/" + self.temp_directory["nested_temp_directory"]["name"])
        ])

    def test_ftp_delete(self) -> None:
        with patch_ftp_client() as mock_ftp:
            storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")
            storage.delete()

        mock_ftp.cwd.assert_called_with("some/dir")
        mock_ftp.delete.assert_called_with("file")

    def test_ftp_delete_directory(self) -> None:
        directory_listing = [
            # root
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir1",
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir2",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file1",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file2",
            ],
            # dir1
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir with spaces",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file3",
            ],
            # dir with spaces
            [
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file with spaces"
            ],
            # dir2
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir4",
            ]
        ]

        with patch_ftp_client(directory_listing) as mock_ftp:
            mock_ftp.pwd.return_value = "some/dir/file"

            storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")
            storage.delete_directory()

        mock_ftp.cwd.assert_has_calls([
            mock.call("/some/dir/file"),
            mock.call("some/dir/file/dir1"),
            mock.call("some/dir/file/dir1/dir with spaces"),
            mock.call("some/dir/file/dir2"),
            mock.call("some/dir/file/dir2/dir4")
        ])
        self.assertEqual(5, mock_ftp.cwd.call_count)

        mock_ftp.delete.assert_has_calls([
            mock.call("/some/dir/file/dir1/dir with spaces/file with spaces"),
            mock.call("/some/dir/file/dir1/file3"),
            mock.call("/some/dir/file/file2"),
            mock.call("/some/dir/file/file1")
        ], any_order=True)
        self.assertEqual(4, mock_ftp.delete.call_count)

        mock_ftp.rmd.assert_has_calls([
            mock.call("/some/dir/file/dir2/dir4"),
            mock.call("/some/dir/file/dir2"),
            mock.call("/some/dir/file/dir1/dir with spaces"),
            mock.call("/some/dir/file/dir1"),
            mock.call("/some/dir/file")
        ])
        self.assertEqual(5, mock_ftp.rmd.call_count)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_get_download_url(self, mock_ftp_class: mock.Mock) -> None:
        download_url_base = quote_plus("http://hostname/path/to/")

        ftpuri = "ftp://user:password@ftp.foo.com/some/dir/file.txt?download_url_base={0}".format(
            download_url_base)

        storage = get_storage(ftpuri)
        temp_url = storage.get_download_url()

        mock_ftp_class.assert_not_called()
        self.assertEqual(temp_url, "http://hostname/path/to/file.txt")

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_get_download_url_returns_none_with_empty_base(
            self, mock_ftp_class: mock.Mock) -> None:
        ftpuri = "ftp://user:password@ftp.foo.com/some/dir/file.txt"

        storage = get_storage(ftpuri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            storage.get_download_url()

        mock_ftp_class.assert_not_called()


class TestFTPSStorage(TestCase):
    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_ftps_scheme_connects_using_ftp_tls_class(self, mock_ftp_tls_class: mock.Mock) -> None:
        temp_output = tempfile.NamedTemporaryFile()

        mock_results = [b"foo", b"bar"]

        def mock_retrbinary(command: str, callback: Callable[[bytes], None]) -> str:
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_tls_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        def assert_tcp_keepalive_already_enabled(username: str, password: str) -> None:
            # It is important that these already be called before
            # login is called, because FTP_TLS.login replaces the
            # socket instance with an SSL-wrapped socket.
            mock_ftp.sock.setsockopt.assert_any_call(
                socket.SOL_SOCKET, socket.SO_KEEPALIVE,
                DEFAULT_FTP_KEEPALIVE_ENABLE)

        mock_ftp.login.side_effect = assert_tcp_keepalive_already_enabled

        storage = get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_filename(temp_output.name)

        mock_ftp_tls_class.assert_called_with(timeout=DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()
