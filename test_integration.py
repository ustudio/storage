import os
import random
import subprocess
import shutil
import string
import tempfile
import time
import unittest

from storage import get_storage


class IntegrationTests(unittest.TestCase):
    dest_prefix: str
    directory: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.dest_prefix = "".join([random.choice(string.ascii_letters) for i in range(16)])
        cls.directory = tempfile.mkdtemp(prefix="source-root")
        contents = "storage-test-{}".format(time.time()).encode("utf8")

        for i in range(3):
            tempfile.mkstemp(
                prefix="source-empty",
                dir=tempfile.mkdtemp(prefix="source-emptyfiledir", dir=cls.directory))

            handle, _ = tempfile.mkstemp(
                prefix="spaces rock",
                dir=tempfile.mkdtemp(prefix="source-spacedir", dir=cls.directory))
            os.write(handle, contents)
            os.close(handle)

            handle, _ = tempfile.mkstemp(
                prefix="source-contentfile",
                dir=tempfile.mkdtemp(prefix="source-contentdir", dir=cls.directory))
            os.write(handle, contents)
            os.close(handle)

            handle, _ = tempfile.mkstemp(
                prefix="source-mimetyped-file", suffix=".png",
                dir=tempfile.mkdtemp(prefix="source-contentdir", dir=cls.directory))
            os.write(handle, contents)
            os.close(handle)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.directory)

    def assert_transport_handles_directories(self, transport: str) -> None:
        variable = "TEST_STORAGE_{}_URI".format(transport)
        uri = os.getenv(variable, None)

        if not uri:
            raise unittest.SkipTest("Skipping {} - define {} to test".format(transport, variable))

        uri += "/{}".format(self.dest_prefix)
        storage = get_storage(uri)
        print(f"Testing using: {storage.get_sanitized_uri()}")

        print("Transport:", transport)
        print("\t* Uploading")
        storage.load_from_directory(self.directory)

        target_directory = tempfile.mkdtemp(prefix="dest-root")

        print("\t* Downloading")
        storage.save_to_directory(target_directory)

        print("\t* Checking")
        try:
            subprocess.check_output(
                ["diff", "-r", self.directory, target_directory], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            print("Diff output:\n{}".format(error.output))
            raise

    def test_file_transport_can_upload_and_download_directories(self) -> None:
        self.assert_transport_handles_directories("FILE")

    def test_ftp_transport_can_upload_and_download_directories(self) -> None:
        self.assert_transport_handles_directories("FTP")

    def test_s3_transport_can_upload_and_download_directories(self) -> None:
        self.assert_transport_handles_directories("S3")

    def test_s3_transport_with_json_credentials_can_upload_and_download_directories(self) -> None:
        self.assert_transport_handles_directories("S3_JSON")

    def test_swift_transport_can_upload_and_download_directories(self) -> None:
        self.assert_transport_handles_directories("SWIFT")

    def test_gs_transport_can_upload_and_download_directories(self) -> None:
        self.assert_transport_handles_directories("GS")
