from __future__ import print_function

import os
import subprocess
import shutil
import tempfile
import time
import unittest

from storage import get_storage


class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.mkdtemp()
        contents = "storage-test-{}".format(time.time()).encode("utf8")

        for i in range(3):
            tempfile.mkstemp(dir=tempfile.mkdtemp(dir=cls.directory))

            handle, _ = tempfile.mkstemp(
                prefix="spaces rock", dir=tempfile.mkdtemp(dir=cls.directory))
            os.write(handle, contents)
            os.close(handle)

            handle, _ = tempfile.mkstemp(dir=tempfile.mkdtemp(dir=cls.directory))
            os.write(handle, contents)
            os.close(handle)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.directory)

    def assert_transport_handles_directories(self, transport):
        variable = "TEST_STORAGE_{}_URI".format(transport)
        uri = os.getenv(variable, None)

        if not uri:
            raise unittest.SkipTest("Skipping {} - define {} to test".format(transport, variable))

        print("Testing using:", uri)
        storage = get_storage(uri)

        print("Transport:", transport)
        print("\t* Uploading")
        storage.load_from_directory(self.directory)

        target_directory = tempfile.mkdtemp()

        print("\t* Downloading")
        storage.save_to_directory(target_directory)

        with open(os.devnull, 'w') as devnull:
            print("\t* Checking")
            child = subprocess.Popen(
                ["diff", "-r", self.directory, target_directory], stderr=devnull, stdout=devnull)

            self.assertEqual(0, child.wait())

    def test_file_transport(self):
        self.assert_transport_handles_directories("FILE")

    def test_ftp_transport(self):
        self.assert_transport_handles_directories("FTP")

    def test_s3_transport(self):
        self.assert_transport_handles_directories("S3")

    def test_swift_transport(self):
        self.assert_transport_handles_directories("SWIFT")
