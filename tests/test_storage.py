import mock
import tempfile
from unittest import TestCase
from storage.storage import get_storage


class TestLocalStorage(TestCase):

    def test_local_storage_save_to_filename(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()

        storage = get_storage("file://%s" % (temp_input.name))
        storage.save_to_filename(temp_output.name)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    def test_local_storage_load_from_filename(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()
        storage = get_storage("file://%s" % (temp_output.name))
        storage.load_from_filename(temp_input.name)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())


class TestRackspaceStorage(TestCase):

    @mock.patch("pyrax.set_credentials")
    @mock.patch("pyrax.cloudfiles")
    def test_rackspace_save_to_filename(self, mock_cloudfiles, mock_set_credentials):
        temp_output = tempfile.NamedTemporaryFile()
        mock_cloudfiles.fetch_object.return_value = ["FOOBAR", ]

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.save_to_filename(temp_output.name)

        mock_set_credentials.assert_called_with("user", "key")
        mock_cloudfiles.fetch_object.assert_called_with("container", "file", chunk_size=4096)

        with open(temp_output.name) as output_fp:
            self.assertEqual("FOOBAR", output_fp.read())

    @mock.patch("pyrax.set_credentials")
    @mock.patch("pyrax.cloudfiles")
    def test_rackspace_load_from_filename(self, mock_cloudfiles, mock_set_credentials):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.load_from_filename(temp_input.name)

        mock_set_credentials.assert_called_with("user", "key")
        mock_cloudfiles.upload_file.assert_called_with("container", temp_input.name, "file")
