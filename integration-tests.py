import os
import subprocess
import tempfile
import time

import storage as storagelib

TRANSPORTS = [
    "FILE",
    "FTP",
    "S3",
    "SWIFT",
]


def test_transport(transport, uri, source_directory):
    storage = storagelib.get_storage(uri)

    print "Transport:", transport
    print "\t* Uploading"
    storage.load_from_directory(source_directory)

    target_directory = tempfile.mkdtemp()

    print "\t* Downloading"
    storage.save_to_directory(target_directory)

    with open(os.devnull, 'w') as devnull:
        print "\t* Checking"
        child = subprocess.Popen(
            ["diff", "-r", source_directory, target_directory], stderr=devnull, stdout=devnull)

        return child.wait()


if __name__ == '__main__':
    directory = tempfile.mkdtemp()
    contents = "storage-test-{}".format(time.time())

    for i in xrange(3):
        tempfile.mkstemp(dir=tempfile.mkdtemp(dir=directory))

        handle, _ = tempfile.mkstemp(prefix="spaces rock", dir=tempfile.mkdtemp(dir=directory))
        os.write(handle, contents)
        os.close(handle)

        handle, _ = tempfile.mkstemp(dir=tempfile.mkdtemp(dir=directory))
        os.write(handle, contents)
        os.close(handle)

    for transport in TRANSPORTS:
        variable = "TEST_STORAGE_{}_URI".format(transport)
        uri = os.getenv(variable, None)

        if not uri:
            print "Skipping {} - define {} to test".format(transport, variable)
            continue

        exit_code = test_transport(transport, uri, directory)
        status = "PASSED" if exit_code == 0 else "FAILED ({})".format(exit_code)

        print "{0}: {1}".format(transport, status)
