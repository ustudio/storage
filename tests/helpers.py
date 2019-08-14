import io
import os
import tempfile

from typing import Any, cast, Dict, List, Optional, Union


class NamedIO(io.BufferedReader):

    name: str


class TempDirectory(object):

    def __init__(self, parent: Optional[str] = None) -> None:
        self.directory = tempfile.TemporaryDirectory(dir=parent)
        self.subdirectories: List["TempDirectory"] = []
        self.files: List[NamedIO] = []

    @property
    def name(self) -> str:
        return self.directory.name

    def add_file(self, contents: bytes) -> NamedIO:
        temp = cast(NamedIO, tempfile.NamedTemporaryFile(dir=self.directory.name))
        temp.write(contents)
        temp.flush()
        temp.seek(0)
        self.files.append(temp)
        return temp

    def add_dir(self) -> "TempDirectory":
        temp = TempDirectory(parent=self.directory.name)
        self.subdirectories.append(temp)
        return temp

    def cleanup(self) -> None:
        for subdir in self.subdirectories:
            subdir.cleanup()
        for temp in self.files:
            temp.close()
        self.directory.cleanup()

    def __enter__(self) -> "TempDirectory":
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self.cleanup()


NestedDirectoryDict = Dict[str, Dict[str, Union[TempDirectory, NamedIO, str]]]


def create_temp_nested_directory_with_files() -> NestedDirectoryDict:
    # temp_directory/
    #   temp_input_one
    #   temp_input_two
    #   nested_temp_directory/
    #      nested_temp_input

    directory = TempDirectory()
    new_file_1 = directory.add_file(b"FOO")
    new_file_2 = directory.add_file(b"BAR")

    nested_directory = directory.add_dir()
    nested_file = nested_directory.add_file(b"FOOBAR")

    return {
        "temp_directory": {
            "path": directory.name,
            "object": directory
        },
        "nested_temp_directory": {
            "path": nested_directory.name,
            "name": os.path.basename(nested_directory.name),
            "object": nested_directory
        },
        "temp_input_one": {
            "file": new_file_1,
            "path": new_file_1.name,
            "name": os.path.basename(new_file_1.name)
        },
        "temp_input_two": {
            "file": new_file_2,
            "path": new_file_2.name,
            "name": os.path.basename(new_file_2.name)
        },
        "nested_temp_input": {
            "file": nested_file,
            "path": nested_file.name,
            "name": os.path.basename(nested_file.name)
        }
    }


class FileSpy(io.BytesIO):

    def __init__(self) -> None:
        self.chunks: List[bytes] = []
        self.index = 0
        self.name = ""

    def write(self, chunk: bytes) -> int:
        self.chunks.append(chunk)
        self.index += len(chunk)
        return len(chunk)

    def seek(self, index: int, whence: int = 0) -> int:
        if whence != 0:
            raise ValueError("FileSpy can only seek absolutely.")
        self.index = index
        return self.index

    def assert_written(self, assertion: bytes) -> None:
        assert b"".join(self.chunks) == assertion

    def assert_number_of_chunks(self, n: int) -> None:
        assert n == len(self.chunks)
