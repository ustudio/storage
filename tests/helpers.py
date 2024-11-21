import io
import os
import tempfile

from typing import Any, cast, List, Optional, Union
from typing_extensions import Buffer
from mypy_extensions import TypedDict


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

    def add_file(self, contents: bytes, suffix: str = "") -> NamedIO:
        temp = cast(NamedIO, tempfile.NamedTemporaryFile(dir=self.directory.name, suffix=suffix))
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


NestedFileInfo = TypedDict("NestedFileInfo", {
    "file": NamedIO,
    "path": str,
    "name": str
})


NestedDirectoryInfo = TypedDict("NestedDirectoryInfo", {
    "path": str,
    "name": str,
    "object": TempDirectory
})


NestedDirectoryTempInfo = TypedDict("NestedDirectoryTempInfo", {
    "path": str,
    "object": TempDirectory
})


NestedDirectoryDict = TypedDict("NestedDirectoryDict", {
    "temp_directory": NestedDirectoryTempInfo,
    "nested_temp_directory": NestedDirectoryInfo,
    "temp_input_one": NestedFileInfo,
    "temp_input_two": NestedFileInfo,
    "nested_temp_input": NestedFileInfo
})


def cleanup(value: Union[TempDirectory, NamedIO, str]) -> None:
    if isinstance(value, TempDirectory):
        value.cleanup()
    else:
        raise ValueError(f"Cannot call cleanup on {type(value)}")


def cleanup_nested_directory(value: NestedDirectoryDict) -> None:
    value["temp_directory"]["object"].cleanup()


def create_temp_nested_directory_with_files(
        suffixes: List[str] = ["", "", ""]
) -> NestedDirectoryDict:
    # temp_directory/
    #   temp_input_one
    #   temp_input_two
    #   nested_temp_directory/
    #      nested_temp_input

    directory = TempDirectory()
    new_file_1 = directory.add_file(b"FOO", suffixes[0])
    new_file_2 = directory.add_file(b"BAR", suffixes[1])

    nested_directory = directory.add_dir()
    nested_file = nested_directory.add_file(b"FOOBAR", suffixes[2])

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

    def write(self, chunk: Buffer) -> int:
        raw = bytes(chunk)
        rawlen = len(raw)
        self.chunks.append(raw)
        self.index += rawlen
        return rawlen

    def seek(self, index: int, whence: int = 0) -> int:
        if whence != 0:
            raise ValueError("FileSpy can only seek absolutely.")
        self.index = index
        return self.index

    def assert_written(self, assertion: bytes) -> None:
        assert b"".join(self.chunks) == assertion

    def assert_number_of_chunks(self, n: int) -> None:
        assert n == len(self.chunks)
