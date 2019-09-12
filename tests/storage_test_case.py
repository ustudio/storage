from unittest import TestCase
from storage.storage import get_storage, InvalidStorageUri

from typing import Dict, Optional, Sequence


class StorageTestCase(TestCase):

    def _generate_storage_uri(
            self, object_path: str, parameters: Optional[Dict[str, str]] = None) -> str:
        raise NotImplementedError(f"_generate_storage_uri is not implemented on {self.__class__}")

    def assert_rejects_multiple_query_values(
            self, object_path: str, query_arg: str,
            values: Sequence[str] = ["a", "b"]) -> None:
        base_uri = self._generate_storage_uri(object_path)
        query_args = []
        list_values = list(values)
        for value in list_values:
            query_args.append(f"{query_arg}={value}")

        separator = "&" if "?" in base_uri else "?"
        uri = f"{base_uri}{separator}{'&'.join(query_args)}"

        with self.assertRaises(InvalidStorageUri):
            get_storage(uri)
