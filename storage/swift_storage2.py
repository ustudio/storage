from urllib.parse import parse_qsl

from keystoneauth1 import session
from keystoneauth1.identity import v2
import swiftclient
from typing import BinaryIO

from .storage import DEFAULT_SWIFT_TIMEOUT, register_storage_protocol, Storage


@register_storage_protocol("swift2")
class SwiftStorage2(Storage):
    def save_to_file(self, out_file: BinaryIO) -> None:
        query = dict(parse_qsl(self._parsed_storage_uri.query))
        auth_endpoint = query["auth_endpoint"]
        tenant_name = query["tenant_id"]
        region_name = query["region"]

        os_options = {
            "tenant_id": tenant_name,
            "region_name": region_name
        }

        auth, _ = self._parsed_storage_uri.netloc.split("@")
        user, key = auth.split(":", 1)

        auth = v2.Password(
            auth_url=auth_endpoint, username=user, password=key, tenant_name=tenant_name)

        keystone_session = session.Session(auth=auth)

        connection = swiftclient.client.Connection(
            session=keystone_session, os_options=os_options, timeout=DEFAULT_SWIFT_TIMEOUT)


        _, container = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]

        resp_headers, object_content = connection.get_object(container, object_name)

        out_file.write(object_content)
