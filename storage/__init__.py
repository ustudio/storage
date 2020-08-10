from storage.storage import get_storage, register_storage_protocol, NotFoundError  # noqa: F401
from storage import local_storage  # noqa: F401
from storage import swift_storage  # noqa: F401
from storage import cloudfiles_storage  # noqa: F401
from storage import ftp_storage  # noqa: F401
from storage import s3_storage  # noqa: F401
from storage import google_storage  # noqa: F401
from storage.swift_storage import register_swift_protocol  # noqa: F401
