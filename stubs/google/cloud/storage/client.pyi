from google.oauth2.service_account import Credentials
from google.cloud.storage.bucket import Bucket


class Client(object):

    def __init__(self, project: str, credentials: Credentials) -> None: ...

    def get_bucket(self, bucket_name: str) -> Bucket: ...
