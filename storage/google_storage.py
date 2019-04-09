import base64
import datetime
import json

import google.cloud.storage
import google.oauth2.service_account

from .storage import Storage, register_storage_protocol


@register_storage_protocol("gs")
class GoogleStorage(Storage):
    def _get_blob(self):
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            json.loads(base64.urlsafe_b64decode(self._parsed_storage_uri.username)))
        client = google.cloud.storage.client.Client(credentials=credentials)
        bucket = client.get_bucket(self._parsed_storage_uri.hostname)
        return bucket.blob(self._parsed_storage_uri.path[1:])

    def save_to_filename(self, file_path):
        blob = self._get_blob()
        blob.download_to_filename(file_path)

    def save_to_file(self, out_file):
        blob = self._get_blob()
        blob.download_to_file(out_file)

    def load_from_filename(self, file_path):
        blob = self._get_blob()
        blob.upload_from_filename(file_path)

    def load_from_file(self, in_file):
        blob = self._get_blob()
        blob.upload_from_file(in_file)

    def delete(self):
        blob = self._get_blob()
        blob.delete()

    def get_download_url(self, seconds=60, key=None):
        blob = self._get_blob()
        return blob.generate_signed_url(datetime.timedelta(seconds=seconds))
