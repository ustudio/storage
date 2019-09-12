from typing import Dict


class Credentials(object):

    @classmethod
    def from_service_account_info(
        self, account_info: Dict[str, str]) -> "Credentials": ...
