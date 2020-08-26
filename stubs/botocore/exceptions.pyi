from typing import Any, Dict

class ClientError(BaseException):
    def __init__(self, error_response: Dict[Any, Any], operation_name: object) -> None:
        self.response = error_response
        self.operation_name = operation_name
