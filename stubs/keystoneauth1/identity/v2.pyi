from typing import Any, Dict, Optional, Union


class _NotPassed(object):
    pass


_NOT_PASSED = _NotPassed()


class Password(object):

    def __init__(
        self,
        auth_url: Optional[str],
        username: Union[_NotPassed, str] = _NOT_PASSED,
        password: Optional[str] = None,
        user_id: Union[_NotPassed, str] = _NOT_PASSED,
        **kwargs: Any) -> None: ...

    def get_auth_data(
        self,
        *args: Any,
        **kwargs: Any) -> Dict[str, Any]: ...
