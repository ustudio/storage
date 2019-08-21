# This is purposefully typed only for the subset that we use, may need to be
# expanded as new arguments, etc. are used.


from keystoneauth1.identity.v2 import Password
from typing import Optional


class Session(object):
    def __init__(
        self,
        auth: Optional[Password] = None,
        # session=None,
        # original_ip=None,
        # verify=True,
        # cert=None,
        # timeout=None,
        # user_agent=None,
        # redirect=_DEFAULT_REDIRECT_LIMIT,
        # additional_headers=None,
        # app_name=None,
        # app_version=None,
        # additional_user_agent=None,
        # discovery_cache=None,
        # split_loggers=None,
        # collect_timing=False,
        # rate_semaphore=None
        ) -> None: ...
