# This is purposefully typed only for the subset that we use, may need to be
# expanded as new arguments, etc. are used.


from keystoneauth1.identity.v2 import Password
from typing import Optional


class Session(object):
    def __init__(self, auth: Optional[Password] = None) -> None: ...
