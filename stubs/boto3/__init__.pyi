import botocore.session
from typing import Literal, Optional, TypedDict


class _AssumeRoleResponseCredentials(TypedDict):
    AccessKeyId: str
    SecretAccessKey: str
    SessionToken: str
    # Other fields omitted


class _AssumeRoleResponse(TypedDict):
    Credentials: _AssumeRoleResponseCredentials
    # Other fields omitted


class _STSClient:
    def assume_role(
        self,
        *,
        RoleArn: str,
        RoleSessionName: str,
        ExternalId: Optional[str] = None
        # Other arguments omitted
    ) -> _AssumeRoleResponse:
        ...


def client(
    service_name: Literal["sts"],
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region_name: Optional[str] = None,
    botocore_session: Optional[botocore.session.Session] = None,
    profile_name: Optional[str] = None
) -> _STSClient:
    ...
