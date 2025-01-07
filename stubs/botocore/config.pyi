from typing import Optional, Union


class Config():
    def __init__(
        self,
        region_name: Optional[str] = None,
        signature_version: Optional[str] = None,
        user_agent: Optional[str] = None,
        user_agent_extra: Optional[str] = None,
        user_agent_appid: Optional[str] = None,
        connect_timeout: Optional[Union[float, int]] = None,
        read_timeout: Optional[Union[float, int]] = None,
        parameter_validation: Optional[bool] = None,
        max_pool_connections: Optional[int] = None,
        proxies: Optional[dict[str, object]] = None,
        proxies_config: Optional[dict[str, object]] = None,
        s3: Optional[dict[str, object]] = None,
        retries: Optional[dict[str, object]] = None,
        client_cert: Optional[Union[str, tuple[str, str]]] = None,
        inject_host_prefix: Optional[bool] = None,
        use_dualstack_endpoint: Optional[bool] = None,
        use_fips_endpoint: Optional[bool] = None,
        ignore_configured_endpoint_urls: Optional[bool] = None,
        tcp_keepalive: Optional[bool] = None,
        request_min_compression_size_bytes: Optional[int] = None,
        disable_request_compression: Optional[bool] = None,
        sigv4a_signing_region_set: Optional[str] = None,
        client_context_params: Optional[dict[str, object]] = None
    ) -> None:
        ...
