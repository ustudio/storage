from urllib.parse import parse_qsl, ParseResult, urlencode


def _new_uri(parsed_uri: ParseResult, new_netloc: str, new_query: str) -> ParseResult:
    return ParseResult(
        parsed_uri.scheme, new_netloc, parsed_uri.path, parsed_uri.params, urlencode(new_query),
        parsed_uri.fragment)


def remove_user_info(parsed_uri: ParseResult) -> str:
    new_netloc = parsed_uri.hostname

    if parsed_uri.port is not None:
        new_netloc = ":".join((new_netloc, str(parsed_uri.port)))

    new_query = dict(parse_qsl(parsed_uri.query))

    if "download_url_key" in new_query:
        del new_query["download_url_key"]

    new_uri = _new_uri(parsed_uri, new_netloc, new_query)

    return new_uri.geturl()


def sanitize_filepath(parsed_uri: ParseResult) -> str:
    new_netloc = parsed_uri.hostname

    if parsed_uri.port is not None:
        new_netloc = ":".join((new_netloc, str(parsed_uri.port)))

    new_uri = _new_uri(parsed_uri, new_netloc, {})

    return new_uri.geturl()
