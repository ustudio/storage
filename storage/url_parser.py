from urllib.parse import parse_qsl, ParseResult, urlencode


def sanitized_uri(parsed_uri: ParseResult) -> str:
    new_netloc = parsed_uri.hostname

    if parsed_uri.port is not None:
        new_netloc = ":".join((new_netloc, str(parsed_uri.port)))

    new_query = dict(parse_qsl(parsed_uri.query))

    if "download_url_key" in new_query:
        del new_query["download_url_key"]

    new_uri = ParseResult(
        parsed_uri.scheme, new_netloc, parsed_uri.path, parsed_uri.params, urlencode(new_query),
        parsed_uri.fragment)

    return new_uri.geturl()
