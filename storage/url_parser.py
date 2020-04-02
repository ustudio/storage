from urllib.parse import ParseResult


def sanitized_uri(parsed_uri: ParseResult) -> str:
    new_netloc = parsed_uri.hostname

    if parsed_uri.port is not None:
        new_netloc = ":".join((new_netloc, str(parsed_uri.port)))

    parsed_url = parsed_uri._replace(netloc=new_netloc)

    return parsed_url.geturl()
