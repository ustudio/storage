from urllib.parse import ParseResult


def sanitized_uri(parsed_uri: ParseResult) -> str:
    if parsed_uri.port is not None:
        parsed_url = parsed_uri._replace(
            netloc=":".join((parsed_uri.hostname, str(parsed_uri.port))))
    else:
        parsed_url = parsed_uri._replace(netloc=parsed_uri.hostname)

    return parsed_url.geturl()
