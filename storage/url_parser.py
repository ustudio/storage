from urlparse import parse_qsl, ParseResult
from urllib import urlencode


def _new_uri(parsed_uri, new_netloc, new_query):
    return ParseResult(
        parsed_uri.scheme, new_netloc, parsed_uri.path, parsed_uri.params, urlencode(new_query),
        parsed_uri.fragment)


def remove_user_info(parsed_uri):
    new_netloc = parsed_uri.hostname

    if parsed_uri.port is not None:
        new_netloc = ":".join((new_netloc, str(parsed_uri.port)))

    new_uri = _new_uri(parsed_uri, new_netloc, dict(parse_qsl(parsed_uri.query)))

    return new_uri.geturl()


def sanitize_resource_uri(parsed_uri):
    new_netloc = parsed_uri.hostname

    if parsed_uri.port is not None:
        new_netloc = ":".join((new_netloc, str(parsed_uri.port)))

    new_uri = _new_uri(parsed_uri, new_netloc, {})

    return new_uri.geturl()
