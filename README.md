storage
=======

[![build status](https://circleci.com/gh/ustudio/storage.png?circle-token=3b3e87d02777a6e2ef90bcb9651457a215b6d333)](https://circleci.com/gh/ustudio/storage)

Python library for accessing files over various file transfer protocols.

## Installation ##

Install via pip:

```sh
pip install object_storage
```

The current version is `0.14.3`.
For Python 2.7, use the latest release from the `v0.12` branch.

## Quick Start ##

```python
from storage import get_storage

# Create a reference to a local or remote file, by URI
source_file = get_storage("file:///path/to/file.txt")

# Upload contents from a local file
source_file.load_from_filename("/path/to/new-source.txt")

# Save the contents to a local file
source_file.save_to_filename("/path/to/other-file.txt")

# Delete the remote file
source_file.delete()
```

## API ##

### `get_storage(uri)` ###

The main entry point to the storage library is the `get_storage`
function, which takes a URI to a file and returns an object which can
perform operations on that file.

### `Storage` ###

The value returned by `get_storage` is a `Storage` object, which
represents a file accessible by the scheme provided to
`get_storage`. This object has the following methods:

#### `load_from_filename(filename)` ####

Uploads the contents of the file at `filename` to the location
specified by the URI to `get_storage`.

#### `load_from_file(file_object)` ####

Uploads to the location specified by the URI to `get_storage` by
reading from the specified file-like-object.

#### `load_from_directory(directory_path)` ####

Uploads to the location specified by the URI to `get_storage` all
of the contents of the directory at directory\_path.

#### `save_to_filename(filename)` ####

Downloads the contents of the file specified by the URI to
`get_storage` into a local file at `filename`.

#### `save_to_file(file_object)` ####

Downloads the contents of the file specified by the URI to
`get_storage` by writing into a file-like-object.

#### `save_to_directory(directory_path)` ####

Downloads the contents of the directory specified by the URI to
`get_storage` into the directory at directory\_path.

#### `delete()` ####

Deletes the file specified by the URI to `get_storage`.

#### `delete_directory()` ####

Recursively deletes the directory structure specified by the URI to `get_storage()`.


#### `get_download_url(seconds=60, key=None)` ####

Returns a download URL to the object specified by the URI to `get_storage`.

For **swift** and **s3** based protocols, this will return a time-limited temporary
URL which can be used to GET the object directly from the container in the
object store. By default the URL will only be valid for 60 seconds, but a
different timeout can be specified by using the `seconds` parameter.

Note that for **swift** based protocols the container must already have a temp url key
set for the container. If it does not have a temp url key, an exception will be raised.

For local file storage, the call will return a URL formed by joining the `download_url_base`
(included in the URI that was passed to `get_storage`) with the object name. If no
`download_url_base` query param was included in the storage URI, `get_download_url`
will raise a `DownloadUrlBaseUndefinedError` exception. (*see* [**file**](#file) *below*)


#### `get_sanitized_uri()` ####

Removes the username/password, as well as all query parameters, form the URL.

### Supported Protocols ###

The following protocols are supported, and can be selected by
specifying them in the scheme/protocol section of the URI:

#### file ####

A reference to a local file. This is primarily useful for running code
in a development environment.

Example:

```

file:///home/user/awesome-file.txt[?download_url_base=<ENCODED-URL>]

```

If the intermediate directories specified in the URI passed to
`get_storage` do not exist, the file-local storage object will attempt
to create them when using `load_from_file` or `load_from_filename`.

If a `download_url_base` is included in the URI specified to `get_storage`, `get_download_url` will
return a URL that that joins the `download_url_base` with the object name.

For example, if a `download_url_base` of (`http://hostname/some/path/`) is included in the URI:

```
file:///home/user/awesome-file.txt?download_url_base=http%3A%2F%2Fhostname%2Fsome%2Fpath%2F
```

then a call to `get_download_url` will return:

```
http://hostname/some/path/awesome-file.txt
```

For local storage objects both the `seconds` and `key` parameters to `get_download_url` are ignored.


#### swift ####

A reference to an Object in a Container in an **OpenStack Swift** object store.
With this scheme, the `host` section of the URI is the Container name, and
the `path` is the Object. Credentials are specified in the `username`
and `password` fields.

In addition, the following parameters are **required** and should be passed as
query parameters in the URI:

| Query Param     | Description                                                             |
|:----------------|:------------------------------------------------------------------------|
| `auth_endpoint` | The authentication endpoint that should be used by the storage library. |
| `tenant_id`     | The tenant ID to be used during authentication. Typically an account or project Id.|
| `region`        | The region which the storage library will use when obtaining the appropriate **object_store** client.                                    |

Example:

```

swift://username:password@container/file.txt?region=REG&auth_endpoint=http://identity.svr.com:1234/v2&tenant_id=123456

```

In addition to the required parameters mentioned above, swift will also
accept the following optional parameters:

| Query Param     | Description                                                             |
|:----------------|:------------------------------------------------------------------------|
| `public`        | Whether or not to use the internal ServiceNet network. This saves bandwidth if you are accessing CloudFiles from within the same datacenter.  (default: true)           |
| `api_key`       | API key to be used during authentication.                               |
| `temp_url_key`  | Key to be used when retrieving a temp download url to the storage object from the **Swift** object store (see `get_download_url()`)|

**Note** The connection will have a default 60 second timeout on network
 operations, which can be set by changing
 `storage.storage.DEFAULT_SWIFT_TIMEOUT`, specified in seconds. The
 timeout is per data chunk, not for transfer of the entire object.


#### cloudfiles ####

A reference to an Object in a Container in Rackspace CloudFiles. This scheme is similar to
the [**swift**](#swift) scheme with the following differences:

- The `auth_endpoint` and `tenant_id` need not be specified.  These are automatically determined
by Rackspace.
- The `region` parameter is optional, and will default to `DFW` if not
specified.


Example:

```

cloudfiles://username:apikey@container/awesome-file.txt

```

Because of the way CloudFiles handles "virtual folders," if the
filename specified in `get_storage` includes subfolders, they will be
created automatically if they do not exist.

**Note**: Currently, the storage library will always connect to the DFW
region in Rackspace; there is no way to specify a region at this
time. It is possible that the URI scheme will change when this support
is added.

**Note** The connection will have a default 60 second timeout on network
 operations, which can be set by changing
 `storage.storage.DEFAULT_SWIFT_TIMEOUT`, specified in seconds. The
 timeout is per data chunk, not for transfer of the entire object.

### Amazon S3 ###

A reference to an object in an Amazon S3 bucket.  The `s3` scheme can be used when storing
files using the Amazon S3 service.

A `region` parameter is not required, but can be specified.

**Note:** Chunked transfer encoding is only used for
`save_to_filename` and `load_from_filename`. If you use `save_to_file`
or `load_from_file`, the entire contents of the file will be loaded
into memory.

Example:

```

s3://aws_access_key_id:aws_secret_access_key@bucket/path/to/file[?region=us-west-2]


```

Note that the `aws_access_key` and `aws_secret_access_key` should be URL encoded, to quote
unsafe characters, if necessary. This may be necessary as AWS sometimes includes characters
such as a `/`.

### ftp ####

A reference to a file on an FTP server. Username and passwords are supported.

Example:

```

ftp://username:password@my-ftp-server/directory/awesome-file.txt[?download_url_base=<ENCODED-URL>]

```

**Note** The FTP connection will have a default 60 second timeout on
 network operations, which can be set by changing
 `storage.storage.DEFAULT_FTP_TIMEOUT`, specified in seconds. The
 timeout is per data chunk, not for transfer of the entire object.

**Note** The FTP connection's command socket will have TCP_KEEPALIVE
  turned on by default, as configurable by
  `storage.storage.DEFAULT_FTP_KEEPALIVE_ENABLE`, and will configure
  TCP keepalive options when the platform supports them, using
  similar configuration globals.

#### ftps ####

A reference to a file on an FTP server, served using the FTPS
(a.k.a. FTP\_TLS) encrypted protocol.

Example:

```
ftps://username:password@my-secure-ftp-server/directory/awesome-file.txt[?download_url_base=<ENCODED-URL>]
```

**Note** The FTP_TLS connection will have a default timeout and TCP
 keepalive specified in the same manner as the `ftp` protocol (see
 above).

#### Google Cloud Storage ####

A reference to an object in a Google Cloud Storage bucket. The `gs` scheme can
be used when storing files using the Google Cloud Storage service.

Example:

```
gs://SERVICE-ACCOUNT-DATA@bucket/path/to/file
```

Note that the `SERVICE-ACCOUNT-DATA` should be a URL-safe base64 encoding of
the JSON key for the service account to be used when accessing the storage.

### retry ###

The `retry` module provides a means for client code to attempt to
transfer a file multiple times, in case of network or other
failures. Exponential backoff is used to wait between retries, and the
operation will be tried a maximum of 5 times before giving up.

No guarantees are made as to the idempotency of the operations. For
example, if your FTP server handles file-naming conflicts by writing
duplicate files to a different location, and the operation retries
because of a network failure *after* some or all of the file has been
transferred, the second attempt might be stored at a different
location.

In general, this is not a problem as long as the remote servers are
configured to overwrite files by default.

#### Quick Start ####

```python
from storage import get_storage
from storage.retry import attempt

# Create a reference to a local or remote file, by URI
source_file = get_storage("file:///path/to/file.txt")

# Upload contents from a local file
attempt(source_file.load_from_filename, "/path/to/new-source.txt")

# Save the contents to a local file
attempt(source_file.save_to_filename, "/path/to/other-file.txt")

# Delete the remote file
attempt(source_file.delete)
```

#### API ####

##### `attempt(function, *args, **kwargs)` #####

Call `function`, passing in `*args` and `**kwargs`. If the function
raises an exception, sleep and try again, using exponential backoff
after each retry.

If the exception raised has an attribute, `do_not_retry`, set to
`True`, then do not retry the operation. This can be used by the
function to indicate that a failure is not worth retrying
(i.e. username/password is incorrect) or the operation is not safe to
retry.

Currently, no methods in the storage library mark exceptions as
`do_not_retry`.

### url_parser ###

The `url_parser` module provides a means for client code to sanitize URIs in
such a way that is most appropriate for the way it encodes secret data.

#### API ####

##### `sanitize_resource_uri(parsed_uri)` #####

Implementation is overly restrictive -- only returning the scheme, hostname,
port and path, no query parameters.

##### `remove_user_info(parsed_uri)` #####

Implementation all credential information before the hostname (if present), and
returns the scheme, hostname, port, path, and query parameters.

### Extending ###

There are two decorators that can be used when extending the storage library.

#### `register_storage_protocol` ####

This class decorator will register a scheme and its associated class with the storage library.
For example, if a new storage class were implemented (*subclassing from* `storage.Storage`),
a scheme could be registered with the storage library using the `register_storage_protocol`.

```python

@register_storage_protocol("xstorage")
class XStorage(storage.Storage):
   ... <implementation> ...

```

This would allow the `XStorage` class to be used by making a call to `get_storage()` using the
specified scheme (`"xstorage"`)

```python

xs = storage.get_storage("xstorage://some/xstorage/path")

```

#### `register_swift_protocol` ####

This class decorator is used for registering OpenStack Swift storage classes.  It is similar to the
`register_storage_protocol` decorator but is specific to classes that are subclasses from
`storage.SwiftStorage`. It accepts two arguments. The first being the scheme it should be
registered under. The second being the authentication endpoint that should be used when
authenticating.

```python

@register_swift_protocol(scheme="ystorage",
                         auth_endpoint="http://identity.svr.com:1234/v1.0/")
class YStorage(storage.SwiftStorage):
   pass

```

This will register the swift based storage protocol under the "ystorage" scheme using the specified
authentication endpoint.

```python

ys = storage.get_storage("ystorage://user:pass@container/obj?region=REG&tenant_id=1234")

```
