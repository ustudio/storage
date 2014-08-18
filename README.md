storage
=======

[![build status](https://circleci.com/gh/ustudio/storage.png?circle-token=3b3e87d02777a6e2ef90bcb9651457a215b6d333)](https://circleci.com/gh/ustudio/storage)

Python library for accessing files over various file transfer protocols.

## Installation ##

Install via pip:

```sh
pip install object_storage
```

The current version is `0.3.1`.

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

#### `save_to_filename(filename)` ####

Downloads the contents of the file specified by the URI to
`get_storage` into a local file at `filename`.

#### `save_to_file(file_object)` ####

Downloads the contents of the file specified by the URI to
`get_storage` by writing into a file-like-object.

#### `delete()` ####

Deletes the file specified by the URI to `get_storage`

### Supported Protocols ###

The following protocols are supported, and can be selected by
specifying them in the scheme/protocol section of the URI:

#### file ####

A reference to a local file. This is primarily useful for runing code
in a development environment.

Example:

```
file:///home/user/awesome-file.txt
```

If the intermediate directories specified in the URI passed to
`get_storage` do not exist, the file-local storage object will attempt
to create them when using `load_from_file` or `load_from_filename`.

#### cloudfiles ####

A reference to an Object in a Container in Rackspace CloudFiles. In
this case, the `host` section of the URI is the Container name, and
the `path` is the Object. Credentials are specified in the `username`
and `password` fields

Example:

```
cloudfiles://username:apikey@container/awesome-file.txt
```

You can force CloudFiles to use the internal ServiceNet network, by
adding the query parameter `?public=false` to the URI. This saves
bandwidth if you are accessing CloudFiles from within the same
datacenter.

Because of the way CloudFiles handles "virtual folders," if the
filename specified in `get_storage` includes subfolders, they will be
created automatically if they do not exist.

Note: Currently, the storage library will always connect to the DFW
region in Rackspace; there is no way to specify a region at this
time. It is possible that the URI scheme will change when this support
is added.

#### swift ####

A reference to an Object in a Container in an **OpenStack Swift** object store.
This scheme is similar to the **cloudfiles** format above with the following
differences:

- An `auth_endpoint` must be specified in the query parameters which tells
the storage library the authetication endpoint to be used.
- A `tenant_id` must be specified in the query parameters which is used 
by the storage library when authenicating. (*typically this is something
like an account or project id.)
- A `region` must be specified in the query parameters which the storage
library uses when attempting to obtain the appropriate **object_store** client.

Example:

```
swift://username:password@container/file.txt?region=REG&auth_endpoint=http://identity.svr.com:1234/v2&tenant_id=123456
```

In addition to the required parameters mentioned above, swift will also
accept the following optional parameters:

- The `public` param may also be specified just as with **cloudfiles**.
(see the [cloudfiles](#cloudfiles) section for info on this param.)
- The `api_key` param may also be specified as a query parameter and
will be included when authenticating with the swift authentication endpoint.

### hpcloud ###

An **HP Cloud** storage type has been defined for use against the **HP Helion**
object store. The `hpcloud` scheme can be used when storing files using
**HP Helion**, which  is based on **OpenStack Swift**.

Example:

```
hpcloud://username:password@container/file?region=region-a.geo-1&tenant_id=PROJECT_ID
```

When using the `hpcloud` scheme the storage library will use a
preregistered authentication endpoint.  `region` and `tenant_id` must
be specified. The `tenant_id` is typicallys the **Project Id**, as
defined by HP.


### ftp ####

A reference to a file on an FTP server. Username and passwords are supported.

Example:

```
ftp://username:password@my-ftp-server/directory/awesome-file.txt
```

#### ftps ####

A reference to a file on an FTP server, served using the FTPS
(a.k.a. FTP_TLS) encrypted protocol.

Example:

```
ftps://username:password@my-secure-ftp-server/directory/awesome-file.txt
```

### `retry` ###

The `retry` module provides a means for client code to attempt to
transfer a file multiple times, in case of network or other
failures. Exponential backoff is used to wait between retries, and the
operation will be tried a maximum of 5 times before giving up.

No guarantees are made as to the idempotency of the operations. For
example, if your FTP server handles file-naming conflicts by writing
duplicate files to a different location, and the operation retries
because of a network failure *after* some or all of the file has been
transfered, the second attempt might be stored at a different
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
