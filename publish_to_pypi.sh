#!/usr/bin/env bash

set -e

is_package_installed() {
    python -c "import $1";

    [ $? -eq 0 ];
}

die() {
    echo "$1. Deployment failed.";

    exit 2;
}


[ -n "$PYPI_USERNAME" ] || die '$PYPI_USERNAME is required';

[ -n "$PYPI_PASSWORD" ] || die '$PYPI_PASSWORD is required';

if ! is_package_installed twine; then
    echo 'Installing twine';
    pip install twine || die 'Twine failed to install';
fi

python setup.py sdist || die 'setup.py sdist failed'

twine upload --username $PYPI_USERNAME --password $PYPI_PASSWORD dist/* ||  die 'Twine upload failed';
