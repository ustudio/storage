try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

install_requires = [
    "boto3 >= 1.0.1",
    "google-cloud-storage",
    "keystoneauth",
    "python-swiftclient"
]

setup(name="object_storage",
      version="0.14.3",
      description="Python library for accessing files over various file transfer protocols.",
      url="https://github.com/ustudio/storage",
      packages=["storage"],
      package_data={"storage": ["py.typed"]},
      install_requires=install_requires)
