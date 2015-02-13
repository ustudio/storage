from distutils.core import setup


with open("requirements.txt") as requirements_file:
    requirements = filter(lambda r_striped: r_striped,
                          map(lambda r: r.strip(), requirements_file.readlines()))

setup(name="object_storage",
      version="0.4.2",
      description="Python library for accessing files over various file transfer protocols.",
      url="https://github.com/ustudio/storage",
      packages=["storage"],
      install_requires=requirements)
