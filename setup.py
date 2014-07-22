from distutils.core import setup


with open("requirements.txt") as requirements_file:
    requirements = filter(lambda r_striped: r_striped,
                          map(lambda r: r.strip(), requirements_file.readlines()))

setup(name="storage",
      version="0.1",
      description="Python library for storing/retrieving files via different protocols",
      url="https://github.com/ustudio/storage",
      packages=["storage"],
      install_requires=requirements)
