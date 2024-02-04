import tomllib

from setuptools import find_packages, setup

with open('pyproject.toml', 'rb') as pyproject:
    project_data = tomllib.load(pyproject)
    dependencies = project_data['tool']['poetry']['dependencies']
    dev_dependencies = project_data['tool']['poetry']['dev-dependencies']


python_version = dependencies.pop('python')


setup(name='src',
      version='0.0.1',
      packages=find_packages(),
      include_package_data=True,
      install_requires=[f'{pkg} {ver}' for pkg, ver in dependencies.items()],
      extras_require={'dev': [f'{pkg} {ver}' for pkg, ver in dev_dependencies.items()]},
      python_requires=python_version,
      entry_points={'console_scripts': ['pony-gen=src.__main__:app']})
