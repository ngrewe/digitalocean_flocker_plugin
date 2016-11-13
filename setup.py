from setuptools import setup, find_packages

setup(
    name='digitalocean_flocker_plugin',
    version='0.1',
    description='DigitalOcean Backend Plugin for ClusterHQ/Flocker',
    author='Niels Grewe',
    author_email='niels.grewe@halbordnung.de',
    license='Apache 2.0',
    install_requires=['python-digitalocean', 'six'],
    keywords='backend, plugin, flocker, docker, python',
    packages=find_packages(exclude=['test*']),
)
