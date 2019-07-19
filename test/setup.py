from setuptools import setup

setup(
    name='cassandra-exporter-e2e-tests',
    version='1.0',
    description='End-to-end testing tools for cassandra-exporter',
    author='Adam Zegelin',
    author_email='adam@instaclustr.com',
    packages=['cassandra-exporter-e2e-tests'],
    install_requires=['ccm', 'prometheus_client', 'cassandra-driver', 'frozendict'],
)