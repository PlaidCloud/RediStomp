from setuptools import setup

# test_deps = [
#     'pytest',
#     'mock',
#     'minimock',
#     'pytest-cov',
#     'pytest-runner',
# ]
#
# extras = {
#     'test': test_deps
# }

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='RediStomp',
    version="1.0.0",
    author='Patrick Buxton',
    author_email='patrick.buxton@tartansolutions.com',
    packages=['redis_stomp'],
    install_requires=required,
    # tests_require=test_deps,
    setup_requires=['pytest-runner'],
    # extras_require=extras,
    long_description=long_description,
    long_description_content_type='text/markdown',
)
