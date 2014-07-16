from distutils.core import setup

setup(
    name='gpudb',
    version='0.1',
    description='Python client for GPUdb',
    packages=['gpudb',],
    package_data={'gpudb' : ['obj_defs/*.json']},
    url='http://www.gpudb.com',
)

