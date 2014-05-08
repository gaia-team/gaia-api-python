from distutils.core import setup

setup(
    name='gaia',
    version='0.1',
    description='Python client for Gaia Db',
    packages=['gaia',],
    package_data={'gaia' : ['obj_defs/*.json']},
    url='http://gaia.gisfederal.com',
)

