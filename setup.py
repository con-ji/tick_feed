from setuptools import setup

setup(
   name='tick_feed',
   version='1.0',
   description='Historical and live tick feed using Tardis',
   author='Jason Ji',
   author_email='jason-ji@berkeley.edu',
   packages=['tick_feed'],
   install_requires=['ciso8601, pandas, tardis-client'],
)
