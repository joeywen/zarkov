import os

from setuptools import setup, find_packages

__version__ = 'undefined'

exec open('zarkov/version.py')

setup(name='Zarkov',
      version=__version__,
      description="Event logger and aggregator",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Rick Copeland',
      author_email='rick@geek.net',
      url='http://sf.net/p/zarkov',
      license='Apache 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
        'Ming>=0.2.2dev-20110922',
        'webob',
        'ipython>=0.11',
        'gevent',
        'pyyaml',
        'python-dateutil==1.5',
        'gevent-zeromq',
        'formencode',
        'setproctitle',
        'colander',
      ],
      scripts=[
        'scripts/zcmd',
        'scripts/zsend.py',
        ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
