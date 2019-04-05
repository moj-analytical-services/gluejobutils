from setuptools import setup

# see https://stackoverflow.com/questions/14399534/reference-requirements-txt-for-the-install-requires-kwarg-in-setuptools-setup-py
setup(name='gluejobutils',
      version='1.0.3',
      description='Python 2.7 utils for glue jobs',
      long_description=open('README.md').read(),
      url='https://github.com/moj-analytical-services/gluejobutils',
      author='Robin Linacre',
      author_email='robinlinacre@hotmail.com',
      license='MIT',
      packages=['gluejobutils'],
      zip_safe=False,
      include_package_data=True)