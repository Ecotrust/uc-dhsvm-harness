import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='dhsvm_harness',
    version='0.0.1',
    packages=['dhsvm_harness'],
    include_package_data=True,
    license='Unlicense',
    description='Snow2Flow Harness for DHSVM On-The-Fly runs',
    long_description=README,
    url='http://www.ecotrust.org',
    author='Ecotrust',
    author_email='ksdev@ecotrust.org',
    classifiers=[
        'Environment :: Web Development',
        'Framework :: Django',
        'Framework :: Django :: 2.2',
        'Intended Audience :: Developers',
        'License :: unlicense',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Internet :: WWW/HTTP',
    ],
)
