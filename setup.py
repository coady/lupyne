import os
from distutils.core import setup

packages = []
for dirpath, dirnames, filenames in os.walk('lupyne'):
    dirnames[:] = [dirname for dirname in dirnames if not dirname.startswith('.')]
    packages.append(dirpath.replace(os.sep, '.'))

setup(
    name='lupyne',
    version='0.2+',
    description='A pythonic search engine, based on PyLucene and CherryPy.',
    author='Aric Coady',
    author_email='aric.coady@gmail.com',
    url='http://code.google.com/p/lupyne/',
    packages=packages,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
    ],
)
