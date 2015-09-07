from setuptools import setup
import lupyne

setup(
    name='lupyne',
    version=lupyne.__version__,
    description='Pythonic search engine based on PyLucene, including a standalone server based on CherryPy.',
    long_description=open('README.rst').read(),
    author='Aric Coady',
    author_email='aric.coady@gmail.com',
    url='https://bitbucket.org/coady/lupyne',
    license='Apache Software License',
    packages=['lupyne', 'lupyne.engine'],
    extras_require={'server': ['cherrypy']},
    tests_require=['pytest-cov'],
    classifiers=[
        'Development Status :: 6 - Mature',
        'Framework :: CherryPy',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP :: HTTP Servers',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Text Processing :: Indexing',
    ],
)
