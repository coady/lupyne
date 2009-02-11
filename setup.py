from distutils.core import setup

setup(
    name='cherrypylucene',
    version='0.1a0',
    description='Search engine based on CherryPy and PyLucene.',
    author='Aric Coady',
    author_email='aric.coady@gmail.com',
    packages=['cherrypylucene', 'cherrypylucene.engine'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
    ],
)
