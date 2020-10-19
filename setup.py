# setup.py
from setuptools import setup, find_packages

author_name = 'Marcos Paulo de Souza Vaz'
author_email = 'marcos.vaz@getrak.com.br'

setup(
    name='cqldump',
    version='0.0.1',
    description='CQL Dump',
    long_description='CLI para exportar dump do Cassandra DB',
    # url='https://github.com/drgarcia1986/cli',
    author=author_name,
    author_email=author_email,
    maintainer=author_name,
    maintainer_email=author_email,
    license='MIT',
    # classifiers=[
    #     'Development Status :: 4 - Beta',
    #     'Intended Audience :: Developers',
    #     'Topic :: System :: Shells',
    #     'Programming Language :: Python :: 2.7',
    #     'Programming Language :: Python :: 3.3',
    #     'Programming Language :: Python :: 3.4',
    #     'Programming Language :: Python :: 3.5',
    # ],
    keywords='cqldump',
    # download_url='https://github.com/drgarcia1986/cli/archive/master.zip',
    packages=find_packages(exclude=['tests*']),
    install_requires=[],
    entry_points={'console_scripts': ['cqldump = cqldump:main']},
    platforms='windows linux',
)