from setuptools import setup, find_packages

setup(
    name='vastdb_getdaft',
    version='0.1.0',
    description='A VastDB Connector for getdaft',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'getdaft', 'pyarrow', 'vastdb'
    ],
    python_requires='>=3.9.0',
)
