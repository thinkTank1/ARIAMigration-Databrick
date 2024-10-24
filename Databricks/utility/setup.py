%shell
python setup.py sdist bdist_wheel

from setuptools import find_packages,setup

setup(

    name='ARIAFUNCITONS',
    version='0.0.1',
    packages=find_packages(include=['utility']),
    description='These are custom functions that will be used for the ARIA data migration',
    author='Ara Islam + Naveen Sriram',
    install_requires = []
)