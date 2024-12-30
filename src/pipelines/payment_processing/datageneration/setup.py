import setuptools
import os

# Get the absolute path of the current directory
here = os.path.abspath(os.path.dirname(__file__))

setuptools.setup(
    name='payment-data-generator',
    version='1.0.0',
    description='Payment Data Generator Pipeline',
    author='Your Name',
    install_requires=[
        'apache-beam[gcp]>=2.50.0',
        'google-cloud-bigtable>=2.18.0',
        'python-dateutil>=2.8.0',
    ],
    packages=setuptools.find_packages(where=here),
    package_dir={'': here},
    py_modules=[
        'datagenerator_pipeline',
        'bigtable_setup',
        'bigtable_writer',
        'payment_generator'
    ]
)