import setuptools

setuptools.setup(
    name='payment-data-generator',
    version='1.0.0',
    description='Payment Data Generator Pipeline',
    author='Your Name',
    install_requires=[
        'apache-beam[gcp]>=2.61.0',
        'google-cloud-bigtable>=2.26.0',
        'python-dateutil>=2.8.0',
    ],
    py_modules=[
        'datagenerator_pipeline',
        'bigtable_setup',
        'bigtable_writer',
        'payment_generator'
    ]
)