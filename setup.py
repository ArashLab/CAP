import setuptools
from pathlib import Path

setuptools.setup(
    name='cap-genomics',
    version='0.1.14',
    description='Cohort Analysis Platform',
    long_description=Path('README.md').read_text(),
    long_description_content_type='text/markdown',
    url='https://github.com/ArashLab/CAP',
    author='Arash Bayat',
    author_email='a.bayat@garvan.org.au',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    include_package_data=True,
    install_requires=['hail', 'munch', 'jsonschema', 'pyarrow', 'fastparquet'],# 'ruamel.yaml'],
    packages=setuptools.find_packages()
)