import setuptools
from pathlib import Path
import subprocess

def GetVersion():

    version = Path('cap/VERSION').read_text().split('-')[0]
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    commit = str(commit, "utf-8").strip()
    versionCommit = f'{version}-{commit}'
    Path('cap/VERSION').write_text(versionCommit)
    return version

setuptools.setup(
    name='cap-genomics',
    version=GetVersion(),
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
    install_requires=['hail', 'munch', 'jsonschema', 'pyarrow', 'fastparquet'],
    packages=setuptools.find_packages()
)