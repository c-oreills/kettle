import os
from setuptools import setup, find_packages

version='0.5.5'

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='kettle',
    version=version,
    description='Python deploy system',
    long_description=read('readme.md'),
    url='http://github.com/c-oreills/kettle',
    packages=find_packages(),
    include_package_data=True,
    package_data = {
        'kettleweb': ['templates/*', 'static/*'],
        },
    entry_points = """\
[console_scripts]
kettleweb=kettleweb.scripts:kettleweb
""",
    install_requires=[
        'Logbook>=0.3',
        'Flask>=0.8',
        'Jinja2>=2.6',
        'WTForms>=0.6.3',
        'mock>=0.7.0',
        'SQLAlchemy>=0.7.5',
    ],
)
