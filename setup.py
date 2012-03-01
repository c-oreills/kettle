from setuptools import setup, find_packages

version='0.1'

setup(
    name='kettle',
    version=version,
    description='Python deploy system',
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
    ],
)
