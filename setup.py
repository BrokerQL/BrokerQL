from setuptools import setup, find_packages

__version__ = ''
with open('broker_ql/version.py') as f:
    exec(f.read())

with open('README.md') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

scripts = []
entry_points = {'console_scripts': [
    'broker-ql = broker_ql.entrypoints.main:main',
]}

setup(
    name='BrokerQL',
    version=__version__,
    description="Use SQL to instantly query your online brokers (IBKR and more).",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Pan Jiabang',
    author_email='panjiabang@gmail.com',
    url='https://brokerql.github.io',
    project_urls={
        'Documentation': 'https://brokerql.github.io',
        'Source': 'https://github.com/BrokerQL/BrokerQL',
        'Twitter': 'https://twitter.com/BrokerQL',
    },
    license='AGPL v3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Office/Business :: Financial :: Investment',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='mysql sql tws brokers trading',
    packages=find_packages(exclude=['tests', 'tests.*']),
    package_data={'broker_ql': ['myclirc*',]},
    include_package_data=True,
    python_requires='>=3.9',
    install_requires=install_requires,
    extras_require={},
    scripts=scripts,
    entry_points=entry_points,
)
