from setuptools import setup, find_packages

setup(
    name='facebook-ads-performance-downloader',
    version='1.3.0',

    description=("Downloads data from the Facebook Ads API to local files"),

    install_requires=[
        'facebookads==2.10.1',
        'click>=6.0'
    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={
        'console_scripts': [
            'download-facebook-performance-data=facebook_downloader.cli:download_data'
        ]
    }
)
