from setuptools import setup, find_packages

setup(
    name='facebook-ads-performance-downloader',
    version='4.0.1',

    description=("Downloads data from the Facebook Ads API to local files"),

    install_requires=[
        'facebook_business==6.0.0',
        'click>=6.0',
        'wheel>=0.29'
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
