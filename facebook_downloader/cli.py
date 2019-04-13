"""Command line interface for facebook downloader"""

import logging
from functools import partial

import click
from facebook_downloader import config


def config_option(config_function):
    """Helper decorator that turns an option function into a cli option"""
    return (lambda function: click.option('--' + config_function.__name__,
                                          help=config_function.__doc__.strip() + '. Example: "' +
                                               str(config_function()) + '"')(function))


def apply_options(kwargs):
    """Applies passed cli parameters to config.py"""
    for key, value in kwargs.items():
        if value: setattr(config, key, partial(lambda v: v, value))


@click.command()
@config_option(config.app_id)
@config_option(config.app_secret)
@config_option(config.access_token)
@config_option(config.data_dir)
@config_option(config.first_date)
@config_option(config.redownload_window)
@config_option(config.target_accounts)
@config_option(config.number_of_ad_performance_threads)
def download_data(**kwargs):
    """
    Downloads data.
    When options are not specified, then the defaults from config.py are used.
    """
    from facebook_downloader import downloader

    apply_options(kwargs)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    downloader.download_data()
